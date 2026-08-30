# Product Requirement Document: Library Events Producer Service

| | |
|---|---|
| **Document Status** | Draft |
| **Version** | 1.0 |
| **Author** | Engineering |
| **Last Updated** | August 30, 2026 |
| **Service Name** | library-producer |

---

## 1. Overview

The **Library Events Producer** is a REST API microservice that allows client
applications to notify the library ecosystem whenever a book is **added** or
**updated** in the catalog. Instead of directly writing to a database that
other services depend on, the service publishes a `LibraryEvent` message to an
Apache Kafka topic. Downstream consumer services can then subscribe to this
topic and react accordingly (e.g., update a search index, persist to a
datastore, send notifications), enabling an event-driven, loosely-coupled
architecture.

## 2. Problem Statement

Library-related systems (search, cataloging, notifications, analytics) need a
reliable, decoupled way to learn about book additions and updates without
being tightly coupled to a single relational database or to each other. A
producer-first, event-driven approach lets each downstream system evolve
independently while still staying in sync with the source of truth.

## 3. Goals

- Provide a simple REST interface to publish `ADD` and `UPDATE` library events.
- Guarantee that every valid request results in a message published to Kafka.
- Validate incoming payloads before publishing to prevent malformed events
  from entering the event stream.
- Provide clear, actionable error messages for invalid requests.
- Support both asynchronous (fire-and-forget with callback logging) and
  synchronous (blocking) publishing modes for future flexibility.
- Be runnable locally with minimal setup (Docker Compose Kafka broker).

### Non-Goals

- This service does **not** persist library events to a database (that is the
  responsibility of downstream consumer services).
- This service does **not** expose read/query endpoints for books or library
  events.
- Authentication/authorization, rate limiting, and multi-tenancy are out of
  scope for v1.
- Consumer-side processing of the published events is out of scope for this
  document (tracked separately as `library-consumer`).
- Actual deployment/orchestration onto a running container platform (ECS
  task definitions, EKS manifests, etc.) is out of scope for v1 - this
  document only covers building and publishing the image to ECR. Runtime
  deployment is tracked as a follow-up (see §12).

## 4. Target Users

- **Internal client applications / UIs** that manage the book catalog (e.g., a
  Library Admin Portal) and need to notify the platform of catalog changes.
- **Other backend services** that programmatically add/update books as part
  of a batch import or integration pipeline.

## 5. User Stories

| ID | As a... | I want to... | So that... |
|----|---------|---------------|------------|
| US-1 | Library administrator | submit a new book via a POST request | the catalog change is broadcast to all interested systems |
| US-2 | Library administrator | update an existing book's details via a PUT request | downstream systems stay in sync with the latest book data |
| US-3 | API consumer | receive clear validation errors | I can fix malformed requests quickly |
| US-4 | Platform engineer | see structured logs of publish success/failure | I can debug and monitor message delivery |
| US-5 | Developer | run the service locally with a single command | I can develop and test without a shared Kafka cluster |
| US-6 | Platform engineer | have every push to `main` automatically build and push a Docker image to ECR | deployable artifacts are always available without a manual build step |

## 6. Functional Requirements

### 6.1 Domain Model

**Book**
| Field | Type | Constraints |
|---|---|---|
| `bookId` | Integer | Required |
| `bookName` | String | Required, non-blank |
| `bookAuthor` | String | Required, non-blank |

**LibraryEvent**
| Field | Type | Constraints |
|---|---|---|
| `libraryEventId` | Integer | Required for `PUT`; system/client supplied |
| `libraryEventType` | Enum: `ADD`, `UPDATE` | Set by the server based on the HTTP verb used |
| `book` | `Book` | Required, cascades validation |

### 6.2 API Endpoints

#### `POST /v1/libraryevent`
- **Purpose**: Publish a new library event representing a newly added book.
- **Request body**: JSON `LibraryEvent` (book required; `libraryEventId` optional/ignored).
- **Behavior**:
  1. Validate request body (`@Valid`).
  2. Server sets `libraryEventType = ADD`.
  3. Publish the event to the configured Kafka topic, keyed by `libraryEventId`.
  4. Return the published event.
- **Responses**:
  - `201 Created` — event accepted and published (async).
  - `400 Bad Request` — validation failure, with field-level error messages.

#### `PUT /v1/libraryevent`
- **Purpose**: Publish a library event representing an update to an existing book.
- **Request body**: JSON `LibraryEvent`, `libraryEventId` **required**.
- **Behavior**:
  1. Validate request body (`@Valid`).
  2. If `libraryEventId` is null, return `400 Bad Request`.
  3. Server sets `libraryEventType = UPDATE`.
  4. Publish the event to the configured Kafka topic, keyed by `libraryEventId`.
  5. Return the published event.
- **Responses**:
  - `200 OK` — event accepted and published (async).
  - `400 Bad Request` — missing `libraryEventId` or validation failure.

### 6.3 Kafka Publishing

- Messages are published to a configurable topic (default: `library-events`).
- Message **key** = `libraryEventId` (Integer), enabling partition affinity per
  book/event id.
- Message **value** = JSON-serialized `LibraryEvent`.
- Publishing supports:
  - **Asynchronous** send with a completion callback that logs success
    (partition, key, value) or failure (exception details).
  - **Synchronous** send (blocking `get()`) for use cases requiring
    confirmation before returning to the caller.

### 6.4 Topic Provisioning & Durability

- The topic is explicitly provisioned at application startup via a
  `KafkaAdmin`-backed `NewTopic` bean (`KafkaTopicConfig`), instead of relying
  on the broker's `auto.create.topics.enable` setting. This bean is idempotent
  (no-op if the topic already exists) and configures:
  - `partitions = 3`, `replicas = 3` — matches the 3-broker cluster so each
    partition has a copy on every broker.
  - `min.insync.replicas = 2` — with the producer's `acks=all`, a write is
    only acknowledged once **2 of the 3** replicas have it, so the cluster
    tolerates **1 broker outage** with zero data loss, while still rejecting
    writes if 2+ brokers are unavailable (fails closed rather than silently
    under-replicating).
  - ⚠️ **Important caveat**: `NewTopic` beans only apply their partition
    count, replication factor, and configs **at creation time**. If a topic
    already exists (e.g., it was previously auto-created by the broker with
    a lower replication factor), the bean is a no-op and will **not**
    retroactively fix it. When we first connected this app to the remote
    cluster, `library-events` had already been auto-created with replication
    factor 1; setting `min.insync.replicas=2` against that under-replicated
    topic would have made every `acks=all` produce fail with
    `NotEnoughReplicasException`. We corrected this once, out-of-band, via a
    partition reassignment (`AdminClient.alterPartitionReassignments`) to
    spread each partition's replicas across all 3 brokers before applying
    `min.insync.replicas=2`. New environments/clusters won't hit this issue
    since the topic will be created fresh with the correct settings from the
    `NewTopic` bean.

### 6.5 Validation & Error Handling

- Bean Validation (`jakarta.validation`) annotations enforce required fields.
- A centralized `@RestControllerAdvice` intercepts
  `MethodArgumentNotValidException` and returns a `400` response with a map of
  `{ field: errorMessage }` for every failing field.

## 7. Non-Functional Requirements

| Category | Requirement |
|---|---|
| **Performance** | Async publish path should not block the HTTP response beyond request validation. |
| **Reliability** | Failed publishes must be logged with enough detail (key, value, exception) to support manual replay/inspection. |
| **Observability** | Structured logs for every publish attempt (success and failure). |
| **Portability** | Runnable locally via Docker Compose (single-broker KRaft-mode Kafka); packaged as a portable Docker image (multi-stage build) published to AWS ECR on every merge to `main`. |
| **Maintainability** | Layered structure: `domain`, `producer`, `controller`. |
| **Configuration** | Kafka bootstrap servers, serializers, and topic name externalized via `application.properties`. |

## 8. Technical Architecture

### 8.1 Tech Stack
- **Language**: Java 25
- **Framework**: Spring Boot 4.1.1 (Spring Framework 7)
- **Build tool**: Gradle (with Java toolchain pinned to JDK 25)
- **Messaging**: Apache Kafka (via `spring-kafka` / `spring-boot-starter-kafka`)
- **Serialization**: Jackson 3 (`tools.jackson.databind.ObjectMapper`) for
  event payloads; Kafka `StringSerializer`/`IntegerSerializer` at the wire
  level.
- **Validation**: Jakarta Bean Validation (Hibernate Validator)
- **Boilerplate reduction**: Lombok
- **Containerization**: Docker (multi-stage build, `eclipse-temurin:25-jdk`
  build stage / `eclipse-temurin:25-jre` runtime stage)
- **CI/CD**: GitHub Actions (`.github/workflows/ci.yml` for tests,
  `.github/workflows/deploy.yml` to build & push the image to AWS ECR)

### 8.2 Package Structure

```
com.kafkaplayground
├── domain
│   ├── Book.java              # record: bookId, bookName, bookAuthor
│   ├── LibraryEventType.java  # enum: ADD, UPDATE
│   └── LibraryEvent.java      # libraryEventId, libraryEventType, book
├── producer
│   └── LibraryEventProducer.java   # KafkaTemplate-based publisher (async + sync)
└── controller
    ├── LibraryEventsController.java       # POST / PUT endpoints
    └── LibraryEventsControllerAdvice.java # validation error handling
```

### 8.3 Sequence (POST example)

1. Client sends `POST /v1/libraryevent` with a `LibraryEvent` JSON payload.
2. Spring MVC binds & validates the payload (`@Valid`).
3. Controller sets `libraryEventType = ADD`.
4. Controller delegates to `LibraryEventProducer.sendLibraryEvent(...)`.
5. Producer serializes the event to JSON and sends it to Kafka via
   `KafkaTemplate`, keyed by `libraryEventId`.
6. On completion, the producer logs success (partition/offset info) or
   failure.
7. Controller immediately returns `201 Created` with the event body (does not
   wait for the Kafka ack in the async path).

### 8.4 Kafka Cluster Environment

The service connects to a **remote 3-broker Apache Kafka cluster** hosted on
a single AWS EC2 instance (Docker containers `kafka-broker-1`,
`kafka-broker-2`, `kafka-broker-3`), monitored via Kafka UI under the cluster
name `playground-cluster`.

| Broker | Advertised Address |
|---|---|
| kafka-broker-1 | `ec2-18-223-60-27.us-east-2.compute.amazonaws.com:9092` |
| kafka-broker-2 | `ec2-18-223-60-27.us-east-2.compute.amazonaws.com:9093` |
| kafka-broker-3 | `ec2-18-223-60-27.us-east-2.compute.amazonaws.com:9094` |

All three bootstrap addresses are configured in
`spring.kafka.producer.bootstrap-servers` so the producer can discover the
full cluster metadata and tolerate a single broker outage. No local Kafka
broker (Docker Compose) is required for this project; `compose.yaml` is kept
empty/documented for reference only.

### 8.5 Local Development Environment

- No local Kafka broker is provisioned — the app connects directly to the
  shared AWS `playground-cluster` cluster described above.
- `gradle.properties` pins the Gradle JVM to the JDK 25 installation.
- Topic auto-creation is enabled on the remote cluster, so
  `library-events` is created on first publish if it doesn't already exist.

### 8.6 Containerization & Deployment (AWS ECR)

**Dockerfile** (multi-stage build):
1. **Build stage** (`eclipse-temurin:25-jdk-jammy`) — copies the Gradle
   wrapper and build files first (for layer caching), then sources, and runs
   `./gradlew bootJar -x test` (tests run separately in CI, not inside the
   image build).
2. **Runtime stage** (`eclipse-temurin:25-jre-jammy`) — copies only the
   built executable jar, runs as a non-root `spring` user, exposes port
   `8080`.
3. `.dockerignore` excludes `build/`, `.gradle/`, `.git/`, docs, and IDE
   files to keep the build context small.

> **Note**: The Spring Boot Gradle plugin normally also produces a
> non-executable "plain" jar (`*-SNAPSHOT-plain.jar`) alongside the
> executable `bootJar` output. Since both match a `*-SNAPSHOT.jar` glob,
> the plain jar is disabled in `build.gradle` (`tasks.named('jar') { enabled
> = false }`) so the Dockerfile's `COPY` step unambiguously picks up the
> single executable jar.

**CI/CD pipeline** (GitHub Actions):
- `.github/workflows/ci.yml` — runs `./gradlew build` (unit + web-slice +
  embedded-Kafka integration tests) on every push/PR to `main`. No AWS
  credentials required (uses the embedded Kafka broker, not the real
  cluster).
- `.github/workflows/deploy.yml` — on every push to `main` (or a `v*` tag, or
  manual dispatch):
  1. **`test` job** — re-runs `./gradlew test` as a gate before building the
     image (fails fast, avoids pushing a broken image).
  2. **`build-and-push` job** (needs `test`):
     - Assumes an AWS IAM role via **GitHub OIDC**
       (`aws-actions/configure-aws-credentials`) — no long-lived AWS access
       keys stored in GitHub Secrets.
     - Logs in to ECR (`aws-actions/amazon-ecr-login`).
     - Ensures the target ECR repository exists (idempotent
       `describe-repositories` / `create-repository` with image scanning
       enabled on push).
     - Builds and pushes the image via `docker/build-push-action`, tagged
       with both the short Git SHA and `latest`, using GitHub Actions layer
       caching (`cache-from`/`cache-to: type=gha`).

**Required GitHub repository configuration:**

| Type | Name | Example / Notes |
|---|---|---|
| Variable | `AWS_REGION` | `us-east-2` (defaults to this if unset) |
| Variable | `ECR_REPOSITORY` | `library-producer` (defaults to this if unset) |
| Secret | `AWS_ROLE_TO_ASSUME` | IAM role ARN trusted for GitHub's OIDC provider, with `ecr:*` push permissions scoped to the target repository |

Deploying the image to a running environment (ECS service update, EKS
rollout, etc.) is intentionally **out of scope** for `deploy.yml` — it only
builds and publishes the artifact; see §12 for the follow-up.

## 9. Configuration Reference

| Property | Default | Description |
|---|---|---|
| `spring.kafka.bootstrap-servers` | `ec2-18-223-60-27.us-east-2.compute.amazonaws.com:9092,...:9093,...:9094` | Remote AWS `playground-cluster` broker addresses (kafka-broker-1/2/3); shared by the producer and `KafkaAdmin` |
| `spring.kafka.producer.key-serializer` | `IntegerSerializer` | Serializer for the message key |
| `spring.kafka.producer.value-serializer` | `StringSerializer` | Serializer for the message value (JSON string) |
| `spring.kafka.producer.acks` | `all` | Wait for `min.insync.replicas` to acknowledge |
| `spring.kafka.producer.retries` | `3` | Retry transient send failures |
| `library-events.topic` | `library-events` | Target Kafka topic |
| `library-events.topic.partitions` | `3` | Partitions requested when the topic is first created |
| `library-events.topic.replicas` | `3` | Replication factor requested when the topic is first created |
| `library-events.topic.min-insync-replicas` | `2` | Min in-sync replicas required for an `acks=all` write to succeed |
| `server.port` | `8080` | HTTP port |

## 10. Success Metrics

- **100%** of valid `POST`/`PUT` requests result in a message published to
  Kafka (measured via producer success logs vs. total accepted requests).
- **0** unhandled exceptions surfaced to API clients for validation failures
  (all validation errors return structured `400` responses).
- Local onboarding time (clone → running service + Kafka) under **5 minutes**.
- **100%** of merges to `main` result in a successfully built and pushed
  Docker image in ECR, tagged with both the Git SHA and `latest`.

## 11. Risks & Open Questions

| Risk / Question | Notes |
|---|---|
| No idempotency/deduplication for retried publishes | Consumers must handle duplicate events (at-least-once semantics). |
| Async publish returns `201`/`200` before Kafka ack completes | Client cannot detect publish failures synchronously in the async path; consider exposing the sync method via a dedicated endpoint or header flag if stronger guarantees are needed. |
| No schema registry / Avro | JSON payloads have no enforced schema evolution strategy; revisit if event volume/consumers grow. |
| No authentication on endpoints | Acceptable for internal/dev use; must be addressed before production exposure. |
| Topic auto-creation relies on broker config | Verified working against the remote `playground-cluster` (auto-creates `library-events` on first publish); production Kafka clusters typically disable this — must be handled via infra-as-code before go-live. |
| Kafka cluster exposed on public EC2 DNS with plaintext (PLAINTEXT) listeners | Credentials/data are not encrypted in transit and there is no broker-side authentication (SASL/mTLS) or network ACLs beyond the security group; anyone who can reach the EC2 host on ports 9092–9094 can produce/consume. Acceptable for a playground/dev cluster only — must move to SASL_SSL + private networking (VPC/VPN) before handling real data. |
| `deploy.yml` only builds/pushes the image; nothing deploys it | There is currently no ECS/EKS/other runtime target wired up to consume the new image automatically; someone (or another pipeline) must still trigger a deployment/rollout after the image lands in ECR. |
| ECR image scanning is enabled but findings aren't gated | `create-repository` enables `scanOnPush`, but the workflow doesn't currently fail the pipeline on HIGH/CRITICAL CVE findings — vulnerable images can still be pushed and pulled. |
| OIDC IAM role permissions not defined in this repo | `AWS_ROLE_TO_ASSUME` must be provisioned out-of-band (Terraform/console) with least-privilege ECR push permissions scoped to the specific repository; not yet codified as infra-as-code. |

## 12. Future Enhancements (Out of Scope for v1)

- `GET`/`DELETE` support and a corresponding `library-consumer` service.
- Dead-letter topic / retry topic for failed publishes.
- Schema Registry (Avro/Protobuf) for stronger contract guarantees.
- AuthN/AuthZ (e.g., OAuth2 resource server) on the REST endpoints.
- Idempotent producer configuration (`enable.idempotence=true`) and
  exactly-once delivery semantics.
- Health/metrics endpoints (Spring Boot Actuator) for production readiness.
- Runtime deployment automation (ECS service update / EKS rollout / Helm
  chart) triggered after the image lands in ECR, so `deploy.yml` results in
  an actually-running updated service rather than just a published image.
- Fail the CI/CD pipeline on HIGH/CRITICAL ECR image scan findings instead of
  only enabling scan-on-push informationally.

