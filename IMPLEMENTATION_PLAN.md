# Implementation Plan: Library Events Producer Service

| | |
|---|---|
| **Document Status** | Living document |
| **Version** | 1.0 |
| **Companion Doc** | `PRD.md` |
| **Last Updated** | August 30, 2026 |

This plan translates the PRD into concrete engineering phases, tasks, owners,
and acceptance criteria. **Phase 0 is already complete** and described here
for traceability; Phases 1–4 are the remaining work, ordered by priority
(hardening → production-readiness → future scope from PRD §12).

---

## Status Legend

- ✅ Done
- 🚧 In progress
- ⬜ Not started

---

## Phase 0 — Core Producer (✅ Done)

Goal: satisfy PRD §6.1–§6.5 (domain model, endpoints, publishing, topic
provisioning, validation).

| # | Task | Status | Notes |
|---|---|---|---|
| 0.1 | Scaffold Spring Boot 4.1.1 / Java 25 Gradle project | ✅ | `build.gradle`, `gradle.properties` pinned to JDK 25 |
| 0.2 | Domain model: `Book`, `LibraryEventType`, `LibraryEvent` | ✅ | Bean Validation annotations in place |
| 0.3 | `POST /v1/libraryevent` (ADD) | ✅ | Returns `201` |
| 0.4 | `PUT /v1/libraryevent` (UPDATE, requires `libraryEventId`) | ✅ | Returns `200` / `400` |
| 0.5 | `LibraryEventProducer` — async publish + sync variant | ✅ | `KafkaTemplate<Integer, String>` + Jackson 3 `ObjectMapper` |
| 0.6 | Centralized validation error handling | ✅ | `LibraryEventsControllerAdvice` |
| 0.7 | Connect to remote AWS `playground-cluster` (3 brokers) | ✅ | `spring.kafka.bootstrap-servers` set to all 3 broker addresses |
| 0.8 | Explicit topic provisioning (`KafkaAdmin` + `NewTopic` bean) | ✅ | `KafkaTopicConfig`: 3 partitions / 3 replicas / `min.insync.replicas=2` |
| 0.9 | Fix under-replicated `library-events` topic on the live cluster | ✅ | One-off `AdminClient` partition reassignment; verified `acks=all` writes succeed |
| 0.10 | End-to-end manual verification (POST/PUT/validation) | ✅ | Verified against live cluster with curl |

**Exit criteria (met):** App builds, starts, and both endpoints publish
successfully to the remote cluster with durable (`acks=all` +
`min.insync.replicas=2`) semantics.

---

## Phase 1 — Automated Testing & CI Confidence (✅ Done)

Goal: replace manual `curl` verification with a real automated test suite so
regressions are caught before deploy. This directly supports PRD §10 Success
Metrics ("100% of valid requests publish successfully").

| # | Task | Status | Notes |
|---|---|---|---|
| 1.1 | Add `EmbeddedKafkaBroker` test config (`@EmbeddedKafka` from `spring-kafka-test`) | ✅ | `src/test/resources/application-test.properties` overrides `spring.kafka.bootstrap-servers` with `${spring.embedded.kafka.brokers}`; activated via `@ActiveProfiles("test")` |
| 1.2 | Unit test: `LibraryEventProducer.sendLibraryEvent` success path | ✅ | `LibraryEventProducerTest` - mocks `KafkaTemplate`, verifies `SendResult` partition/offset |
| 1.3 | Unit test: `LibraryEventProducer` failure path (mock `KafkaTemplate` to fail) | ✅ | `LibraryEventProducerTest` - both async (`CompletableFuture` completes exceptionally) and sync (`RuntimeException` thrown) paths covered |
| 1.4 | `@WebMvcTest` for `LibraryEventsController` — POST valid/invalid, PUT valid/invalid/missing-id | ✅ | `LibraryEventsControllerTest` - 6 cases incl. nested `Book` field validation errors |
| 1.5 | Integration test: full Spring context + `@EmbeddedKafka`, POST/PUT via `TestRestTemplate`, assert consumed record on test topic | ✅ | `LibraryEventsIntegrationTest` - 2 cases, asserts real message lands on the embedded topic |
| 1.6 | Replace the ad hoc `hs_err_pid*.log` / JIT-crash workaround notes with a documented `org.gradle.jvmargs` mitigation if recurring | ✅ | `gradle.properties`: `-XX:TieredStopAtLevel=1` with explanatory comment |
| 1.7 | Wire `./gradlew test` into a CI workflow (GitHub Actions) | ✅ | `.github/workflows/ci.yml` - runs `./gradlew build` on JDK 25, uploads test report artifact |

**Exit criteria (met):** `./gradlew test` passes 13/13 tests across 4 test
classes (unit, web-slice, full-context integration) with zero dependency on
the remote AWS cluster. `./gradlew clean build` verified green end-to-end.

**Notable findings while implementing (Spring Boot 4.1 API relocations):**
- `@WebMvcTest` moved from `org.springframework.boot.test.autoconfigure.web.servlet`
  to `org.springframework.boot.webmvc.test.autoconfigure`.
- `TestRestTemplate` moved out of core `spring-boot-test` into a new
  `org.springframework.boot:spring-boot-resttestclient` module (which itself
  needs `spring-boot-restclient` for `RestTemplateBuilder`, plus an explicit
  `@AutoConfigureTestRestTemplate` annotation - it's no longer wired
  automatically by `@SpringBootTest(webEnvironment = RANDOM_PORT)`).
- `@MockBean` is gone; replaced by `@org.springframework.test.context.bean.override.mockito.MockitoBean`.
- Nested `@Valid` field validation errors report the field path as a single
  string key (e.g. `"book.bookName"`), not a nested JSON object - so test/API
  consumers must query `$['book.bookName']`, not `$.book.bookName`.
- `KafkaTestUtils.getSingleRecord` is unsuitable when multiple tests in the
  same class share the cached Spring context/embedded topic; switched to
  polling all available records and matching on a unique payload marker.

---

## Phase 1.5 — Containerization & ECR Image Publishing (✅ Done)

Goal: package the service as a portable Docker image and automatically
publish it to AWS ECR on every merge to `main`, per PRD §8.6.

| # | Task | Status | Notes |
|---|---|---|---|
| 1.5.1 | Multi-stage `Dockerfile` (JDK build stage / JRE runtime stage, non-root user) | ✅ | `eclipse-temurin:25-jdk-jammy` → `eclipse-temurin:25-jre-jammy` |
| 1.5.2 | Fix ambiguous jar output (`*-SNAPSHOT.jar` matching both bootJar and the plain jar) | ✅ | Disabled the plain `jar` task in `build.gradle` so only the executable jar remains in `build/libs/` |
| 1.5.3 | `.dockerignore` to keep build context small | ✅ | Excludes `build/`, `.gradle/`, `.git/`, docs, IDE files |
| 1.5.4 | `deploy.yml` — test gate + OIDC AWS auth + ECR login + build/push | ✅ | Two jobs: `test` (gates on `./gradlew test`) → `build-and-push` (assumes IAM role via OIDC, ensures repo exists, builds & pushes via `docker/build-push-action` with GHA layer caching) |
| 1.5.5 | Tag pushed images with both Git SHA and `latest` | ✅ | `steps.image-meta` computes both tags from the ECR registry + `GITHUB_SHA` |
| 1.5.6 | Document required repo configuration (variables/secrets) | ✅ | `AWS_REGION`, `ECR_REPOSITORY` (vars); `AWS_ROLE_TO_ASSUME` (secret) - documented in `deploy.yml` header comment and PRD §8.6 |

**Exit criteria (met):** Pushing to `main` runs tests, then builds and
pushes a Docker image to ECR tagged with the commit SHA and `latest`, using
short-lived OIDC credentials (no static AWS keys in GitHub Secrets).

**Not yet done (tracked in Phase 2 / PRD §12):** nothing automatically
*deploys* the new image to a running environment (ECS/EKS) - `deploy.yml`
only builds and publishes it; and ECR scan-on-push findings aren't yet used
to gate the pipeline.

---

## Phase 2 — Production Hardening (⬜ Not started)

Goal: close the gaps flagged in PRD §11 Risks that block real (non-playground)
use.

| # | Task | Owner | Acceptance Criteria |
|---|---|---|---|
| 2.1 | **Secure the Kafka cluster**: move from `PLAINTEXT` to `SASL_SSL` (or at minimum SASL/PLAIN over TLS) on the AWS brokers; rotate to a private VPC/VPN-only network path | Infra | Producer connects via `SASL_SSL`; ports 9092-9094 no longer world-reachable in plaintext |
| 2.2 | Externalize Kafka credentials via env vars / secrets manager (no plaintext secrets in `application.properties`) | Eng | `spring.kafka.properties.sasl.jaas.config` sourced from env at runtime |
| 2.3 | Add authentication/authorization to `/v1/libraryevent` endpoints (e.g., API key header or OAuth2 resource server) | Eng | Unauthenticated requests receive `401` |
| 2.4 | Enable `spring.kafka.producer.properties.enable.idempotence=true` explicitly (verify it's not silently defaulted) + document exactly-once vs at-least-once tradeoffs | Eng | Confirmed via producer logs / config dump |
| 2.5 | Dead-letter / retry topic for publish failures that exhaust retries | Eng | Failed sends after `retries` are written to `library-events.DLT` with original headers preserved |
| 2.6 | Add Spring Boot Actuator (`/actuator/health`, `/actuator/metrics`, `/actuator/info`) | Eng | `kafka` health indicator reports cluster reachability |
| 2.7 | Structured JSON logging (for log aggregation) instead of default console pattern | Eng | Logs parseable by ELK/CloudWatch |
| 2.8 | Rate limiting / basic abuse protection on the two endpoints | Eng | Configurable requests/sec threshold returns `429` beyond limit |
| 2.9 | Infra-as-code for topic provisioning (Terraform/Ansible) as the long-term source of truth, superseding the app-managed `NewTopic` bean for prod | Infra | Topic config reproducible without running the app |
| 2.10 | Automate runtime deployment after image publish (ECS service update / EKS rollout / Helm) | Infra | New image in ECR results in an actually-running updated service, not just a published artifact |
| 2.11 | Gate `deploy.yml` on ECR image scan findings (fail on HIGH/CRITICAL CVEs) | Eng | Pipeline fails before a vulnerable image can be pulled/deployed |

**Exit criteria:** Service can be exposed outside a trusted network boundary
without violating the risks called out in PRD §11.

---

## Phase 3 — Schema Governance (⬜ Not started)

Goal: address PRD §11 "No schema registry / Avro" risk before consumer count
grows.

| # | Task | Owner | Acceptance Criteria |
|---|---|---|---|
| 3.1 | Stand up / connect to a Schema Registry (Confluent or Apicurio) | Infra | Registry reachable from producer's network |
| 3.2 | Define Avro (or Protobuf) schema for `LibraryEvent` mirroring current JSON shape | Eng | Schema registered under subject `library-events-value` |
| 3.3 | Swap `StringSerializer`/`ObjectMapper` JSON path for `KafkaAvroSerializer` (or Protobuf equivalent) | Eng | Producer publishes Avro-encoded records; schema ID embedded |
| 3.4 | Define a compatibility mode (`BACKWARD` recommended) and document the schema evolution process | Eng | Registry enforces compatibility on new schema versions |
| 3.5 | Update `library-consumer` (or stub) to deserialize via the registry | Eng | Consumer round-trips a produced event correctly |

**Exit criteria:** New fields can be added to `LibraryEvent` without breaking
existing consumers, enforced automatically by the registry.

---

## Phase 4 — Ecosystem Completion (⬜ Not started)

Goal: deliver the remaining PRD §12 future enhancements once the producer is
hardened.

| # | Task | Owner | Acceptance Criteria |
|---|---|---|---|
| 4.1 | Build `library-consumer` service (separate repo/module) subscribing to `library-events` | Eng | Consumes ADD/UPDATE events, persists to its own datastore |
| 4.2 | `GET /v1/libraryevent/{id}` on the consumer side (read model) | Eng | Returns last-known state per `libraryEventId` |
| 4.3 | `DELETE` event type + endpoint (`LibraryEventType.DELETE`) | Eng | New enum value plumbed through producer + consumer; soft-delete semantics documented |
| 4.4 | Idempotent consumer processing (dedupe by `libraryEventId` + offset/timestamp) | Eng | Re-delivered messages don't corrupt consumer state |
| 4.5 | End-to-end demo/documentation (README with sequence diagram, sample Postman collection) | Eng | New engineer can run producer + consumer locally against a shared cluster within the PRD's 5-minute onboarding target |

**Exit criteria:** Full ADD/UPDATE/DELETE lifecycle observable end-to-end
across producer and consumer services.

---

## Cross-Cutting Tracking

| Workstream | Phase(s) | Priority | Rough Effort |
|---|---|---|---|
| Automated tests + CI | 1 | ✅ Done | 1–2 days |
| Kafka cluster security (SASL_SSL, network) | 2 | High before any non-playground use (do next) | 2–3 days (mostly infra) |
| API authN/authZ | 2 | High before external exposure | 1 day |
| DLT + Actuator + structured logging | 2 | Medium | 1–2 days |
| Schema Registry migration | 3 | Medium (do before consumer count grows) | 2–3 days |
| Consumer service + DELETE support | 4 | Low (separate project) | 1+ week |

---

## Immediate Next Steps (recommended order)

1. ~~**Phase 1.1–1.5**: Add `spring-kafka-test` + `@EmbeddedKafka` tests~~ ✅ Done.
2. **Phase 2.1/2.2**: Since the cluster is currently plaintext and publicly
   reachable, prioritize securing it (or at minimum restricting the EC2
   security group to known IPs) before doing anything else with real data.
3. **Phase 2.3**: Add basic auth/API-key gating to the two endpoints — cheap
   to add now, meaningfully reduces risk while the cluster is public.
4. Revisit Phases 3–4 once the above are in place and consumer demand is
   confirmed.

