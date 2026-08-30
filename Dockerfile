# syntax=docker/dockerfile:1

# ---------- Build stage ----------
# Full JDK needed to run the Gradle build itself.
FROM eclipse-temurin:25-jdk-jammy AS build
WORKDIR /workspace

# Copy the Gradle wrapper and build files first so dependency resolution is
# cached in its own Docker layer and isn't invalidated by source changes.
COPY gradlew ./
COPY gradle ./gradle
COPY build.gradle settings.gradle ./
RUN chmod +x gradlew

# Copy sources and build the executable jar (skip tests here - they run in CI
# before this image is built; see .github/workflows/deploy.yml).
COPY src ./src
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew bootJar -x test --no-daemon --console=plain

# ---------- Runtime stage ----------
# Slim JRE-only image for the actual runtime footprint.
FROM eclipse-temurin:25-jre-jammy AS runtime

# Run as a non-root user.
RUN groupadd --system spring && useradd --system --gid spring spring

WORKDIR /app
COPY --from=build /workspace/build/libs/*-SNAPSHOT.jar app.jar
RUN chown spring:spring app.jar
USER spring

ENV JAVA_OPTS=""
EXPOSE 8080

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]

