# Build context: kafka-java-web-multi-consumer/  (set in docker-compose.yml)
# ── Build stage ───────────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jdk AS builder
WORKDIR /app
RUN apt-get update -qq && apt-get install -y --no-install-recommends maven && rm -rf /var/lib/apt/lists/*
# Copy all poms first so Docker can cache the dependency-download layer
COPY pom.xml .
COPY model/pom.xml        ./model/pom.xml
COPY util/pom.xml         ./util/pom.xml
COPY config/pom.xml       ./config/pom.xml
COPY dlq-producer/pom.xml ./dlq-producer/pom.xml
COPY dlq-viewer/pom.xml   ./dlq-viewer/pom.xml
COPY storage-consumer/pom.xml ./storage-consumer/pom.xml
COPY alert-consumer/pom.xml   ./alert-consumer/pom.xml
COPY producer-app/pom.xml     ./producer-app/pom.xml
COPY consumer-app/pom.xml     ./consumer-app/pom.xml
# Download deps for consumer-app only (no model dependency needed)
RUN mvn dependency:go-offline -pl consumer-app -q
COPY consumer-app/src ./consumer-app/src
RUN mvn package -DskipTests -q -pl consumer-app

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=builder /app/consumer-app/target/consumer-app-*.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]
