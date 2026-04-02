# Build context: web-apps/  (set in docker-compose.yml)
# ── Build stage ───────────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jdk AS builder
WORKDIR /app
RUN apt-get update -qq && apt-get install -y --no-install-recommends maven && rm -rf /var/lib/apt/lists/*
# Copy all poms first so Docker can cache the dependency-download layer
COPY pom.xml .
COPY telemetry-model/pom.xml ./telemetry-model/pom.xml
COPY producer-app/pom.xml    ./producer-app/pom.xml
COPY consumer-app/pom.xml    ./consumer-app/pom.xml
# Download deps for consumer-app only (no dependency on telemetry-model)
RUN mvn dependency:go-offline -pl consumer-app -q
COPY consumer-app/src ./consumer-app/src
RUN mvn package -DskipTests -q -pl consumer-app

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=builder /app/consumer-app/target/consumer-app-*.jar app.jar
EXPOSE 9502
ENTRYPOINT ["java", "-jar", "app.jar"]
