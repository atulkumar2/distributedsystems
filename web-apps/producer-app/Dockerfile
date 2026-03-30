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
# Download deps for the modules we'll compile (-pl = project list)
RUN mvn dependency:go-offline -pl telemetry-model,producer-app -q
# Copy sources for the modules producer-app depends on
COPY telemetry-model/src ./telemetry-model/src
COPY producer-app/src    ./producer-app/src
# -am = "also make" — builds telemetry-model before producer-app automatically
RUN mvn package -DskipTests -q -pl producer-app -am

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=builder /app/producer-app/target/producer-app-*.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
