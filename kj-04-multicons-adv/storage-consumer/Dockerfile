# Build context: kafka-java-web-multi-consumer/

# ── Build stage ───────────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jdk AS builder
WORKDIR /app
RUN apt-get update -qq && apt-get install -y --no-install-recommends maven && rm -rf /var/lib/apt/lists/*

# Copy parent pom + all module poms first — lets Docker cache the dependency download layer
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

# Download all dependencies offline — only re-runs when a pom.xml changes
RUN mvn dependency:go-offline -q

# Copy sources and build the storage-consumer fat JAR (with all dependencies)
COPY model/src        ./model/src
COPY util/src         ./util/src
COPY config/src       ./config/src
COPY dlq-producer/src ./dlq-producer/src
COPY storage-consumer/src ./storage-consumer/src

RUN mvn package -DskipTests -q -pl model,util,config,dlq-producer,storage-consumer

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=builder /app/storage-consumer/target/storage-consumer-*.jar app.jar

# KAFKA_BOOTSTRAP_SERVERS and SLOW_MODE can be overridden via docker-compose environment section
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV SLOW_MODE=false

EXPOSE 8084
ENTRYPOINT ["java", "-jar", "app.jar"]
