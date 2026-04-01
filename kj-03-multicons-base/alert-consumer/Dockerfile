# Build context: kj-03-multicons-base/

# ── Build stage ───────────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jdk AS builder
WORKDIR /app
RUN apt-get update -qq && apt-get install -y --no-install-recommends maven && rm -rf /var/lib/apt/lists/*

COPY pom.xml .
COPY model/pom.xml        ./model/pom.xml
COPY util/pom.xml         ./util/pom.xml
COPY config/pom.xml       ./config/pom.xml
COPY dlq-producer/pom.xml ./dlq-producer/pom.xml
COPY storage-consumer/pom.xml ./storage-consumer/pom.xml
COPY alert-consumer/pom.xml   ./alert-consumer/pom.xml
COPY producer-app/pom.xml     ./producer-app/pom.xml
COPY consumer-app/pom.xml     ./consumer-app/pom.xml

RUN mvn dependency:go-offline -q

COPY model/src        ./model/src
COPY util/src         ./util/src
COPY config/src       ./config/src
COPY alert-consumer/src ./alert-consumer/src

RUN mvn package -DskipTests -q -pl model,util,config,alert-consumer

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=builder /app/alert-consumer/target/alert-consumer-*.jar app.jar

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV SLOW_MODE=false

EXPOSE 8083
ENTRYPOINT ["java", "-jar", "app.jar"]
