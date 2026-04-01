# Build context: kj-04-multicons-adv/
FROM eclipse-temurin:25-jdk AS builder
WORKDIR /app
RUN apt-get update -qq && apt-get install -y --no-install-recommends maven && rm -rf /var/lib/apt/lists/*

COPY pom.xml .
COPY model/pom.xml      ./model/pom.xml
COPY util/pom.xml       ./util/pom.xml
COPY config/pom.xml     ./config/pom.xml
COPY dlq-producer/pom.xml ./dlq-producer/pom.xml
COPY storage-consumer/pom.xml ./storage-consumer/pom.xml
COPY alert-consumer/pom.xml   ./alert-consumer/pom.xml
COPY producer-app/pom.xml     ./producer-app/pom.xml
COPY consumer-app/pom.xml     ./consumer-app/pom.xml
COPY dlq-viewer/pom.xml ./dlq-viewer/pom.xml

RUN mvn dependency:go-offline -q -pl model,util,config,dlq-viewer

COPY model/src      ./model/src
COPY util/src       ./util/src
COPY config/src     ./config/src
COPY dlq-viewer/src ./dlq-viewer/src

RUN mvn package -DskipTests -q -pl model,util,config,dlq-viewer

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=builder /app/dlq-viewer/target/dlq-viewer-*.jar app.jar
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
EXPOSE 8085
ENTRYPOINT ["java", "-jar", "app.jar"]
