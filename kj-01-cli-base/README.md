# Vehicle Telemetry Kafka Learning Project (Java + Maven)

A minimal but practical hands-on project to learn Java + Kafka integration using an IoT/telematics scenario.
This is **Stage 1** of four setups in this repo — pure Java, no frameworks, lowest possible abstraction layer.

## What this project demonstrates

- Java Kafka producer
- Java Kafka consumer (auto-commit and manual-commit)
- JSON telemetry payloads with `vehicleId` as the message key
- Standard consumer read/print loop
- Manual offset commit for at-least-once delivery guarantees
- Broker endpoint loaded from a config file (`src/main/resources/kafka.properties`)
- Kafka UI dashboard via Docker Compose
- Inline comments explaining *why* each Kafka setting matters

## Project structure

```text
kj-01-cli-base/
├── pom.xml
├── run.sh
├── docker-compose.yml
└── src/main/
    ├── java/com/example/telematics/
    │   ├── consumer/
    │   │   ├── KafkaConsumerExample.java          auto-commit consumer
    │   │   └── KafkaManualCommitConsumerExample.java
    │   ├── model/
    │   │   └── TelemetryEvent.java
    │   ├── producer/
    │   │   └── KafkaProducerExample.java
    │   └── util/
    │       └── JsonUtil.java                      singleton ObjectMapper
    └── resources/
        └── kafka.properties                       broker address + group ids
```

## Prerequisites

- Java 25
- Maven wrapper included — no separate Maven installation needed (runs Maven 4.0.0 via `./mvnw`)
- Docker with `docker compose` for the local Kafka stack

## Shared local infrastructure

Kafka, Kafka UI, and Portainer now run from the repo-level [`infra/`](../infra/) stack.
`./run.sh --start` brings that shared infrastructure up automatically before compiling this module.

You can also start the shared infra directly:

```bash
cd ../infra
./run.sh --start
```

This starts:

- **Kafka** on `localhost:9092` (for the Java app on your host)
- **Kafka UI** at [http://localhost:8080](http://localhost:8080) — browse topics, partitions, offsets,
  consumer groups, and live messages in a web browser
- **Portainer** at [http://localhost:9000](http://localhost:9000) — inspect the local Docker stack

Stop everything:

```bash
cd ../infra
./run.sh --stop
```

> **Note:** if you already have a `kafka-local` container running from a
> previous `docker run`, remove it first: `docker rm -f kafka-local`

## Quick start

Use the helper script to start Kafka, wait for broker readiness, create the topic,
and compile the Java project:

```bash
./run.sh --start
```

After it completes, Kafka is available on `localhost:9092`, Kafka UI is available at
[http://localhost:8080](http://localhost:8080), and the topic `vehicle-telemetry`
is ready to use.

If host ports `9092` or `8080` are already in use by another repo stage, clear the blockers first:

```bash
./run.sh --stop-blockers
```

## Create the topic

Wait for the broker to be healthy, then:

```bash
docker exec -it kafka-local /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic vehicle-telemetry \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

Or create the topic directly in **Kafka UI → Topics → Add a Topic**.

Check topic details:

```bash
docker exec -it kafka-local /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic vehicle-telemetry \
  --bootstrap-server localhost:9092
```

## Kafka UI

Open [http://localhost:8080](http://localhost:8080) once the stack is up.

Useful views for this project:

| UI section | What to look at |
| --- | --- |
| Topics → vehicle-telemetry | Partition count, message count per partition |
| Topics → vehicle-telemetry → Messages | Live messages with key, value, partition, offset |
| Consumers | `vehicle-telemetry-group` lag per partition |
| Brokers | Cluster health, partition leadership |

## Compile the project

```bash
./mvnw clean compile
```

## Run producer

```bash
./mvnw exec:java -Dexec.mainClass=com.example.telematics.producer.KafkaProducerExample
```

Expected producer output (example):

```text
Sent key=VH-1001 to topic=vehicle-telemetry partition=2 offset=0
Sent key=VH-1002 to topic=vehicle-telemetry partition=1 offset=0
Sent key=VH-1003 to topic=vehicle-telemetry partition=0 offset=0
Producer finished sending telemetry events.
```

## Run consumer (auto commit)

In a second terminal:

```bash
./mvnw exec:java -Dexec.mainClass=com.example.telematics.consumer.KafkaConsumerExample
```

Expected consumer output (example):

```text
Received key=VH-1001 value={"vehicleId":"VH-1001","timestamp":"2026-03-29T10:21:15.214Z","latitude":12.97,"longitude":77.56,"speed":62.45,"fuelLevel":81.17,"engineStatus":"ON"} partition=2 offset=0
```

## Run manual commit consumer

```bash
./mvnw exec:java -Dexec.mainClass=com.example.telematics.consumer.KafkaManualCommitConsumerExample
```

Expected output (example):

```text
Processed key=VH-1002 value={...} partition=1 offset=3
Offsets committed manually for latest processed batch.
```

## End-to-end test flow

1. Start shared infra with `./run.sh --start` or `cd ../infra && ./run.sh --start`.
2. Create topic `vehicle-telemetry` if needed (the helper script already does this).
3. Run one consumer (`./mvnw exec:java -Dexec.mainClass=com.example.telematics.consumer.KafkaConsumerExample`) in terminal A.
4. Run producer (`./mvnw exec:java -Dexec.mainClass=com.example.telematics.producer.KafkaProducerExample`) in terminal B.
5. Observe key/value/partition/offset in terminal A.
6. Stop the auto-commit consumer and run the manual-commit consumer with `./mvnw exec:java -Dexec.mainClass=com.example.telematics.consumer.KafkaManualCommitConsumerExample`.
7. Run producer again and observe manual commit logs.

## Why key and consumer group choices matter

- **`vehicleId` as key**: events for each vehicle stay ordered because Kafka maps the same key to the same partition.
- **Partitioning**: different vehicles spread across partitions enable parallel processing.
- **Consumer group**: multiple consumer instances in the same group share partitions and scale throughput.
- **Auto commit vs manual commit**:
  - Auto commit is simple but may acknowledge offsets before business logic fully completes.
  - Manual commit is safer when you need at-least-once delivery guarantees.

## How this maps to a real telematics architecture

| This project | Real system |
| --- | --- |
| Producer | Vehicle gateway / IoT ingestion API |
| Kafka topic | Durable event backbone decoupling producers and consumers |
| Auto-commit consumer | Analytics pipeline or dashboard feed |
| Manual-commit consumer | Time-series DB writer (InfluxDB, TimescaleDB) |
| `vehicleId` message key | Device ID ensuring per-device ordered processing |

## Next steps

Once comfortable here, step up to:

- **[kj-02-web/](../kj-02-web/)** — Spring Boot UIs for producing and consuming events in the browser
- **[kj-03-multicons-base/](../kj-03-multicons-base/)** — multi-consumer platform with alerting, storage, DLQ routing, and always-on simulation
- **[kj-04-multicons-adv/](../kj-04-multicons-adv/)** — advanced failure handling with retry-before-DLQ and a dedicated DLQ viewer
