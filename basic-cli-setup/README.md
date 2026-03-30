# Vehicle Telemetry Kafka Learning Project (Java + Maven)

A minimal but practical hands-on project to learn Java to Kafka integration using an IoT/telematics scenario.

## What this project demonstrates

- Java Kafka producer
- Java Kafka consumer
- JSON telemetry payloads
- `vehicleId` as Kafka message key
- Standard consumer read/print loop
- Manual offset commit consumer
- Broker endpoint from config file (`src/main/resources/kafka.properties`)
- Kafka UI dashboard via Docker Compose
- Practical comments in code explaining why key settings matter

## Project structure

```text
vehicle-telemetry-kafka/
├── pom.xml
└── src
    └── main
        └── java
            └── com
                └── example
                    └── telematics
                        ├── consumer
                        │   ├── KafkaConsumerExample.java
                        │   └── KafkaManualCommitConsumerExample.java
                        ├── model
                        │   └── TelemetryEvent.java
                        ├── producer
                        │   └── KafkaProducerExample.java
                        └── util
                            └── JsonUtil.java
```

## Prerequisites

- Java 25+
- Maven wrapper included — no separate Maven installation needed (runs Maven 4.0.0 via `./mvnw`)
- Apache Kafka running locally on `localhost:9092`

## Run Kafka locally with Docker Compose

The `docker-compose.yml` starts both Kafka (KRaft, single node) and the
[Kafka UI dashboard](https://github.com/provectus/kafka-ui) with one command:

```bash
docker compose up -d
```

This starts:

- **Kafka** on `localhost:9092` (for the Java app on your host)
- **Kafka UI** at [http://localhost:8080](http://localhost:8080) — browse topics, partitions, offsets,
  consumer groups, and live messages in a web browser

Stop everything:

```bash
docker compose down
```

> **Note:** if you already have a `kafka-local` container running from a
> previous `docker run`, remove it first: `docker rm -f kafka-local`

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

Open http://localhost:8080 once the stack is up.

Useful views for this project:

| UI section | What to look at |
|---|---|
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

1. Start Kafka locally.
2. Create topic `vehicle-telemetry`.
3. Run one consumer (`KafkaConsumerExample`) in terminal A.
4. Run producer (`KafkaProducerExample`) in terminal B.
5. Observe key/value/partition/offset in terminal A.
6. Stop auto-commit consumer and run manual-commit consumer.
7. Run producer again and observe manual commit logs.

## Why key and consumer group choices matter

- `vehicleId` as key: events for each vehicle stay ordered because Kafka maps the same key to the same partition.
- Partitioning: different vehicles can be spread across partitions for parallel processing.
- Consumer group: multiple consumer instances in the same group share partitions and scale processing.
- Auto commit vs manual commit:
  - Auto commit is simple but can acknowledge offsets before your business logic fully completes.
  - Manual commit is safer when you need stronger processing guarantees.

## How this maps to a real telematics architecture

- Producer service: vehicle gateway or ingestion API publishes telemetry to Kafka.
- Kafka topic: durable event backbone (`vehicle-telemetry`) decouples producers and consumers.
- Consumer service: one service can detect over-speeding, another can track fuel anomalies.
- Downstream analytics: stream processor or data lake pipeline builds dashboards, alerts, and fleet insights.

## Notes for learning

- This project intentionally uses plain Java with minimal abstractions.
- JSON is sent as plain string to keep focus on Kafka fundamentals.
- Once comfortable, add schema management, retries/backoff tuning, and error handling patterns.
