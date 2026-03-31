# Streaming Enhancements

Full-stack vehicle telemetry Kafka system — four browser UIs, backend stream processors,
and a Dead-Letter Queue, all in one self-contained Docker Compose stack.

---

## Architecture

```text
  [Producer UI :8081]          [Consumer UI :8082]
  producer-app                 consumer-app
  - form / random send         - SSE live stream
  - KafkaTemplate              - Spring @KafkaListener
         │                            ▲
         ▼  key = vehicleId           │ (fan-out group)
  ┌─────────────────────┐            │
  │  vehicle-telemetry  │ ───────────┘
  └──────────┬──────────┘  ← Kafka topic (3 partitions)
             │ fan-out: all groups receive every message independently
     ┌────────┴────────┐
     ▼                 ▼
[telemetry-storage-group]   [telemetry-alert-group]
  StorageConsumer             AlertConsumer
  - parse JSON                - speed > 100 → ALERT
  - in-memory store           - fuel  < 20  → WARNING
  - manual commit             - auto commit
  - retry 3x on failure       - SSE push to browser
  - speed > 120 → DLQ
  - SSE push to browser
  [Storage UI :8085]          [Alert UI :8086]
         │
         ▼
  ┌─────────────────────────┐
  │  vehicle-telemetry-dlq  │  ← failed events with error reason
  └─────────────────────────┘
```

### Key concepts explained

#### Partitions

A Kafka topic is split into N partitions. Each partition is an ordered, immutable log.
Multiple consumers in a group can read different partitions in parallel — more partitions
= higher throughput. Use `vehicle-telemetry` which has 3 partitions.

#### Same key → same partition

The producer uses `vehicleId` as the message key. Kafka's default hash-partitioner
maps the same key to the same partition deterministically. This means all events for
`VH-1004` always land in the same partition, preserving per-vehicle ordering without
any coordination.

#### Fan-out pattern

Two different consumer group IDs (`telemetry-storage-group`, `telemetry-alert-group`)
subscribe to the same topic. Kafka delivers every message to **both** groups independently.
Neither consumer "steals" messages from the other.

#### Manual offset commit (Storage Consumer)

`enable.auto.commit=false` + `commitSync()` after successful processing.
This gives *at-least-once* delivery: if the process crashes before committing, Kafka
re-delivers the record on restart. The DLQ ensures even records that always fail are
not replayed forever.

#### Dead-Letter Queue (DLQ)

Events that exceed the failure threshold (speed > 120) or fail all retries are
published to `vehicle-telemetry-dlq` with an error reason. This prevents a bad
message from blocking the consumer indefinitely (poison-pill problem).

#### Consumer lag

Lag = (latest offset in partition) − (committed offset for the consumer group).
Non-zero lag means the consumer is falling behind producers. Enable `SLOW_MODE=true`
to watch lag grow in the Kafka UI.

---

## Project structure

```text
streaming-enhancements/
├── pom.xml                  ← parent POM (9 modules)
├── docker-compose.yml       ← Kafka + Kafka UI + all 4 app services
├── README.md
├── model/                   ← TelemetryEvent, DlqEvent (shared POJOs)
├── util/                    ← JsonUtil (singleton ObjectMapper)
├── config/                  ← AppConfig (all constants + env-var overrides)
├── dlq-producer/            ← DlqProducer (shared Kafka producer for DLQ)
├── storage-consumer/        ← Spring Boot UI :8085 — event store dashboard + SSE feed
├── alert-consumer/          ← Spring Boot UI :8086 — alert feed dashboard + SSE feed
├── producer-app/            ← Spring Boot UI :8081 — send events via browser
└── consumer-app/            ← Spring Boot SSE UI :8082 — live event display
```

---

## How to Run

### Option A — Docker Compose (recommended)

```bash
cd streaming-enhancements/
docker compose up --build
```

This starts:

- Kafka (port `9094` on host)
- Kafka UI at <http://localhost:8084>
- **Producer UI** at <http://localhost:8081> — send events from the browser
- **Consumer UI** at <http://localhost:8082> — watch events arrive live via SSE
- **Storage Consumer UI** at <http://localhost:8085> — in-memory event store table + SSE feed
- **Alert Consumer UI** at <http://localhost:8086> — live ALERT / WARNING / OK feed

Open <http://localhost:8081> and click **Randomise & Send** a few times, then switch
to <http://localhost:8082> to see those events arrive live. Open <http://localhost:8085>
for the event store dashboard and <http://localhost:8086> for the real-time alert feed.

### Option B — Run locally (Kafka must already be running)

```bash
cd streaming-enhancements/

# Build all modules
mvn clean package -DskipTests

# Terminal 1 — Storage Consumer
java -jar storage-consumer/target/storage-consumer-*.jar

# Terminal 2 — Alert Consumer
java -jar alert-consumer/target/alert-consumer-*.jar

# Terminal 3 — Producer UI
java -jar producer-app/target/producer-app-*.jar

# Terminal 4 — Consumer UI
java -jar consumer-app/target/consumer-app-*.jar
```

Override Kafka address:

```bash
# All four apps are Spring Boot — use KAFKA_BOOTSTRAP_SERVERS (read by AppConfig):
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 java -jar storage-consumer/target/storage-consumer-*.jar
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 java -jar alert-consumer/target/alert-consumer-*.jar
# producer-app and consumer-app also accept the Spring Boot property:
SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092 java -jar producer-app/target/producer-app-*.jar
```

Enable slow mode (lag simulation):

```bash
SLOW_MODE=true java -jar storage-consumer/target/storage-consumer-*.jar
```

---

## What to Observe

| What | Where to look |
|---|---|
| Send events from browser | <http://localhost:8081> → click **Randomise & Send** |
| Live event stream in browser | <http://localhost:8082> → events appear in real time via SSE |
| In-memory event store (latest state per vehicle) | <http://localhost:8085> → event store table |
| Storage Consumer SSE feed (STORED / DLQ status) | <http://localhost:8085> → live feed panel |
| Storage Consumer metrics (processed / failures / store size) | <http://localhost:8085> → metrics bar |
| Real-time alert classification (ALERT / WARNING / OK) | <http://localhost:8086> → alert feed |
| Alert Consumer metrics (processed / alerts / warnings) | <http://localhost:8086> → metrics bar |
| Both consumer groups receive same Kafka message | Kafka UI → Topics → vehicle-telemetry → Messages |
| DLQ receives events with speed > 120 | Kafka UI → Topics → vehicle-telemetry-dlq |
| Lag grows when SLOW_MODE=true | Kafka UI → Consumer Groups → telemetry-storage-group |
| Same vehicleId always on same partition | Kafka UI → Topics → vehicle-telemetry → Messages → Partition column |

### Observing lag step by step

1. Start both consumers normally (`SLOW_MODE=false`)
2. Send ~20 events quickly from the UI producer
3. Observe lag = 0 (consumers keep up)
4. Restart storage-consumer with `SLOW_MODE=true`
5. Send another 20 events
6. Open Kafka UI → Consumer Groups → `telemetry-storage-group` → watch lag increase

---

## Configuration reference

All values live in `config/src/main/java/com/example/telematics/config/AppConfig.java`.
Override at runtime via environment variables:

| Env var | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `SLOW_MODE` | `false` | Set `true` to sleep 200–500 ms per message |

Thresholds (change in source only):

| Constant | Value | Meaning |
|---|---|---|
| `SPEED_ALERT_THRESHOLD` | 100 km/h | Alert Consumer raises ALERT |
| `SPEED_FAILURE_THRESHOLD` | 120 km/h | Storage Consumer simulates failure → DLQ |
| `FUEL_WARN_THRESHOLD` | 20 % | Alert Consumer raises WARNING |
| `MAX_RETRIES` | 3 | Retries before routing to DLQ |

---

## Real-world telematics mapping

| This project | Real system |
|---|---|
| `vehicle-telemetry` topic | MQTT broker / raw IoT ingest topic |
| Storage Consumer | Time-series database writer (InfluxDB, TimescaleDB) |
| Alert Consumer | Rules engine / CEP (Complex Event Processing) |
| DLQ | Ops alerting + manual replay queue |
| `vehicleId` message key | Device ID ensuring ordered processing per device |
| Fan-out consumer groups | Multiple downstream systems (analytics, billing, safety) |
