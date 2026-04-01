# Streaming Enhancements

**Step 3** — Full-stack vehicle telemetry Kafka platform. Four browser UIs, two stream-processing
backends, a Dead-Letter Queue, an always-on vehicle fleet simulator, 8 real-time alert rules, and
rolling file logs. All in one self-contained Docker Compose stack.

---

## Architecture

```text
  [Producer UI :8081]          [Consumer UI :8082]
  producer-app                 consumer-app
  - form / random send         - SSE live stream
  - AlwaysOnPool simulator     - configurable thread pool
  - KafkaTemplate              - crash / lag simulation
  - client-id: telemetry-      - client-id: consumer-worker-N
    producer-1
         │                            ▲
         ▼  key = vehicleId           │ (fan-out group)
  ┌─────────────────────┐            │
  │  vehicle-telemetry  │ ───────────┘
  └──────────┬──────────┘  ← Kafka topic (3 partitions)
             │ fan-out: all groups receive every message independently
     ┌────────┴────────┐
     ▼                 ▼
[telemetry-storage-group]    [telemetry-alert-group]
 StorageService               AlertService
 client-id:                   client-id:
  storage-consumer-1           alert-consumer-1
 - parse JSON                 - 8 alert rules (see below)
 - in-memory store            - offline watcher (10 s)
 - manual commit              - auto commit
 - retry 3× on failure        - SSE push to browser
 - speed > 120 → DLQ
 - SSE push to browser
 [Storage UI :8085]           [Alert UI :8086]
        │
        ▼
 ┌─────────────────────────┐
 │  vehicle-telemetry-dlq  │  ← failed events with error reason
 └─────────────────────────┘
```

### 8 alert rules (Alert Consumer)

| Priority | Rule | Condition | Type |
| --- | --- | --- | --- |
| 1 | `critical-speed` | speed > 120 km/h | CRITICAL |
| 2 | `overspeed` | speed > 100 km/h | ALERT |
| 3 | `critical-fuel` | fuel < 10 % | CRITICAL |
| 4 | `low-fuel` | fuel < 20 % | WARNING |
| 5 | `engine-anomaly` | engineStatus not ON or OFF | WARNING |
| 6 | `sudden-change` | speed delta > 40 km/h vs. previous event | WARNING |
| 7 | `geofence` | lat/lng outside Bengaluru bounding box | WARNING |
| 8 | `vehicle-offline` | vehicle silent for > 10 s | WARNING |
| — | `nominal` | none of the above | OK |

### Key concepts explained

#### Partitions

A Kafka topic is split into N partitions. Each partition is an ordered, immutable log.
Multiple consumers in a group can read different partitions in parallel — more partitions
= higher throughput. `vehicle-telemetry` has 3 partitions.

#### Same key → same partition

The producer uses `vehicleId` as the message key. Kafka's default hash-partitioner
maps the same key to the same partition deterministically — all events for
`VH-1004` always land in partition N, preserving per-vehicle ordering.

#### Fan-out pattern

`telemetry-storage-group` and `telemetry-alert-group` both subscribe to the same topic.
Kafka delivers every message to **both** groups independently — neither consumer steals
messages from the other.

#### Manual offset commit (Storage Consumer)

`enable.auto.commit=false` + `commitSync()` after successful processing gives _at-least-once_
delivery: if the process crashes before committing, Kafka re-delivers the record on restart.
The DLQ prevents a bad message from blocking the consumer indefinitely.

#### Dead-Letter Queue (DLQ)

Events that exceed the failure threshold (speed > 120) or exhaust all retries are
published to `vehicle-telemetry-dlq` with an error reason attached as metadata.

#### Consumer lag

Lag = (latest offset in partition) − (committed offset for the consumer group).
Non-zero lag means the consumer is falling behind producers. Enable `SLOW_MODE=true`
on storage-consumer to watch lag grow in the Kafka UI.

#### Always-On vehicle simulator (Producer UI)

`AlwaysOnPool` runs N virtual threads (one per vehicle) that continuously evolve and
publish telemetry at 1-second intervals. Each thread has its own lifecycle — vehicles
can be paused, frozen, or stopped individually. `activate(count)` is idempotent —
it targets a total active count, not "add N more".

#### Fixed consumer client IDs

Every consumer and producer sets a stable `client.id` in Kafka props. This makes the
client appear under a meaningful name in broker logs and consumer-lag metrics tools
instead of an auto-assigned random UUID.

---

## Project structure

```text
kafka-java-web-multi-consumer/
├── pom.xml                  ← parent POM (9 modules)
├── run.sh
├── docker-compose.yml       ← Kafka + Kafka UI + all 4 app services
├── README.md
├── model/                   ← TelemetryEvent, DlqEvent (shared POJOs)
├── util/                    ← JsonUtil (singleton ObjectMapper)
├── config/                  ← AppConfig (all constants + env-var overrides)
├── dlq-producer/            ← DlqProducer (shared Kafka producer for DLQ)
├── storage-consumer/        ← Spring Boot UI :8085 — event store dashboard + SSE feed
├── alert-consumer/          ← Spring Boot UI :8086 — 8-rule alert dashboard + SSE feed
├── producer-app/            ← Spring Boot UI :8081 — send events + always-on simulator
└── consumer-app/            ← Spring Boot UI :8082 — live event stream + thread pool demo
```

---

## How to Run

### Option A — Docker Compose (recommended)

```bash
./run.sh --start
```

Detached mode:

```bash
./run.sh --start --detach
```

Slow mode for lag simulation:

```bash
./run.sh --start --slow
```

This starts:

- Kafka (port `9092` on host)
- Kafka UI at <http://localhost:8080>
- **Producer UI** at <http://localhost:8081> — send events or start the always-on simulator
- **Consumer UI** at <http://localhost:8082> — watch events arrive live via SSE
- **Storage Consumer UI** at <http://localhost:8085> — in-memory event store table + SSE feed
- **Alert Consumer UI** at <http://localhost:8086> — real-time 8-rule alert feed

### Option B — Run locally (Kafka must already be running)

```bash
cd kafka-java-web-multi-consumer

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
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 java -jar storage-consumer/target/storage-consumer-*.jar
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 java -jar alert-consumer/target/alert-consumer-*.jar
SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092 java -jar producer-app/target/producer-app-*.jar
```

Enable slow mode (lag simulation):

```bash
SLOW_MODE=true java -jar storage-consumer/target/storage-consumer-*.jar
```

---

## Log files

Each service writes to both stdout (console) and its own rolling log files under `logs/`.

| Service | Generic app log | Focused Kafka log |
| --- | --- | --- |
| `producer-app` | `logs/producer-app.log` | `logs/producer-kafka.log` — only the `producer` package |
| `consumer-app` | `logs/consumer-app.log` | `logs/consumer-kafka.log` — only the `consumer` package |
| `alert-consumer` | `logs/alert-consumer.log` | — |
| `storage-consumer` | `logs/storage-consumer.log` | — |

Rolling policy: 10 MB per file, daily rotation, 7-day history, gzip-compressed archives, 100 MB total cap per service.

---

## What to Observe

| What | Where to look |
| --- | --- |
| Send manual events | <http://localhost:8081> → click **Randomise & Send** |
| Start always-on simulator | <http://localhost:8081> → **Always-On Vehicles** → set count → Activate |
| Live event stream | <http://localhost:8082> → events appear in real time via SSE |
| Crash / lag simulation | <http://localhost:8082> → configure thread pool with crash enabled |
| In-memory event store (latest state per vehicle) | <http://localhost:8085> → event store table |
| Storage Consumer SSE feed (STORED / DLQ) | <http://localhost:8085> → live feed panel |
| Storage Consumer metrics | <http://localhost:8085> → metrics bar |
| Real-time 8-rule alert classifications | <http://localhost:8086> → alert feed |
| Alert severity badges and rule tags | <http://localhost:8086> → CRITICAL / ALERT / WARNING / OK rows |
| Alert metrics (criticals, engine anomalies, sudden changes, offline) | <http://localhost:8086> → metrics bar |
| Both consumer groups receive same Kafka message | Kafka UI → Topics → vehicle-telemetry → Messages |
| DLQ receives events with speed > 120 | Kafka UI → Topics → vehicle-telemetry-dlq |
| Lag grows when SLOW_MODE=true | Kafka UI → Consumer Groups → telemetry-storage-group |
| Fixed consumer client IDs in broker logs | Kafka UI → Consumers → look for `alert-consumer-1`, `storage-consumer-1` |
| Same vehicleId always on same partition | Kafka UI → Topics → vehicle-telemetry → Messages → Partition column |

### Observing lag step by step

1. Start all services normally
2. Activate the always-on simulator with 5 vehicles from <http://localhost:8081>
3. Observe lag ≈ 0 (consumers keep up)
4. Restart the stack with `./run.sh --start --slow --detach`
5. Watch lag grow in Kafka UI → Consumer Groups → `telemetry-storage-group`

---

## Configuration reference

All values live in `config/src/main/java/com/example/telematics/config/AppConfig.java`.
Override at runtime via environment variables:

| Env var | Default | Description |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `SLOW_MODE` | `false` | Set `true` to sleep 200–500 ms per message in storage-consumer |

Thresholds (change in source only):

| Constant | Value | Rule triggered |
| --- | --- | --- |
| `SPEED_ALERT_THRESHOLD` | 100 km/h | `overspeed` ALERT |
| `SPEED_CRITICAL_THRESHOLD` | 120 km/h | `critical-speed` CRITICAL; also routes to DLQ in storage-consumer |
| `FUEL_WARN_THRESHOLD` | 20 % | `low-fuel` WARNING |
| `FUEL_CRITICAL_THRESHOLD` | 10 % | `critical-fuel` CRITICAL |
| `SPEED_SUDDEN_CHANGE_KMH` | 40 km/h | `sudden-change` WARNING |
| `GEO_LAT_MIN/MAX` | 12.88–13.12 | Bengaluru geofence latitude bounds |
| `GEO_LNG_MIN/MAX` | 77.48–77.72 | Bengaluru geofence longitude bounds |
| `VEHICLE_OFFLINE_SECONDS` | 10 s | `vehicle-offline` WARNING |
| `MAX_RETRIES` | 3 | Retries before routing to DLQ |

---

## Real-world telematics mapping

| This project | Real system |
| --- | --- |
| `vehicle-telemetry` topic | MQTT broker / raw IoT ingest topic |
| Storage Consumer | Time-series database writer (InfluxDB, TimescaleDB) |
| Alert Consumer | Rules engine / CEP (Complex Event Processing) |
| DLQ | Ops alerting + manual replay queue |
| `vehicleId` message key | Device ID ensuring ordered processing per device |
| Fan-out consumer groups | Multiple downstream systems (analytics, billing, safety) |
| AlwaysOnPool simulator | Fleet of always-connected vehicles streaming live telemetry |
| Fixed `client.id` | Stable identity in broker logs and monitoring dashboards |
