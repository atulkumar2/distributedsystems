# Streaming Enhancements

**Stage 3** — Full-stack vehicle telemetry Kafka platform. One portal hub, four browser UIs, two stream-processing
backends, a Dead-Letter Queue, an always-on vehicle fleet simulator, 8 real-time alert rules, and
rolling file logs. The stage apps run against the shared repo-level Kafka infrastructure.

---

## Architecture

```text
  [Producer UI :9501]          [Consumer UI :9502]
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
 - Postgres sink             - offline watcher (10 s)
 - manual commit              - auto commit
 - retry 3× on failure        - SSE push to browser
 - speed > 120 → DLQ
 - SSE push to browser
 [Alert UI :9503]             [Storage UI :9504]
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
kj-03-multicons-base/
├── pom.xml                  ← parent POM (9 modules)
├── run.sh
├── docker-compose.yml       ← portal hub + all 4 app services attached to the shared Kafka network
├── README.md
├── model/                   ← TelemetryEvent, DlqEvent (shared POJOs)
├── util/                    ← JsonUtil (singleton ObjectMapper)
├── config/                  ← AppConfig (all constants + env-var overrides)
├── dlq-producer/            ← DlqProducer (shared Kafka producer for DLQ)
├── ../web-apps/portal-hub/  ← Shared static launcher UI :9500 — discovers reachable dashboards
├── alert-consumer/          ← Spring Boot UI :9503 — 8-rule alert dashboard + SSE feed
├── storage-consumer/        ← Spring Boot UI :9504 — Postgres-backed event store dashboard + SSE feed
├── producer-app/            ← Spring Boot UI :9501 — send events + always-on simulator
└── consumer-app/            ← Spring Boot UI :9502 — live event stream + thread pool demo
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

- Shared Kafka broker on `localhost:9092` if it is not already running
- Postgres on `localhost:5432` (`telematics` / `telematics`, database `telemetry`)
- **Portal Hub** at <http://localhost:9500> — shared launcher that shows only reachable dashboards
- Kafka UI at <http://localhost:8080>
- Portainer at <http://localhost:9000>
- **Producer UI** at <http://localhost:9501> — send events or start the always-on simulator
- **Consumer UI** at <http://localhost:9502> — watch events arrive live via SSE
- **Alert Consumer UI** at <http://localhost:9503> — real-time 8-rule alert feed
- **Storage Consumer UI** at <http://localhost:9504> — Postgres-backed latest-state table + SSE feed

### Option B — Run locally (Kafka must already be running)

```bash
cd kj-03-multicons-base

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
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/telemetry \
SPRING_DATASOURCE_USERNAME=telematics \
SPRING_DATASOURCE_PASSWORD=telematics \
java -jar storage-consumer/target/storage-consumer-*.jar
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
| Send manual events | <http://localhost:9501> → click **Randomise & Send** |
| Start always-on simulator | <http://localhost:9501> → **Always-On Vehicles** → set count → Activate |
| Live event stream | <http://localhost:9502> → events appear in real time via SSE |
| Crash / lag simulation | <http://localhost:9502> → configure thread pool with crash enabled |
| Real-time 8-rule alert classifications | <http://localhost:9503> → alert feed |
| Alert severity badges and rule tags | <http://localhost:9503> → CRITICAL / ALERT / WARNING / OK rows |
| Alert metrics (criticals, engine anomalies, sudden changes, offline) | <http://localhost:9503> → metrics bar |
| Postgres-backed latest state per vehicle | <http://localhost:9504> → event store table |
| Storage Consumer SSE feed (STORED / DLQ) | <http://localhost:9504> → live feed panel |
| Storage Consumer metrics | <http://localhost:9504> → metrics bar |
| Query persisted telemetry rows | `psql postgres://telematics:telematics@localhost:5432/telemetry -c "select vehicle_id, count(*) from telemetry_events group by vehicle_id order by vehicle_id;"` |
| Both consumer groups receive same Kafka message | Kafka UI → Topics → vehicle-telemetry → Messages |
| DLQ receives events with speed > 120 | Kafka UI → Topics → vehicle-telemetry-dlq |
| Lag grows when SLOW_MODE=true | Kafka UI → Consumer Groups → telemetry-storage-group |
| Fixed consumer client IDs in broker logs | Kafka UI → Consumers → look for `alert-consumer-1`, `storage-consumer-1` |
| Same vehicleId always on same partition | Kafka UI → Topics → vehicle-telemetry → Messages → Partition column |

### Observing lag step by step

1. Start all services normally
2. Activate the always-on simulator with 5 vehicles from <http://localhost:9501>
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
| `SPRING_DATASOURCE_URL` | `jdbc:postgresql://localhost:5432/telemetry` | Postgres sink JDBC URL |
| `SPRING_DATASOURCE_USERNAME` | `telematics` | Postgres username |
| `SPRING_DATASOURCE_PASSWORD` | `telematics` | Postgres password |

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
| `eventId` unique key + duplicate ignore | Practical idempotent sink for at-least-once Kafka delivery |
| Alert Consumer | Rules engine / CEP (Complex Event Processing) |
| DLQ | Ops alerting + manual replay queue |
| `vehicleId` message key | Device ID ensuring ordered processing per device |
| Fan-out consumer groups | Multiple downstream systems (analytics, billing, safety) |
| AlwaysOnPool simulator | Fleet of always-connected vehicles streaming live telemetry |
| Fixed `client.id` | Stable identity in broker logs and monitoring dashboards |
