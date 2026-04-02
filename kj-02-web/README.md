# Vehicle Telemetry — Web App Setup

**Stage 2** of four setups in this repo. Two Spring Boot apps wired to Kafka: a producer UI where
you send telemetry events from the browser and a consumer UI where you watch them arrive live.
Introduces Spring Boot, `@KafkaListener`, SSE streaming, and Server-Sent Events — without the
complexity of the full multi-consumer platform.

```text
producer-app  →  Kafka (vehicle-telemetry)  →  consumer-app
   :9501                                            :9502
```

> **Note — shared infra:** Kafka, Kafka UI, and Portainer now run from the repo-level
> [`infra/`](../infra/) stack. `./run.sh --start` brings that shared infra up first, then
> starts just the producer and consumer apps for this stage.
>
> | Service | `infra/` | `kj-02-web/` |
> | --- | --- | --- |
> | Kafka broker (host) | `9092` | shared |
> | Kafka UI | `8080` | shared |
> | Portainer | `9000` / `9443` | shared |
> | Portal Hub | — | `9500` |
> | Producer / Consumer | — | `9501` / `9502` |

## Quick start (Docker — everything in one command)

```bash
./run.sh --start
```

| URL | What you see |
| --- | --- |
| [http://localhost:9500](http://localhost:9500) | Portal Hub — launch the Stage 2 apps from one page |
| [http://localhost:9501](http://localhost:9501) | Producer — compose and send telemetry events |
| [http://localhost:9502](http://localhost:9502) | Consumer — live stream of incoming events |
| [http://localhost:8080](http://localhost:8080) | Kafka UI — explore topics, partitions, offsets |
| [http://localhost:9000](http://localhost:9000) | Portainer — inspect the shared local Docker stack |

The topic `vehicle-telemetry` is created automatically on first use (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`).

Stop everything:

```bash
./run.sh --stop
```

## Standalone (no Docker for the apps)

Start only the shared infra:

```bash
cd ../infra && ./run.sh --start
```

Then run the apps in separate terminals from the `kj-02-web/` directory:

```bash
# Terminal 1 — producer on http://localhost:9501
cd producer-app && mvn spring-boot:run

# Terminal 2 — consumer on http://localhost:9502
cd consumer-app && mvn spring-boot:run
```

## How it works

### Producer app (`producer-app/`, port 9501)

- Static HTML form lets you pick a vehicle, adjust speed/fuel/engine status, and click **Send Event**.
- **Randomise & Send** generates a random event instantly — good for bulk testing.
- `POST /api/events` — sends a manually composed JSON body to Kafka.
- `POST /api/events/random` — generates and sends a random event, returns it as JSON.
- Uses `vehicleId` as the Kafka message key to preserve per-vehicle ordering.

### Consumer app (`consumer-app/`, port 9502)

- Connects to `GET /api/events/stream` (Server-Sent Events) immediately on page load.
- A Spring `@KafkaListener` receives every record and pushes the raw JSON to all active SSE clients.
- The browser parses each message and adds a row to the live event table with a flash animation.
- Tracks total events, unique vehicles, and rolling average speed in the stats bar.

## Project structure

```text
kj-02-web/
├── pom.xml                  parent POM (Spring Boot 3.4.4, Java 17) — 3 modules
├── run.sh
├── docker-compose.yml       stage app containers on the shared Kafka network
├── telemetry-model/         shared library module
│   ├── pom.xml
│   └── src/main/java/…/model/
│       └── TelemetryEvent.java   single source of truth for the POJO
├── producer-app/
│   ├── Dockerfile           multi-stage; build context is kj-02-web/
│   ├── pom.xml              depends on telemetry-model
│   └── src/main/
│       ├── java/…/producer/
│       │   ├── ProducerApplication.java
│       │   ├── TelemetryProducer.java   (KafkaTemplate wrapper)
│       │   └── ProducerController.java  (REST endpoints)
│       └── resources/
│           ├── application.properties
│           └── static/index.html        (vanilla HTML/JS form)
└── consumer-app/
    ├── Dockerfile           multi-stage; build context is kj-02-web/
    ├── pom.xml
    └── src/main/
        ├── java/…/consumer/
        │   ├── ConsumerApplication.java
        │   ├── TelemetryConsumer.java   (@KafkaListener + SSE emitter list)
        │   └── ConsumerController.java  (SSE endpoint)
        └── resources/
            ├── application.properties
            └── static/index.html        (live event table via EventSource)
```

## What to observe

| What | Where to look |
| --- | --- |
| Launch stage apps | <http://localhost:9500> → open Producer or Consumer |
| Send events | <http://localhost:9501> → click **Randomise & Send** |
| Live event stream | <http://localhost:9502> → events appear via SSE |
| Topic messages with key/partition/offset | Kafka UI → Topics → vehicle-telemetry → Messages |
| Same vehicleId always on same partition | Partition column in Kafka UI messages view |
| Consumer group lag | Kafka UI → Consumer Groups → `vehicle-telemetry-streaming-web-group` |

## Next steps

For a full streaming platform, step up to **[kj-03-multicons-base/](../kj-03-multicons-base/)**, which adds:

- Two additional consumer services (alert + storage) each with their own dashboards
- 8 real-time alert rules with severity classification
- Dead-Letter Queue for poison-pill events
- Always-on vehicle fleet simulator
- Manual commit with retry logic
- Rolling file logs per service
