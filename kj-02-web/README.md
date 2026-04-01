# Vehicle Telemetry — Web App Setup

**Step 2** of three setups in this repo. Two Spring Boot apps wired to Kafka: a producer UI where
you send telemetry events from the browser and a consumer UI where you watch them arrive live.
Introduces Spring Boot, `@KafkaListener`, SSE streaming, and Server-Sent Events — without the
complexity of the full streaming-enhancements stack.

```text
producer-app  →  Kafka (vehicle-telemetry)  →  consumer-app
   :8081                                            :8082
```

> **Note — port assignments:** This setup now uses the standard local ports where possible.
> That makes local defaults simpler, but it also means it will conflict with
> `kafka-java-basic-cli` if both stacks are started at the same time.
>
> | Service | `kafka-java-basic-cli/` | `kafka-java-web-apps/` |
> | --- | --- | --- |
> | Kafka broker (host) | `9092` | `9092` |
> | Kafka UI | `8080` | `8080` |
> | Producer / Consumer | — | `8081` / `8082` |

## Quick start (Docker — everything in one command)

```bash
./run.sh --start
```

| URL | What you see |
| --- | --- |
| [http://localhost:8081](http://localhost:8081) | Producer — compose and send telemetry events |
| [http://localhost:8082](http://localhost:8082) | Consumer — live stream of incoming events |
| [http://localhost:8080](http://localhost:8080) | Kafka UI — explore topics, partitions, offsets |

The topic `vehicle-telemetry` is created automatically on first use (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`).

Stop everything:

```bash
./run.sh --stop
```

## Standalone (no Docker for the apps)

Start only Kafka and Kafka UI:

```bash
docker compose up kafka kafka-ui
```

Then run the apps in separate terminals from the `kafka-java-web-apps/` directory:

```bash
# Terminal 1 — producer on http://localhost:8081
cd producer-app && mvn spring-boot:run

# Terminal 2 — consumer on http://localhost:8082
cd consumer-app && mvn spring-boot:run
```

## How it works

### Producer app (`producer-app/`, port 8081)

- Static HTML form lets you pick a vehicle, adjust speed/fuel/engine status, and click **Send Event**.
- **Randomise & Send** generates a random event instantly — good for bulk testing.
- `POST /api/events` — sends a manually composed JSON body to Kafka.
- `POST /api/events/random` — generates and sends a random event, returns it as JSON.
- Uses `vehicleId` as the Kafka message key to preserve per-vehicle ordering.

### Consumer app (`consumer-app/`, port 8082)

- Connects to `GET /api/events/stream` (Server-Sent Events) immediately on page load.
- A Spring `@KafkaListener` receives every record and pushes the raw JSON to all active SSE clients.
- The browser parses each message and adds a row to the live event table with a flash animation.
- Tracks total events, unique vehicles, and rolling average speed in the stats bar.

## Project structure

```text
kafka-java-web-apps/
├── pom.xml                  parent POM (Spring Boot 3.4.4, Java 17) — 3 modules
├── run.sh
├── docker-compose.yml       orchestrates Kafka + Kafka UI + both apps
├── telemetry-model/         shared library module
│   ├── pom.xml
│   └── src/main/java/…/model/
│       └── TelemetryEvent.java   single source of truth for the POJO
├── producer-app/
│   ├── Dockerfile           multi-stage; build context is kafka-java-web-apps/
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
    ├── Dockerfile           multi-stage; build context is kafka-java-web-apps/
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
| Send events | <http://localhost:8081> → click **Randomise & Send** |
| Live event stream | <http://localhost:8082> → events appear via SSE |
| Topic messages with key/partition/offset | Kafka UI → Topics → vehicle-telemetry → Messages |
| Same vehicleId always on same partition | Partition column in Kafka UI messages view |
| Consumer group lag | Kafka UI → Consumer Groups → `vehicle-telemetry-streaming-web-group` |

## Next steps

For a full streaming platform, step up to **[kafka-java-web-multi-consumer/](../kafka-java-web-multi-consumer/)**, which adds:

- Two additional consumer services (alert + storage) each with their own dashboards
- 8 real-time alert rules with severity classification
- Dead-Letter Queue for poison-pill events
- Always-on vehicle fleet simulator
- Manual commit with retry logic
- Rolling file logs per service
