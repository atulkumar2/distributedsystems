# Vehicle Telemetry — Web App Setup

Two Spring Boot apps wired to Kafka: a producer UI where you send telemetry events from your browser and a consumer UI where you watch them arrive in real-time.

```
producer-app  →  Kafka (vehicle-telemetry)  →  consumer-app
   :8081                                            :8082
```

## Quick start (Docker — everything in one command)

```bash
docker compose up --build
```

| URL | What you see |
|---|---|
| http://localhost:8081 | Producer — compose and send telemetry events |
| http://localhost:8082 | Consumer — live stream of incoming events |
| http://localhost:8080 | Kafka UI — explore topics, partitions, offsets |

The topic `vehicle-telemetry` is created automatically on first use (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`).

Stop everything:

```bash
docker compose down
```

## Standalone (no Docker for the apps)

Start only Kafka and Kafka UI:

```bash
docker compose up kafka kafka-ui
```

Then run the apps in separate terminals:

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

```
web-app-setup/
├── docker-compose.yml       orchestrates Kafka + both apps
├── producer-app/
│   ├── Dockerfile
│   ├── pom.xml              Spring Boot 3.4.4, Java 25
│   └── src/main/
│       ├── java/…/producer/
│       │   ├── ProducerApplication.java
│       │   ├── TelemetryEvent.java
│       │   ├── TelemetryProducer.java   (KafkaTemplate wrapper)
│       │   └── ProducerController.java  (REST endpoints)
│       └── resources/
│           ├── application.properties
│           └── static/index.html        (vanilla HTML/JS form)
└── consumer-app/
    ├── Dockerfile
    ├── pom.xml              Spring Boot 3.4.4, Java 25
    └── src/main/
        ├── java/…/consumer/
        │   ├── ConsumerApplication.java
        │   ├── TelemetryEvent.java
        │   ├── TelemetryConsumer.java   (@KafkaListener + SSE emitter list)
        │   └── ConsumerController.java  (SSE endpoint)
        └── resources/
            ├── application.properties
            └── static/index.html        (live event table via EventSource)
```
