# Vehicle Telemetry Kafka — Learning Project

Three self-contained setups for learning Java + Apache Kafka integration using an IoT/vehicle-telematics scenario.
Each builds on the previous one, progressing from raw CLI fundamentals to a full multi-service streaming platform.

| Setup | What it is |
|---|---|
| [basic-cli/](basic-cli/) | Plain Java CLI producer and consumer — minimal, no frameworks, great starting point |
| [web-apps/](web-apps/) | Spring Boot web apps — generate events from a browser, watch them arrive live in another tab |
| [streaming-enhancements/](streaming-enhancements/) | Full multi-service platform — 4 UIs, alert engine (8 rules), storage consumer, DLQ, always-on vehicle simulators, rolling file logs |

All three setups can run **simultaneously** — they use different ports to avoid conflicts:

| Service | `basic-cli/` | `web-apps/` | `streaming-enhancements/` |
|---|---|---|---|
| Kafka broker (host) | `9092` | `9093` | `9094` |
| Kafka UI | `8080` | `8083` | `8084` |
| Producer UI | CLI only | `8081` | `8081` |
| Consumer UI | CLI only | `8082` | `8082` |
| Storage Consumer UI | — | — | `8085` |
| Alert Consumer UI | — | — | `8086` |

## Prerequisites

- Docker & Docker Compose (all setups use Compose to start Kafka)
- Java 17+
- Maven wrapper included in each setup — no separate Maven installation needed (runs Maven 4.0.0 via `./mvnw`)
