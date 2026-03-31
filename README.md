# Vehicle Telemetry Kafka — Learning Project

Two self-contained setups for learning Java + Apache Kafka integration using an IoT/vehicle-telematics scenario.

| Setup | What it is |
|---|---|
| [basic-cli/](basic-cli/) | Plain Java CLI producer and consumer — minimal, no frameworks, great starting point |
| [web-apps/](web-apps/) | Spring Boot web apps — generate events from a browser form, watch them appear live in another tab |

Both setups can run **simultaneously** — they use different ports to avoid conflicts:

| Service | `basic-cli/` | `web-apps/` |
|---|---|---|
| Kafka broker (host) | `9092` | `9093` |
| Kafka UI | `8080` | `8083` |
| Producer / Consumer | CLI only | `8081` / `8082` |

## Prerequisites

- Docker & Docker Compose (both setups use Compose to start Kafka)
- Java 25+
- Maven wrapper included in each setup — no separate Maven installation needed (runs Maven 4.0.0 via `./mvnw`)
