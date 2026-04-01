# Vehicle Telemetry Kafka Learning Repo

Three self-contained Java + Apache Kafka projects built around the same vehicle-telematics domain.
They progress from Kafka fundamentals to browser-based apps and then to a fuller multi-consumer
streaming platform.

## Repo layout

| Folder | What it contains | Main entry point |
| --- | --- | --- |
| [`kafka-java-basic-cli/`](./kafka-java-basic-cli/) | Step 1: plain Java CLI producer and consumer examples, including manual offset commit | [`run.sh`](/home/atul-kumar/workspace/distributedsystems/kafka-java-basic-cli/run.sh) |
| [`kafka-java-web-apps/`](./kafka-java-web-apps/) | Step 2: Spring Boot producer and consumer web apps with live SSE streaming | [`run.sh`](/home/atul-kumar/workspace/distributedsystems/kafka-java-web-apps/run.sh) |
| [`kafka-java-web-multi-consumer/`](./kafka-java-web-multi-consumer/) | Step 3: full multi-consumer platform with storage UI, alert UI, DLQ, and always-on simulator | [`run.sh`](/home/atul-kumar/workspace/distributedsystems/kafka-java-web-multi-consumer/run.sh) |

## What each project teaches

| Step | Focus | Highlights |
| --- | --- | --- |
| 1 | Kafka fundamentals | plain producer/consumer, keys, partitions, consumer groups, manual commit |
| 2 | Spring Boot + web integration | REST endpoints, KafkaTemplate, `@KafkaListener`, Server-Sent Events |
| 3 | Streaming architecture | fan-out consumer groups, DLQ, retry behavior, alert rules, consumer lag, multiple dashboards |

## Ports

These projects are designed so their Docker stacks can run side by side without port collisions.

| Service | `kafka-java-basic-cli` | `kafka-java-web-apps` | `kafka-java-web-multi-consumer` |
| --- | --- | --- | --- |
| Kafka broker (host) | `9092` | `9093` | `9094` |
| Kafka UI | `8080` | `8083` | `8084` |
| Producer UI | — | `8081` | `8081` |
| Consumer UI | — | `8082` | `8082` |
| Storage Consumer UI | — | — | `8085` |
| Alert Consumer UI | — | — | `8086` |

## Quick start

### Step 1: CLI Kafka example

```bash
cd kafka-java-basic-cli
./run.sh --start
```

This starts Kafka and Kafka UI, prepares the topic, and compiles the Java project.

### Step 2: Web apps

```bash
cd kafka-java-web-apps
./run.sh --start
```

This starts Kafka, Kafka UI, the producer UI, and the consumer UI.

### Step 3: Multi-consumer platform

```bash
cd kafka-java-web-multi-consumer
./run.sh --start
```

This starts Kafka, Kafka UI, producer UI, consumer UI, storage consumer UI, and alert consumer UI.

## Prerequisites

- Docker with `docker compose`
- Java 17+ for local non-Docker runs
- The Maven wrapper included in each project when building locally

## Recommended learning order

1. Start with [`kafka-java-basic-cli/`](./kafka-java-basic-cli/) to understand Kafka keys, partitions, and commits.
2. Move to [`kafka-java-web-apps/`](./kafka-java-web-apps/) to see Kafka integrated with Spring Boot and browser UIs.
3. Finish with [`kafka-java-web-multi-consumer/`](./kafka-java-web-multi-consumer/) for a more realistic streaming setup with multiple consumer groups and operational behavior.

## Project docs

- [`kafka-java-basic-cli/README.md`](/home/atul-kumar/workspace/distributedsystems/kafka-java-basic-cli/README.md)
- [`kafka-java-web-apps/README.md`](/home/atul-kumar/workspace/distributedsystems/kafka-java-web-apps/README.md)
- [`kafka-java-web-multi-consumer/README.md`](/home/atul-kumar/workspace/distributedsystems/kafka-java-web-multi-consumer/README.md)
