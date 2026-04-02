# Vehicle Telemetry Kafka Learning Repo

Four Java + Apache Kafka learning projects built around the same vehicle telemetry domain. The repo
progresses from plain Java Kafka clients to Spring Boot web apps, then to multi-consumer streaming
architectures with alerting, storage, and DLQ handling.

All stages now share a common infra stack in [`infra/`](./infra/): Kafka, Kafka UI, and Portainer
run on a shared Docker network, while each stage starts only its own app containers.

## Repo layout

| Folder | Stage | What it contains | Main entry point |
| --- | --- | --- | --- |
| [`kj-01-cli-base/`](./kj-01-cli-base/) | 1 | Plain Java producer and consumer examples, including manual offset commits | [`run.sh`](/home/atul-kumar/workspace/distributedsystems/kj-01-cli-base/run.sh) |
| [`kj-02-web/`](./kj-02-web/) | 2 | Spring Boot producer and consumer web apps with live SSE streaming | [`run.sh`](/home/atul-kumar/workspace/distributedsystems/kj-02-web/run.sh) |
| [`kj-03-multicons-base/`](./kj-03-multicons-base/) | 3 | Multi-consumer platform with alert dashboards, a Postgres storage sink, idempotent inserts, and an always-on simulator | [`run.sh`](/home/atul-kumar/workspace/distributedsystems/kj-03-multicons-base/run.sh) |
| [`kj-04-multicons-adv/`](./kj-04-multicons-adv/) | 4 | Advanced multi-consumer platform with a Postgres storage sink, retry-before-DLQ behavior, idempotent inserts, and a dedicated DLQ viewer UI | [`run.sh`](/home/atul-kumar/workspace/distributedsystems/kj-04-multicons-adv/run.sh) |
| [`infra/`](./infra/) | Shared | Common Kafka broker, Kafka UI, and Portainer for all stages | [`run.sh`](/home/atul-kumar/workspace/distributedsystems/infra/run.sh) |
| [`web-apps/`](./web-apps/) | Shared | Reusable browser UI assets, including the common portal hub | — |

## What each stage teaches

| Stage | Focus | Highlights |
| --- | --- | --- |
| 1 | Kafka fundamentals | plain producer/consumer, partitions, keys, consumer groups, manual commit |
| 2 | Spring Boot + browser integration | REST endpoints, `KafkaTemplate`, `@KafkaListener`, Server-Sent Events |
| 3 | Multi-consumer fan-out | independent consumer groups, alerting, Kafka-to-Postgres sink, idempotent writes, always-on simulation, DLQ basics |
| 4 | Failure handling and operations | retry with backoff, Kafka-to-Postgres sink, idempotent writes, direct-vs-retry DLQ routing, DLQ inspection UI, lag simulation |

## Quick start

### Shared infra

```bash
cd infra
./run.sh --start
```

### Stage 1: CLI Kafka example

```bash
cd kj-01-cli-base
./run.sh --start
```

### Stage 2: Web apps

```bash
cd kj-02-web
./run.sh --start
```

### Stage 3: Multi-consumer base

```bash
cd kj-03-multicons-base
./run.sh --start
```

### Stage 4: Multi-consumer advanced

```bash
cd kj-04-multicons-adv
./run.sh --start
```

## Ports

The shared infra owns the common Kafka and tooling ports. Stage stacks add only their own app UIs,
so multiple stages can share the same broker if their app ports do not collide.

| Stack | Kafka broker | Kafka UI | Portainer | App UIs |
| --- | --- | --- | --- |
| `infra` | `9092` | `8080` | `9000` / `9443` | — |
| `kj-01-cli-base` | shared | shared | shared | — |
| `kj-02-web` | shared | shared | shared | hub `9500`, producer `9501`, consumer `9502` |
| `kj-03-multicons-base` | shared | shared | shared | hub `9500`, producer `9501`, consumer `9502`, alert `9503`, storage `9504` |
| `kj-04-multicons-adv` | shared | shared | shared | hub `9500`, producer `9501`, consumer `9502`, alert `9503`, storage `9504`, DLQ viewer `9505` |

## Prerequisites

- Docker with `docker compose`
- Java 17+ for local non-Docker runs
- Maven or the Maven wrapper included in the individual projects

## Recommended learning order

1. Start with [`kj-01-cli-base/`](./kj-01-cli-base/) to learn the core Kafka producer and consumer mechanics.
2. Move to [`kj-02-web/`](./kj-02-web/) to see Spring Boot apps produce and stream events to the browser.
3. Continue with [`kj-03-multicons-base/`](./kj-03-multicons-base/) to learn Kafka fan-out and multiple consumer groups.
4. Finish with [`kj-04-multicons-adv/`](./kj-04-multicons-adv/) for more realistic failure handling, Postgres-backed storage, retries, DLQ inspection, and operational behavior.

## Project docs

- [`kj-01-cli-base/README.md`](/home/atul-kumar/workspace/distributedsystems/kj-01-cli-base/README.md)
- [`kj-02-web/README.md`](/home/atul-kumar/workspace/distributedsystems/kj-02-web/README.md)
- [`kj-03-multicons-base/README.md`](/home/atul-kumar/workspace/distributedsystems/kj-03-multicons-base/README.md)
- [`kj-04-multicons-adv/README.md`](/home/atul-kumar/workspace/distributedsystems/kj-04-multicons-adv/README.md)
