# KJ-04 Multi Consumer Advanced — Implemented Specs

## Overview

`kj-04-multicons-adv` is the advanced multi-consumer telemetry module in this repo.
It extends the earlier web-based Kafka setup into a fuller operational learning stack with:

- producer UI
- consumer UI
- storage consumer UI
- alert consumer UI
- dedicated DLQ viewer UI
- retry-before-DLQ behavior
- lag simulation
- always-on fleet simulator

## Implemented services

### Kafka and platform services

- Kafka broker on host port `9092`
- Kafka UI on host port `8080`

### Application services

- Producer UI on `9501`
- Consumer UI on `9502`
- Alert Consumer UI on `9503`
- Storage Consumer UI on `9504`
- DLQ Viewer UI on `9505`

## Kafka topics

- `vehicle-telemetry`
- `vehicle-telemetry-dlq`

## Consumer groups

- `vehicle-telemetry-web-group`
- `telemetry-storage-group`
- `telemetry-alert-group`
- `telemetry-dlq-viewer-group`

## Failure handling

The storage consumer now uses a split failure strategy:

- malformed JSON: direct DLQ
- invalid payload after deserialization: direct DLQ
- transient processing failure: retry with backoff, then DLQ if retries are exhausted

### Retry behavior

- `MAX_RETRIES = 3`
- retry backoff is incremental from `RETRY_BACKOFF_MS = 250`
- simulated processing failure is triggered when `speed > 120`

This means the storage consumer no longer sends every processing failure straight to DLQ on the first attempt.

## DLQ Viewer

A dedicated `dlq-viewer` service is implemented.

It:

- consumes `vehicle-telemetry-dlq`
- runs on `9505`
- shows:
  - original event
  - error reason
  - timestamp
  - partition
  - offset

This makes failed-event inspection possible without relying only on Kafka UI.

## Storage consumer behavior

The storage consumer:

- reads from `vehicle-telemetry`
- uses manual offset commits
- stores latest event per vehicle in memory
- pushes `STORED` and `DLQ` updates over SSE
- exposes metrics
- supports `SLOW_MODE=true` for lag simulation

## Alert consumer behavior

The alert consumer:

- reads from `vehicle-telemetry` in an independent consumer group
- classifies events into alert states
- exposes a live dashboard
- demonstrates Kafka fan-out with the storage consumer

## Producer behavior

The producer app supports:

- manual event composition
- random event generation
- burst sends
- always-on vehicle simulator

The always-on simulator targets a desired active count rather than adding blindly on each activation.

## Project structure

```text
kj-04-multicons-adv/
├── pom.xml
├── run.sh
├── docker-compose.yml
├── README.md
├── specs.md
├── model/
├── util/
├── config/
├── dlq-producer/
├── dlq-viewer/
├── storage-consumer/
├── alert-consumer/
├── producer-app/
└── consumer-app/
```

## Run modes

Primary startup flow:

```bash
./run.sh --start
```

Supported variants:

```bash
./run.sh --start --detach
./run.sh --start --slow
./run.sh --start --slow --detach
./run.sh --stop
./run.sh --stop-blockers
```

## Learning goals covered

This module now demonstrates:

- multiple consumer groups on the same topic
- manual commit vs auto commit
- retry-before-DLQ
- direct DLQ for invalid payloads
- DLQ inspection through a dedicated UI
- consumer lag observation
- partition affinity by key
- operational dashboards for both success and failure paths

## Notes

- This file reflects the current implementation, not just the original requested design.
- For end-user walkthroughs and URLs, see [`README.md`](/home/atul-kumar/workspace/distributedsystems/kj-04-multicons-adv/README.md).
