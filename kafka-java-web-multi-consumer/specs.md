# Kafka Java Web Multi Consumer Specs

## Table of Contents

- [Kafka Java Web Multi Consumer Specs](#kafka-java-web-multi-consumer-specs)
  - [Table of Contents](#table-of-contents)
  - [Current System (DO NOT MODIFY)](#current-system-do-not-modify)
  - [Goal](#goal)
  - [Improvements in This New Module](#improvements-in-this-new-module)
    - [1. New Kafka Consumers (Backend Services)](#1-new-kafka-consumers-backend-services)
      - [A. Storage Consumer](#a-storage-consumer)
      - [B. Alert Consumer](#b-alert-consumer)
    - [2. Dead Letter Queue (DLQ)](#2-dead-letter-queue-dlq)
    - [3. Failure Simulation](#3-failure-simulation)
    - [4. Partition Awareness](#4-partition-awareness)
    - [5. Lag Simulation Mode](#5-lag-simulation-mode)
    - [6. Project Structure](#6-project-structure)
    - [7. Docker Support](#7-docker-support)
    - [8. Logging \& Traceability](#8-logging--traceability)
    - [9. README.md (VERY IMPORTANT)](#9-readmemd-very-important)
      - [How to Run](#how-to-run)
      - [What to Observe](#what-to-observe)
      - [Architecture Explanation](#architecture-explanation)
  - [Constraints](#constraints)
  - [Bonus (if possible)](#bonus-if-possible)
  - [Output Requirement](#output-requirement)

---

Enhance my existing **Kafka-based telemetry system** by adding a new module in a **separate subfolder**, without modifying the current producer and consumer UI services. Copy everything from web-apps and create new set of services.

## Current System (DO NOT MODIFY)

I already have:

- A **Telemetry Producer UI (port 8081)** that sends events to Kafka topic `vehicle-telemetry`
- A **Telemetry Consumer UI (port 8082)** that reads and displays live events
- Kafka running via Docker
- Kafka UI available
- Events are JSON-based and include:

  - vehicleId
  - speed
  - fuel
  - engine status
  - latitude / longitude

👉 Treat these as **existing production services**. Do not change their code.

---

## Goal

Work in the current subfolder by copying existing code from web-apps:

```text
/kafka-java-web-multi-consumer/
```

This will simulate a **real backend processing layer** like in IoT / telematics platforms.

---

## Improvements in This New Module

### 1. New Kafka Consumers (Backend Services)

#### A. Storage Consumer

- Group ID: `telemetry-storage-group`
- Reads from topic: `vehicle-telemetry`
- Runs independently from existing UI consumer

Responsibilities:

- Parse JSON events

- Simulate storing data (in-memory map or simple file)

- Add logs:

  - eventId (generate if missing)
  - vehicleId
  - partition
  - offset

- Disable auto-commit

- Commit offset ONLY after successful processing

---

#### B. Alert Consumer

- Group ID: `telemetry-alert-group`
- Reads from same topic (`vehicle-telemetry`)

Responsibilities:

- Detect conditions:

  - speed > 100 → log alert
  - fuel < 20 → log warning

- Print clean logs:

  - "ALERT: Overspeed vehicle VH-1004 speed=120"

---

### 2. Dead Letter Queue (DLQ)

Create a new topic:

```text
vehicle-telemetry-dlq
```

Enhance Storage Consumer:

- If event is invalid OR processing fails:

  - send event to DLQ
  - include error reason

Example:

```json
{
  "originalEvent": {...},
  "error": "Invalid speed value",
  "timestamp": "..."
}
```

---

### 3. Failure Simulation

Inside Storage Consumer:

- If speed > 120 → simulate failure
- Do NOT commit offset
- Send to DLQ instead

---

### 4. Partition Awareness

Add logs in both consumers:

- partition
- offset

Explain via comments:

- how Kafka partitions work
- why same vehicleId should go to same partition

---

### 5. Lag Simulation Mode

Add config flag:

```text
SLOW_MODE=true
```

If enabled:

- consumer sleeps 200–500 ms per message

README should explain:

- how to observe lag in Kafka UI
- what lag means

---

### 6. Project Structure

Create:

```text
kafka-java-web-multi-consumer/
  ├── storage-consumer/
  ├── alert-consumer/
  ├── dlq-producer/
  ├── model/
  ├── util/
  ├── config/
  ├── pom.xml
  └── README.md
```

---

### 7. Docker Support

Add Dockerfiles for:

- storage consumer
- alert consumer

Optional:

- docker-compose snippet to run all services

---

### 8. Logging & Traceability

Add:

- eventId (generate UUID if not present)
- consistent logging format:

```text
[eventId=123][vehicleId=VH-1004][partition=1][offset=45]
```

---

### 9. README.md (VERY IMPORTANT)

Include:

#### How to Run

1. start Kafka
2. start existing UI producer/consumer
3. start new storage consumer
4. start alert consumer
5. send events via UI

#### What to Observe

- both consumer groups receive same message
- DLQ gets failed events
- lag increases when slow mode enabled
- same vehicleId → same partition

#### Architecture Explanation

Explain:

- producer → Kafka → multiple consumers
- fan-out pattern
- DLQ pattern
- offset management
- real-world telematics mapping

---

## Constraints

- Do NOT modify existing UI services
- Keep new module independent
- Use plain Java (no heavy frameworks)
- Keep code readable and well-commented
- Generate FULL working code

---

## Bonus (if possible)

- Retry logic before sending to DLQ
- Basic metrics logging (processed count, failures)

---

## Output Requirement

Generate:

- full folder structure
- all Java files
- pom.xml
- Dockerfiles
- README

Do not describe — generate complete working code.
