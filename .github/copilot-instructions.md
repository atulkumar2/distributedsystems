# Copilot Instructions — Vehicle Telemetry Kafka Project

## Project overview

This is a plain Java 17 Maven project for learning Apache Kafka integration.
Domain: IoT vehicle telematics. No Spring Boot — pure Java main classes.

## Package structure

```
com.example.telematics
  ├── model      → TelemetryEvent (POJO, no annotations)
  ├── util       → JsonUtil (Jackson ObjectMapper, singleton)
  ├── producer   → KafkaProducerExample
  └── consumer   → KafkaConsumerExample, KafkaManualCommitConsumerExample
```

## Conventions

- Kafka broker: `localhost:9092`
- Topic: `vehicle-telemetry` (3 partitions)
- Message key: always `vehicleId` (String) — preserves per-vehicle ordering
- Message value: JSON string (no schema registry, plain String serializer)
- Consumer group (auto commit): `vehicle-telemetry-group`
- Consumer group (manual commit): `vehicle-telemetry-group-manual-commit`
- New consumers should use `auto.offset.reset=earliest` unless stated otherwise

## Kafka configuration rules

- Always set `acks=all` on producers for durability
- Always set `retries=3` on producers
- Never use `enable.auto.commit=true` for new manual-commit consumers
- Call `commitSync()` only after business logic has completed successfully
- Use `StringSerializer` / `StringDeserializer` — no Avro or Protobuf in this project

## JSON serialization

- Use `JsonUtil.toJson(TelemetryEvent)` for serialization — do not create new `ObjectMapper` instances
- The `ObjectMapper` in `JsonUtil` is a static singleton; reuse it

## TelemetryEvent fields

`vehicleId`, `timestamp` (ISO-8601 String), `latitude`, `longitude`, `speed`, `fuelLevel`, `engineStatus`

## Code style

- Keep each class runnable as a standalone Java `main` method
- Add comments explaining *why* a Kafka setting matters, not just what it does
- Avoid Spring, Lombok, or external frameworks — keep dependencies minimal
- No test classes needed unless explicitly requested

## Build and run

```bash
mvn clean compile
mvn exec:java -Dexec.mainClass=com.example.telematics.producer.KafkaProducerExample
mvn exec:java -Dexec.mainClass=com.example.telematics.consumer.KafkaConsumerExample
mvn exec:java -Dexec.mainClass=com.example.telematics.consumer.KafkaManualCommitConsumerExample
```

## When suggesting new code

- New producer variants: extend or copy `KafkaProducerExample`, place in `producer` package
- New consumer variants: extend or copy `KafkaConsumerExample`, place in `consumer` package
- New topics: use snake-case names, document in README
- New model fields: add them to `TelemetryEvent` with getter/setter only
