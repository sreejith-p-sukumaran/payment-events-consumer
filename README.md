# payment-events-consumer

An **idempotent** Kafka consumer for the `payment-events` topic. It is the
downstream half of a Transactional Outbox setup: the producer delivers events
**at-least-once**, so this service is built to process each event **exactly
once** by deduplicating on the event id.

> Sibling service to
> [`outbox-payments-service`](https://github.com/sreejith-p-sukumaran/outbox-payments-service).
> The two share no code — they meet only at the `payment-events` topic.

## How it works

```
Kafka: payment-events ──▶ @KafkaListener ──▶ IdempotentPaymentProcessor (one @Transactional)
                                              │
                                              ├─ event id already in processed_event?  → SKIP
                                              └─ otherwise: INSERT processed_event (PK = eventId)
                                                            INSERT payment_audit   ← the effect
```

- `processed_event` — one row per handled event id; the **primary key is the
  dedupe guarantee**. A duplicate delivery can't be processed twice.
- `payment_audit` — the observable side effect, with its own surrogate key. It
  would double if dedupe were broken, which is exactly what the tests assert
  against.
- A duplicate that races past the existence check is rejected by the
  `processed_event` primary key, and the whole transaction (including the audit
  row) rolls back — so the effect is applied at most once.

## Stack

Kotlin · Spring Boot 3.5 · Spring Data JPA · Flyway · MySQL 8 ·
Spring for Apache Kafka · Java 21 · JUnit 5 · Testcontainers (MySQL + Kafka).

## Run locally

Requires Docker and Java 21. Kafka is owned by the producer repo, so start that
first.

```bash
# 1. In the outbox-payments-service repo: docker compose up -d   (brings up Kafka on :9092)

# 2. Here: start this service's own MySQL (on 3307, to avoid the producer's 3306)
docker compose up -d

# 3. Run the consumer (it subscribes to payment-events and processes events)
./gradlew bootRun
```

- Connects to Kafka at `localhost:9092` (the producer's broker)
- MySQL: `localhost:3307`, database `payments_consumer` (user `consumer` / `consumer`)
- Consumer group: `payment-events-consumer`, reading from `earliest`

There is no HTTP endpoint — this service is a Kafka listener.

## Test

```bash
./gradlew test
```

Tests use Testcontainers (**Docker must be running**): real MySQL for the
processor, and EmbeddedKafka / a real Kafka container for the listener and
end-to-end tests. The key test — `a duplicate event delivered twice is processed
once` — proves the idempotency guarantee over a real broker.

## Project structure

```
src/main/kotlin/com/sreejith/consumer/
  domain/      ProcessedEvent (dedupe ledger), PaymentAudit (effect)
  event/       PaymentCompletedEvent (consumer-side copy of the contract)
  repository/  ProcessedEventRepository, PaymentAuditRepository
  service/     IdempotentPaymentProcessor (the once-only logic)
  listener/    PaymentEventListener (@KafkaListener)
  config/      Jackson, clock
src/main/resources/db/migration/   Flyway migrations
```
