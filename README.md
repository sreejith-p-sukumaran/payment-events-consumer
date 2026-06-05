# payment-events-consumer

An **idempotent, fault-tolerant** Kafka consumer for the `payment-events` topic. It
is the downstream half of a Transactional Outbox setup: the producer delivers
events **at-least-once**, so this service processes each event **exactly once**,
**retries transient failures without blocking the partition**, and **dead-letters**
anything it can't handle — with a path to **inspect and replay** those messages
after a fix.

> Sibling service to
> [`outbox-payments-service`](https://github.com/sreejith-p-sukumaran/outbox-payments-service).
> The two share no code — they meet only at the `payment-events` topic.

---

## The problem

Consuming payment events reliably means surviving three very different kinds of trouble:

1. **Duplicates.** At-least-once delivery means the same event can arrive more than
   once. Applying it twice would double the effect (e.g. a duplicated audit/charge).
2. **Transient failures.** A momentary blip — DB connection drop, downstream timeout —
   shouldn't drop the message. But retrying *in place* blocks the partition: every
   later event sits behind the one that's failing (head-of-line blocking).
3. **Poison messages.** A malformed payload or a broken business rule will **never**
   succeed. Retrying it wastes attempts and, if retried forever, jams the pipeline.

A robust consumer has to tell these apart and respond to each correctly — without
ever losing a message or silently double-applying one.

---

## The solution

Four cooperating mechanisms:

| Concern | Mechanism |
|---|---|
| Duplicates | **Idempotent processing** — dedupe on event id (`processed_event` primary key), effect in the same transaction. |
| Transient failures | **Non-blocking retry** via Spring Kafka `@RetryableTopic` — failed records move to retry topics with backoff; the main partition keeps flowing. |
| Poison messages | **Failure classification** — a `NonRetryableException` skips retries and goes straight to the dead-letter topic. |
| Recovery | **Dead-letter topic + inspect/replay** — failed records land on `payment-events-dlt` with full failure context; after a fix they can be replayed to the main topic. |

---

## Workflow

```
                                  ┌──────────────────────────────────────────────┐
   Kafka                          │  PaymentEventListener  (@RetryableTopic)       │
   payment-events ───────────────▶│  1. parse payload                              │
        ▲                         │  2. IdempotentPaymentProcessor.process()       │
        │                         └──────────────────────────────────────────────┘
        │ replay                          │              │                  │
        │ (after fix)            success  │   transient  │   non-retryable  │
        │                          ▼      │   failure    │   failure        │
        │                      ┌───────┐  │      ▼       │   (NonRetryable  ▼
        │                      │ DONE  │  │  ┌────────────────────┐ Exception / bad payload)
        │                      └───────┘  │  │ payment-events-     │        │
        │                                 │  │   retry-0  (wait 2s)│        │
        │                                 │  │   retry-1  (wait 2s)│        │
        │                                 │  └────────────────────┘        │
        │                                 │      │ still failing           │
        │                                 │      │ after 3 attempts        │
        │                                 ▼      ▼                         ▼
        │                          ┌─────────────────────────────────────────────┐
        └──────────────────────────│  payment-events-dlt  (dead-letter topic)     │
              DlqReplayer          │  value = original payload                     │
                                   │  headers = exception, attempts, failed-at, …  │
                                   └─────────────────────────────────────────────┘
                                                      ▲
                                            DlqInspector reads it back
```

### 1. Idempotent processing (exactly-once effect)

```
@KafkaListener ──▶ IdempotentPaymentProcessor (one @Transactional)
                     ├─ validate(event)                       → invalid? throw NonRetryableException
                     ├─ event id already in processed_event?  → SKIP (duplicate)
                     └─ otherwise: INSERT processed_event (PK = eventId)
                                   INSERT payment_audit        ← the effect
```

- `processed_event` — one row per handled event id; the **primary key is the dedupe
  guarantee**. A duplicate delivery can't be processed twice.
- `payment_audit` — the observable side effect. It would double if dedupe were broken.
- A duplicate that races past the existence check is rejected by the `processed_event`
  primary key, and the whole transaction (audit row included) rolls back — so the
  effect is applied **at most once**. This also makes **retries and replays safe**:
  re-delivering an event that already succeeded is a no-op.

### 2. Failure classification

| Failure | Example | Treatment |
|---|---|---|
| **Non-retryable** (`NonRetryableException`) | malformed JSON, blank id, non-positive amount, bad currency | **skip retries → DLT immediately** |
| **Transient** (anything else) | DB/broker hiccup, downstream timeout | **retry with backoff** |

Classification happens at two points: the listener wraps unparseable payloads in
`NonRetryableException`; the processor validates business rules before any side effect.

### 3. Non-blocking retry

`@RetryableTopic(attempts = 3, backoff = fixed 2s, exclude = NonRetryableException)`.
A transient failure forwards the record to a retry topic stamped with a due-time and
returns immediately — **the consumer thread never sleeps and the main partition keeps
flowing**. A dedicated retry-topic consumer waits out the backoff and re-delivers.
After 3 total attempts (1 original + 2 retries) the record is routed to the DLT.

### 4. Dead-letter routing & context

Both **exhausted** and **non-retryable** records land on `payment-events-dlt`. The
original payload is the record **value**; failure context is added as headers
(in addition to Spring's standard `kafka_dlt-*` headers):

| Header | Meaning |
|---|---|
| `x-dlt-exception-message` | why it failed |
| `x-dlt-attempts` | how many delivery attempts were made |
| `x-dlt-failed-at` | when it was dead-lettered (ISO-8601) |
| `x-dlt-original-topic` | the main topic to replay back to |

### 5. Inspect & replay

- **`DlqInspector`** spins up a throwaway consumer, seeks to the beginning of the DLT,
  and returns a snapshot of what's there — **without committing offsets**, so
  inspecting is side-effect-free and repeatable.
- **`DlqReplayer`** republishes a dead-lettered record to its original topic,
  preserving the key. Because the consumer is idempotent, replay can't double-apply an
  effect that already succeeded; replay is meant for after the root cause is fixed.

---

## Trade-offs worth knowing

- **Relaxed per-key ordering.** Moving a failed record to a retry topic means later
  records for the *same key* on the main partition may be processed before the failed
  one is retried. We trade strict per-key ordering for head-of-line-blocking-free
  throughput; idempotency makes out-of-order re-application safe.
- **DLT is append-only.** Replay doesn't delete from the DLT (Kafka topics are
  append-only). Re-inspecting after a fix shows what remains to handle.

---

## Stack

Kotlin · Spring Boot 3.5 · Spring for Apache Kafka (`@RetryableTopic`) · Spring Data
JPA · Flyway · MySQL 8 · Java 21 · JUnit 5 · MockK · Testcontainers (MySQL + Kafka).

## Configuration

```yaml
consumer:
  topic: payment-events
  dlt-topic: payment-events-dlt
  retry:
    attempts: 3       # 1 original + 2 retries, then the DLT
    backoff-ms: 2000  # fixed 2s between attempts
```

## Run locally

Requires Docker and Java 21. Kafka is owned by the producer repo, so start that first.

```bash
# 1. In outbox-payments-service: docker compose up -d   (brings up Kafka on :9092)

# 2. Here: start this service's own MySQL (on 3307, to avoid the producer's 3306)
docker compose up -d

# 3. Run the consumer
./gradlew bootRun
```

- Connects to Kafka at `localhost:9092`; consumer group `payment-events-consumer` (`earliest`).
- MySQL: `localhost:3307`, database `payments_consumer` (user `consumer` / `consumer`).
- Retry/DLT topics (`payment-events-retry-*`, `payment-events-dlt`) are created automatically.
- There is no HTTP endpoint — this service is a Kafka listener; the DLQ inspector/replay
  are programmatic components (no web layer).

## Test

```bash
./gradlew test
```

Docker must be running. Tests use Testcontainers (real MySQL + real Kafka) and
EmbeddedKafka. Coverage includes: exactly-once over a real broker; failure
classification; transient-retry-then-succeed; non-retryable → DLT; retry exhaustion →
DLT; and DLQ inspect + replay.

## Project structure

```
src/main/kotlin/com/sreejith/consumer/
  domain/      ProcessedEvent (dedupe ledger), PaymentAudit (effect)
  event/       PaymentCompletedEvent (consumer-side copy of the contract)
  error/       NonRetryableException (the retry/DLT classification signal)
  repository/  ProcessedEventRepository, PaymentAuditRepository
  service/     IdempotentPaymentProcessor (once-only logic + validation)
  listener/    PaymentEventListener (@KafkaListener + @RetryableTopic)
  dlq/         DltHeaders, DeadLetterRecord, DlqInspector, DlqReplayer
  config/      ConsumerProperties, KafkaRetryConfig (retry/DLT infra), Jackson, clock
src/main/resources/db/migration/   Flyway migrations
DESIGN.md                          design rationale
```

See [`DESIGN.md`](./DESIGN.md) for the rationale behind each decision.
