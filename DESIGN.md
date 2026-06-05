# Dead-Letter Queue & Retry Handling — Design

<!-- Skeleton only. Headings are in place; fill in the rationale yourself. -->

## 1. Context & Goals

<!-- TODO -->

## 2. Topology

<!-- TODO: main topic, retry topics, dead-letter topic -->

## 3. Failure Classification

### 3.1 Retryable (transient) failures

<!-- TODO -->

### 3.2 Non-retryable failures (NonRetryableException)

<!-- TODO -->

## 4. Non-Blocking Retry

### 4.1 Why @RetryableTopic over blocking retry

<!-- TODO -->

### 4.2 Attempts & backoff

<!-- TODO -->

### 4.3 The retry scheduler

<!-- TODO -->

## 5. Dead-Letter Routing

### 5.1 When records are dead-lettered

<!-- TODO -->

### 5.2 Failure-context headers

<!-- TODO -->

## 6. DLQ Inspection & Replay

### 6.1 Inspector

<!-- TODO -->

### 6.2 Replay

<!-- TODO -->

## 7. Idempotency & Exactly-Once Effects

<!-- TODO -->

## 8. Ordering Trade-offs

<!-- TODO -->

## 9. Configuration

<!-- TODO -->

## 10. Testing Strategy

<!-- TODO -->

## 11. Operations & Future Work

<!-- TODO -->
