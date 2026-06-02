-- Dedupe ledger: one row per event id the consumer has handled. The primary
-- key is the event id, so a duplicate delivery can never be processed twice —
-- this is how at-least-once delivery is made safe.
CREATE TABLE processed_event (
    event_id     VARCHAR(36) NOT NULL,
    payment_id   VARCHAR(36) NOT NULL,
    processed_at DATETIME(6) NOT NULL,
    PRIMARY KEY (event_id)
);

-- The observable side effect of processing. It has its own surrogate key (not
-- the event id), so without dedupe a re-delivered event would insert a second
-- audit row. The idempotency test asserts exactly one row survives.
CREATE TABLE payment_audit (
    id          BIGINT         NOT NULL AUTO_INCREMENT,
    payment_id  VARCHAR(36)    NOT NULL,
    amount      DECIMAL(19, 4) NOT NULL,
    currency    VARCHAR(3)     NOT NULL,
    recorded_at DATETIME(6)    NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX idx_payment_audit_payment ON payment_audit (payment_id);
