-- WP-13 idempotent-consumer table, added to the SAME `inventory`
-- database WP-11/WP-12 already use. Reuses this container purely for
-- its Postgres instance -- this WP needs no Kafka Connect worker, no
-- Debezium, and this table is never part of any publication.
--
-- The full production upgrade over WP-06's file-backed
-- `ProcessedEventStore` (a plain Set<String>, one process, no
-- concurrency story): a real table with a UNIQUE constraint doing the
-- duplicate-detection work atomically, so two concurrent attempts
-- claiming the SAME event_id can never both believe they own it --
-- exactly the race a check-then-insert Set can't prevent.
--
-- `status` models a claim lifecycle, not just a boolean "seen it":
--   IN_PROGRESS -- claimed, business logic not yet confirmed successful
--   DONE        -- business logic succeeded; a future redelivery of the
--                  same event_id is a true no-op duplicate
-- A row that never reaches DONE (the owning process crashed, or
-- processing exhausted its retries and went to the DLQ) is either
-- explicitly released (DELETEd, see IdempotencyStore.release) or left
-- as a stale IN_PROGRESS claim that `claimed_at` lets a later attempt
-- reclaim after a lease window -- see docs/retry-dlq/RETRY_DLQ_AND_IDEMPOTENCY.md,
-- the crash-scenario section, for why this matters.
CREATE TABLE IF NOT EXISTS processed_events (
    group_id     VARCHAR(128) NOT NULL,
    event_id     VARCHAR(128) NOT NULL,
    status       VARCHAR(16)  NOT NULL,
    claimed_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (group_id, event_id)
);
