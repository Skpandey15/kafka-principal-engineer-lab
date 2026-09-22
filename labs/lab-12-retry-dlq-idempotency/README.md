# Lab 12 — Retry, DLQ & Idempotency

## Objective

Take WP-06's introductory idempotent-consumer check (a file-backed
`Set<String>`) to full production depth: a real `processed_events` table
with a unique constraint doing atomic duplicate detection, bounded retry
with backoff, poison-message handling, and a dead-letter queue with
real, Spring-Kafka-shaped headers.

See [`docs/retry-dlq/RETRY_DLQ_AND_IDEMPOTENCY.md`](../../docs/retry-dlq/RETRY_DLQ_AND_IDEMPOTENCY.md)
for the full conceptual depth, the claim lifecycle, real captured
evidence, and 7 Principal Engineer questions. This README covers setup,
commands, and a condensed experiment walkthrough.

## Prerequisites

- Docker (this lab reuses the WP-07 3-broker cluster and, purely for its
  Postgres instance, WP-11's `platform/kafka-connect/` -- no Kafka
  Connect worker or Debezium involvement at all)
- JDK 21+
- Completion of WP-06 (`lab-05-offset-management-delivery-semantics`) is
  useful context -- this lab's `IdempotencyStore` is the direct,
  production-depth successor to that lab's `ProcessedEventStore`.

### Why a separate project

Same one-project-per-lab convention every prior lab uses -- see lab-03's
README, "Why a separate project," for the full rationale.

## Environment

Reuses **`platform/kafka-cluster/`** (WP-07) and, for ONLY its Postgres
container, **`platform/kafka-connect/`** (WP-11) -- one new table added
via `platform/kafka-connect/init-postgres-idempotency.sql`. If that
Postgres volume was already initialized before this WP existed, run
`docker compose down -v` first so the new init script runs.

This lab's own automated tests run against a dedicated, single-node
Testcontainers cluster (`RetryDlqTestCluster`) -- Kafka + Postgres only,
no Connect worker.

## Architecture

```text
platform/kafka-cluster/          (reused, unmodified)
platform/kafka-connect/          (reused for Postgres ONLY; +init-postgres-idempotency.sql)
  processed_events  -- the claim table: group_id, event_id, status, claimed_at

labs/lab-12-retry-dlq-idempotency/
  support/   OrderEvent, IdempotencyStore, BusinessLogicSimulator
  consumer/  RetryingRecordProcessor (the core logic), RetryDlqConsumerApp
  producer/  OrderEventProducerApp
```

## Setup

```bash
cd platform/kafka-cluster && docker compose up -d
cd ../kafka-connect && docker compose up -d postgres
```

(No `fetch-plugins.sh` needed -- this lab never touches Kafka Connect.)

## Commands

| Task | What it runs |
|---|---|
| `./gradlew produceOrderEvent -PeventId=ORDER-1` | Produces a valid record |
| `./gradlew produceOrderEvent -Ppoison=true` | Produces a malformed, unparsable record |
| `./gradlew runRetryDlqConsumer` | Real consumer: claim/retry/complete-or-DLQ for every record |

## Implementation

See the conceptual doc for full source-level discussion. One design note
worth calling out: `RetryingRecordProcessor` is a plain class, not
static `main`-method logic -- both `RetryDlqConsumerApp` and this lab's
own test suite exercise the EXACT same code path, so there's no
test-only reimplementation that could silently drift from what actually
runs.

## Verification

```bash
./gradlew test
```

7 automated integration tests, against a real Kafka + PostgreSQL cluster
(Testcontainers) -- see "Automated tests" below.

## Experiment

### Experiment 1 — retry with backoff, then success

```bash
./gradlew runRetryDlqConsumer &
./gradlew produceOrderEvent -PeventId=ORDER-1
```

Real captured evidence: bounded retries with measurable backoff delay.
Conceptual doc, Section 3.

### Experiment 2 — poison message → DLQ, consumption continues

```bash
./gradlew produceOrderEvent -Ppoison=true -PeventId=poison-key
./gradlew produceOrderEvent -PeventId=ORDER-2
```

Real captured evidence: the poison record lands on
`retry-dlq-orders.DLQ` with `kafka_dlt-*` headers; `ORDER-2` is still
processed. Conceptual doc, Sections 4-5.

### Experiment 3 — duplicate redelivery is a no-op

```bash
./gradlew produceOrderEvent -PeventId=ORDER-3
./gradlew runRetryDlqConsumer -PgroupId=retry-dlq-consumer  # process it once, let it exit
./gradlew produceOrderEvent -PeventId=ORDER-3  # same eventId again
./gradlew runRetryDlqConsumer -PgroupId=retry-dlq-consumer  # second run: DUPLICATE_SKIPPED
```

Conceptual doc, Sections 1 and 7.

## Failure injection

No dedicated failure-matrix row names this WP -- this WP's own crash
scenarios (redelivery after a crash before offset commit, an orphaned
in-progress claim after a mid-processing crash) are modeled directly in
the automated suite via fresh DB connections and real elapsed time, not
via container-level failure injection.

## Troubleshooting

### `FATAL: invalid value for parameter "TimeZone"`

Same real, environment-specific finding as WP-11 and WP-12 -- see the
conceptual doc. Already fixed in `RetryDlqConsumerApp`
(`TimeZone.setDefault(UTC)` before connecting).

## Cleanup

```bash
cd platform/kafka-connect && docker compose down
cd ../kafka-cluster && docker compose down
```

## Automated tests

`./gradlew test` -- 7 tests, against a real Kafka + PostgreSQL cluster
(`RetryDlqTestCluster`, Testcontainers):

1. `duplicateEventIsProcessedOnlyOnceEvenAfterRedeliveryAfterACrashBeforeOffsetCommit`
2. `concurrentDuplicateClaimsAreSafelySerializedByTheUniqueConstraint`
3. `aTransientlyFailingMessageIsRetriedWithBackoffThenSucceeds`
4. `aPoisonMessageIsRoutedToTheDlqImmediatelyWithOriginalHeadersAndPayload`
5. `anAlwaysFailingMessageExhaustsRetriesIsRoutedToTheDlqAndItsClaimIsReleased`
6. `anOrphanedInProgressClaimOlderThanTheLeaseWindowCanBeReclaimed`
7. `consumptionContinuesPastAPoisonMessageToTheNextRecordOnTheSamePartition`

Every timing-sensitive assertion measures real elapsed time or real
condition-polling, per this repository's established test convention --
no mocked clock, no fixed sleep standing in for a real wait.

## Production considerations

See the conceptual doc's Section 8 for what this lab deliberately does
NOT build (jittered exponential backoff, a lease reaper, DLQ replay
tooling, out-of-band retry topics) and why each is a reasonable scope
boundary for a lab rather than a hidden gap.

## Principal Engineer questions

See the conceptual doc's Section 9 for all 7 questions with detailed,
experimentally-grounded answers.
