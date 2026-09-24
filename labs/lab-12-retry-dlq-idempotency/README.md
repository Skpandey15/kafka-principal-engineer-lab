# Lab 12 — Retry, DLQ & Idempotency

## Quick Summary

- **Why this lab:** To take WP-06's introductory idempotent-consumer check (a file-backed set) to full production depth: a real `processed_events` table with a unique constraint doing atomic duplicate detection, bounded retry with backoff, and a real dead-letter queue with Spring-Kafka-shaped headers.
- **How to run:** Reuse `platform/kafka-cluster/` plus, for Postgres only, `platform/kafka-connect/`, then `produceOrderEvent -PeventId=...` (and `-Ppoison=true` for a malformed record) alongside `runRetryDlqConsumer`; `./gradlew test` for the 7-test suite.
- **Expected input:** Docker, JDK 21+; a Postgres `processed_events` claim table (`group_id`, `event_id`, `status`, `claimed_at`).
- **Expected output:** Bounded retries with measurable backoff before success; a poison record landing on `retry-dlq-orders.DLQ` with real `kafka_dlt-*` headers while consumption continues past it; a duplicate redelivery of the same `eventId` resulting in `DUPLICATE_SKIPPED`, not reprocessing.
- **What we learned:** A database unique constraint is what makes concurrent duplicate claims safely serialize — not application-level locking. `RetryingRecordProcessor` is a plain class exercised identically by both the runnable app and the test suite, so there's no test-only reimplementation that could silently drift from what actually runs in production. An orphaned in-progress claim (from a mid-processing crash) can be reclaimed once it's older than a lease window — a real, deliberate scope boundary against building a full lease-reaper or DLQ-replay tool in this lab.

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

### Kubernetes (k3d) alternative

```bash
platform-k8s/bootstrap-cluster.sh   # once
platform-k8s/kafka-cluster/setup.sh
platform-k8s/kafka-connect/setup.sh   # for its Postgres only -- no Connect worker needed
```

Same host ports as Docker Compose (`localhost:9093-9095`, `localhost:5432`).
The `processed_events` table is already part of the k8s Postgres init
ConfigMap. See [`platform-k8s/README.md`](../../platform-k8s/README.md)
for the internal-listener and path-mangling gotchas. Cleanup (in reverse
order): `platform-k8s/kafka-connect/cleanup.sh` then
`platform-k8s/kafka-cluster/cleanup.sh [--wipe]`.

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
