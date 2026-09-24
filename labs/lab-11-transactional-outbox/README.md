# Lab 11 — Transactional Outbox

## Quick Summary

- **Why this lab:** To demonstrate the dual-write problem directly (a real business commit with zero corresponding Kafka events), then fix it with the transactional outbox pattern built on WP-11's own CDC pipeline and Debezium's real `EventRouter` transform — not a hand-rolled dual-write workaround.
- **How to run:** Reuse `platform/kafka-cluster/` and `platform/kafka-connect/` (adding one new SQL init script for the outbox tables — `docker compose down -v` first if the Postgres volume predates this WP), then `runNaiveDualWrite -PcrashAfterCommit=true` → `runOutboxWriter` → `registerOutboxConnector` → `runOutboxConsumer`; `./gradlew test` for the 6-test suite.
- **Expected input:** Docker, JDK 21+, WP-11 completed; a business table (`outbox_demo_orders`, never captured by CDC) and a dedicated outbox table (`outbox_event`, the only thing Debezium reads).
- **Expected output:** A naive dual-write leaving a committed business row with no matching Kafka event; the outbox pattern producing a business row and its outbox row atomically in one transaction, routed by Debezium to a topic named from `aggregatetype`.
- **What we learned:** `OutboxWriterApp` never imports `KafkaProducer` at all — publishing is entirely Debezium's responsibility, which is the actual point: the application only ever writes to Postgres, atomically, and CDC does the rest. A real finding from this lab's own test suite: PostgreSQL's `jsonb` column type re-serializes output with a space after `:`/`,`, breaking exact-substring assertions — parse as JSON and compare fields, never compare raw payload substrings.

## Objective

Demonstrate the dual-write problem directly, then fix it with the
transactional outbox pattern, built DIRECTLY on WP-11's CDC pipeline and
Debezium's own `io.debezium.transforms.outbox.EventRouter` transform.

See [`docs/outbox/TRANSACTIONAL_OUTBOX.md`](../../docs/outbox/TRANSACTIONAL_OUTBOX.md)
for the full conceptual depth, real captured evidence, a real finding
(PostgreSQL `jsonb`'s re-serialization behavior), and 7 Principal
Engineer questions. This README covers setup, commands, and a condensed
experiment walkthrough.

## Prerequisites

- Completion of WP-11 (`labs/lab-10-kafka-connect-cdc`) -- this lab
  reuses `platform/kafka-connect/` (Postgres + Kafka Connect worker)
  as-is, adding only two new tables via
  `platform/kafka-connect/init-postgres-outbox.sql`.
- Docker, JDK 21+

### Why a separate project

Same one-project-per-lab convention every prior lab uses -- see lab-03's
README, "Why a separate project," for the full rationale.

## Environment

Reuses **`platform/kafka-cluster/`** (WP-07) and **`platform/kafka-connect/`**
(WP-11) UNCHANGED, adding one new SQL init script,
`init-postgres-outbox.sql` (business table `outbox_demo_orders` + outbox
table `outbox_event` + a dedicated `dbz_outbox_publication` scoped to
ONLY `outbox_event`). If `platform/kafka-connect/`'s Postgres volume was
already initialized under WP-11 before this WP existed, run
`docker compose down -v` first so both init scripts run on the next
`docker compose up` (Postgres init scripts run once, against an empty
data directory).

This lab's own automated tests run against a dedicated, single-node
Testcontainers cluster (`OutboxTestCluster`), same shape as WP-11's own
`KafkaConnectCluster`.

## Architecture

```text
platform/kafka-cluster/          (reused, unmodified)
platform/kafka-connect/          (reused; +init-postgres-outbox.sql)
  outbox_demo_orders  -- business table, NEVER captured by CDC
  outbox_event        -- outbox table, the ONLY thing Debezium reads

labs/lab-11-transactional-outbox/
  support/  LabConfig, ConnectRestClient
  naive/    NaiveDualWriteApp              (the problem)
  outbox/   OutboxWriterApp,                (the fix)
            OutboxConnectorRegistrationApp,
            OutboxEventConsumerApp
```

## Setup

```bash
cd platform/kafka-cluster && docker compose up -d
cd ../kafka-connect && bash fetch-plugins.sh && docker compose up -d
```

(If reusing an already-initialized WP-11 volume, run
`docker compose down -v` first -- see "Environment" above.)

### Kubernetes (k3d) alternative

```bash
platform-k8s/bootstrap-cluster.sh   # once
platform-k8s/kafka-cluster/setup.sh
platform-k8s/kafka-connect/setup.sh
```

Same host ports as Docker Compose. The outbox tables
(`outbox_demo_orders`, `outbox_event`) are already part of the k8s
Postgres init ConfigMap, no separate re-init step needed. See
[`platform-k8s/README.md`](../../platform-k8s/README.md) for the
internal-listener and path-mangling gotchas. Cleanup (in reverse order):
`platform-k8s/kafka-connect/cleanup.sh` then
`platform-k8s/kafka-cluster/cleanup.sh [--wipe]`.

## Commands

| Task | What it runs |
|---|---|
| `./gradlew runNaiveDualWrite -PcrashAfterCommit=true` | The dual-write problem (Section 1) |
| `./gradlew runOutboxWriter -PorderId=O-1 -Pamount=10.00` | Business row + outbox row, one transaction (Section 2) |
| `./gradlew registerOutboxConnector` | Registers the real Debezium connector with the Event Router SMT (Sections 4-5) |
| `./gradlew runOutboxConsumer -Ptopic=outbox.event.Order` | Decodes a routed outbox topic (Section 5) |

## Implementation

See the conceptual doc for full source-level discussion. One design note
worth calling out: `OutboxWriterApp` never imports
`org.apache.kafka.clients.producer.KafkaProducer` at all -- publishing is
entirely Debezium's responsibility, which is the whole point (Section 3).

## Verification

```bash
./gradlew test
```

6 automated integration tests, against a real Kafka + PostgreSQL + Kafka
Connect cluster (Testcontainers) -- see "Automated tests" below.

## Experiment

### Experiment 1 — the dual-write problem

```bash
./gradlew runNaiveDualWrite -PcrashAfterCommit=true -PorderId=O-CRASH-1
```

Real captured evidence: a business row committed with zero corresponding
Kafka events. Conceptual doc, Section 1.

### Experiment 2 — the outbox pattern, atomically

```bash
./gradlew runOutboxWriter -PorderId=O-1 -Pamount=10.00
./gradlew registerOutboxConnector
./gradlew runOutboxConsumer -Ptopic=outbox.event.Order
```

Real captured routing evidence (`aggregatetype` → topic), and the
`jsonb` re-serialization finding: conceptual doc, Sections 4-6.

## Failure injection

No dedicated failure-matrix row names this WP -- it reuses WP-11's CDC
transport unchanged, and WP-11's own two failure-matrix experiments
(Connect task failure, source DB unavailability) already cover it. See
the conceptual doc, Section 10.

## Troubleshooting

### Connector registration succeeds but no init script ran

Postgres init scripts run once, against an empty data volume. Run
`docker compose down -v` under `platform/kafka-connect/`, then
`docker compose up -d` again.

### A `jsonb` payload assertion fails on an exact substring match

A real finding this lab's own test suite hit -- see the conceptual doc,
Section 6: PostgreSQL's `jsonb` output re-serializes with a space after
`:`/`,`. Parse the payload as JSON and compare fields, not raw
substrings.

## Cleanup

```bash
cd platform/kafka-connect && docker compose down
cd ../kafka-cluster && docker compose down
```

Neither this lab nor `platform/kafka-connect/`'s outbox tables modify
WP-11's own `orders` table or `dbz_publication`.

## Automated tests

`./gradlew test` -- 6 tests, against a real Kafka + PostgreSQL + Kafka
Connect cluster (`OutboxTestCluster`, Testcontainers):

1. `naiveDualWriteLosesTheKafkaEventWhenTheProcessCrashesAfterTheDbCommit`
2. `outboxRowAndBusinessRowCommitOrRollbackTogetherInOneTransaction`
3. `debeziumRoutesOutboxEventsToATopicNamedByAggregateTypeWithTheOriginalPayload`
4. `outboxEventIsCapturedEvenWhenTheWritingProcessNeverTalksToKafka`
5. `deletingAnAlreadyCapturedOutboxRowProducesNoFurtherEventOnTheRoutedTopic`
6. `outboxDemoOrdersTableChangesAreNeverCapturedOnlyOutboxEventIs`

Every assertion uses a bounded condition-polling loop rather than a
fixed sleep as the synchronization mechanism, per this repository's
established test convention.

## Production considerations

See the conceptual doc's Sections 7-9 for outbox table cleanup safety,
the reliability guarantees this pattern actually delivers (and does
NOT), and its operational cost.

## Principal Engineer questions

See the conceptual doc's Section 11 for all 7 questions with detailed,
experimentally-grounded answers.
