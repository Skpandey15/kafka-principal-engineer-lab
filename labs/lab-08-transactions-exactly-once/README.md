# Lab 08 — Transactions & Exactly-Once Semantics

## Quick Summary

- **Why this lab:** To precisely answer what Kafka's exactly-once semantics actually guarantees, and why that does NOT automatically mean exactly-once business processing across Kafka plus a database or external API — via idempotent producers, real transactions, isolation levels, and an atomic consume-transform-produce pipeline.
- **How to run:** Reuse `platform/kafka-cluster/` (WP-07's 3-broker cluster), create the lab topics, then run the producer/consumer/pipeline apps (`runDuplicateRiskDemo`, `runIdempotentProducer`, `runTransactionalProducer -Paction=commit|abort`, `runIsolationConsumer`, `runMultiPartitionTransaction`, `runFencingDemo`, `runTransactionalPipeline -PfailurePoint=...`); `./gradlew test` for the 10-test Testcontainers suite (the authoritative evidence source).
- **Expected input:** Docker, JDK 21+, WP-07 and WP-06 completed first; crash points are named and deterministic (`FailurePoint`), never a random kill.
- **Expected output:** `read_uncommitted` consumers seeing aborted records that `read_committed` never does; one transaction committing atomically across three separate topics or none at all; a second producer instance fencing the first under a shared `transactional.id`; a transactional pipeline reprocessing exactly once after a pre-commit crash and never duplicating after a post-commit one.
- **What we learned:** Kafka's idempotent producer and transactions guarantee no duplicate records from producer retries and no partially-visible transactional reads — guarantees entirely about Kafka's *own* state. They say nothing about a database write, an email, or a payment your application performs in response to a record; that's a separate reliability problem (idempotent consumer, transactional outbox — WP-12/WP-13), never something Kafka's EOS extends to for free. A fenced-out producer's next operation fails with `InvalidProducerEpochException`, not the commonly-assumed `ProducerFencedException` — verified against the real class hierarchy, not assumed from tutorials.

## Objective

Build a hands-on progression through producer retries, duplicate risk, the
idempotent producer, Kafka transactions, `read_committed`/`read_uncommitted`,
atomic multi-partition writes, and an atomic consume-transform-produce
pipeline — with enough real, captured evidence to answer two questions
precisely:

1. What does Kafka exactly-once semantics actually guarantee?
2. Why does that NOT automatically mean exactly-once business processing
   across Kafka + a database + external APIs?

See [`docs/transactions/TRANSACTIONS_AND_EXACTLY_ONCE_SEMANTICS.md`](../../docs/transactions/TRANSACTIONS_AND_EXACTLY_ONCE_SEMANTICS.md)
for the full conceptual depth, every experiment's real captured evidence,
the failure matrix, and 18 Principal Engineer questions. This README covers
setup, commands, and a condensed experiment walkthrough.

## Prerequisites

- Docker (this lab reuses the WP-07 3-broker cluster — see "Environment"
  below)
- JDK 21+
- Completion of WP-06 (replication/ISR) and WP-06's delivery-semantics lab
  (lab-05) — this lab builds directly on both

### Why a separate project

Same one-project-per-lab convention every prior lab uses — see lab-03's
README, "Why a separate project," for the full rationale.

## Environment

This lab reuses **`platform/kafka-cluster/`** (WP-07's 3-broker KRaft
cluster) rather than standing up a new environment — it already configures
`__transaction_state` with replication factor 3 / `min.insync.replicas 2`,
which is exactly what a cluster "configured appropriately for transactional
state" means. See the conceptual doc's "Environment" section for the full
reasoning, including why this lab's OWN automated tests instead use a
different, single-node Testcontainers cluster for speed.

## Architecture

```text
platform/kafka-cluster/  (reused from WP-07, unmodified)
  kafka-broker-1, kafka-broker-2, kafka-broker-3   (combined broker+controller, RF=3)

labs/lab-08-transactions-exactly-once/
  producer/  DuplicateRiskProducerApp, IdempotentProducerApp,
             TransactionalProducerApp, MultiPartitionTransactionApp,
             FencingDemoApp
  consumer/  IsolationLevelConsumerApp
  eos/       NonTransactionalConsumeTransformProduceApp,
             TransactionalConsumeTransformProduceApp
  support/   OrderEvent, LabConfig, FailurePoint, SimulatedCrashException
```

## Concepts

See the conceptual doc's Sections 1-11 for the full progression (duplicate
risk → idempotence → PID/epoch/sequence → transactions →
`transactional.id` → commit/abort → isolation levels → multi-partition
atomicity → the transaction coordinator → fencing) with real evidence for
each step.

## Setup

```bash
cd platform/kafka-cluster
docker compose up -d
```

Wait for all three brokers to report healthy (`docker compose ps`), then
create this lab's topics:

```bash
for t in txn-lab-orders txn-lab-payments txn-lab-audit txn-lab-fencing \
         txn-lab-pipeline-input txn-lab-pipeline-output; do
  docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh \
    --create --topic "$t" --partitions 3 --replication-factor 3 \
    --bootstrap-server kafka-broker-1:19092
done
```

### Kubernetes (k3d) alternative

```bash
platform-k8s/bootstrap-cluster.sh   # once
platform-k8s/kafka-cluster/setup.sh
```

Same three host ports as Docker Compose. Topic creation:
`kubectl -n kafka-cluster exec deploy/kafka-broker-1 --
//opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-broker-1:19092
--create ...`. See
[`platform-k8s/README.md`](../../platform-k8s/README.md) for the
internal-listener and path-mangling gotchas. Cleanup:
`platform-k8s/kafka-cluster/cleanup.sh [--wipe]`.

## Commands

| Task | What it runs |
|---|---|
| `./gradlew runDuplicateRiskDemo` | Experiment 1 — non-idempotent producer, app-level retry (Section 1) |
| `./gradlew runIdempotentProducer` | Experiment 2 — idempotent burst; inspect with `kafka-transactions.sh describe-producers` afterward (Section 2) |
| `./gradlew runTransactionalProducer -Paction=commit\|abort` | Experiments 3-4 — commit/abort visibility (Sections 6-7) |
| `./gradlew runIsolationConsumer -PisolationLevel=read_committed\|read_uncommitted` | Experiment 5 — isolation-level comparison (Section 8) |
| `./gradlew runMultiPartitionTransaction -Paction=commit\|abort` | Experiment 6 — atomic multi-topic transaction (Section 9) |
| `./gradlew runFencingDemo -PinstanceName=instance-A\|instance-B` | Experiment 7 — producer fencing, run twice with the same `transactionalId` (Section 11) |
| `./gradlew runNonTransactionalPipeline -PcrashAfterProduce=true` | Experiment 8 — duplicate output without transactions (Section 12) |
| `./gradlew runTransactionalPipeline -PfailurePoint=NONE\|BEFORE_COMMIT\|AFTER_COMMIT` | Experiments 9-11 — atomic consume-transform-produce, with crash injection (Sections 13-15) |

## Implementation

See the conceptual doc for full source-level discussion. Two design notes
worth calling out here:

- **`DuplicateRiskProducerApp` does not fake a deterministic ACK-loss
  reproduction.** The mechanism (app-level retry after an unknown-outcome
  send) is real and deterministic; whether a GIVEN run actually produces a
  duplicate depends on real timing against a real broker, which this lab
  does not force. See the conceptual doc, Section 1.
- **`TransactionalConsumeTransformProduceApp`'s crash points are
  batch-level**, not per-record — the atomic unit in a transactional
  pipeline is the whole transaction (one poll batch), not one record, so
  that is where `FailurePoint.BEFORE_COMMIT`/`AFTER_COMMIT` are injected.

## Expected output

Every runnable app prints its resolved configuration, each record's
topic/partition/offset, and an explicit narration of what Kafka guarantee
each step demonstrates. See the conceptual doc for full real captured
output from every experiment below.

## Verification

```bash
./gradlew test
```

10 automated integration tests, against a real single-node KRaft cluster
(Testcontainers) — see "Automated tests" below for the full list.

## Experiment

### Experiments 1-2 — Duplicate risk and idempotence

Run `runDuplicateRiskDemo`, optionally killing this topic-partition's
leader broker mid-run to try to land in the ack-loss window (real,
timing-dependent — see the conceptual doc, Section 1). Run
`runIdempotentProducer`, then inspect the real PID/epoch/sequence with:

```bash
docker exec kafka-broker-1 /opt/kafka/bin/kafka-transactions.sh \
  --bootstrap-server kafka-broker-1:19092 \
  describe-producers --topic txn-lab-idempotent --partition 0
```

### Experiments 3-5 — Commit, abort, and isolation levels

```bash
./gradlew runTransactionalProducer -Paction=commit -PstartEventNumber=100
./gradlew runTransactionalProducer -Paction=abort -PstartEventNumber=200
./gradlew runIsolationConsumer -PisolationLevel=read_committed -PrunForMs=8000
./gradlew runIsolationConsumer -PisolationLevel=read_uncommitted -PrunForMs=8000
```

Compare the two consumers' output directly — `read_uncommitted` shows the
aborted `Order-200`/`Order-201`/`Order-202` records; `read_committed` never
does. Real captured output: conceptual doc, Section 8.

### Experiment 6 — Atomic multi-partition transaction

```bash
./gradlew runMultiPartitionTransaction -Paction=commit -PeventId=Order-500
./gradlew runMultiPartitionTransaction -Paction=abort -PeventId=Order-501
```

Then check all three topics (`txn-lab-orders`, `txn-lab-payments`,
`txn-lab-audit`) with a `read_committed` console consumer — only
`Order-500`'s three records appear, never `Order-501`'s. Real captured
output: conceptual doc, Section 9.

### Experiment 7 — Producer fencing

```bash
# Terminal 1:
./gradlew runFencingDemo -PinstanceName=instance-A
# (sends one record, pauses for Enter)
# Terminal 2, while Terminal 1 is paused:
./gradlew runFencingDemo -PinstanceName=instance-B
# (runs to completion immediately)
# Terminal 1: press Enter
```

Instance A's next operation fails with `InvalidProducerEpochException` —
**not** `ProducerFencedException`, which many tutorials name for this
scenario; see the conceptual doc, Section 11, for the real exception, the
`javap`-verified class hierarchy, and why this matters.

### Experiments 8-11 — Consume-transform-produce, with and without transactions

```bash
# Seed one input record first, then:
./gradlew runNonTransactionalPipeline -PcrashAfterProduce=true    # crashes; leaves a stranded, uncommitted output
./gradlew runNonTransactionalPipeline -PcrashAfterProduce=false   # "restart" -- reprocesses and DUPLICATES the output

./gradlew runTransactionalPipeline -PfailurePoint=BEFORE_COMMIT   # crashes; nothing visible, hanging txn
./gradlew runTransactionalPipeline -PfailurePoint=NONE            # "restart" -- initTransactions() cleans up, reprocesses ONCE
./gradlew runTransactionalPipeline -PfailurePoint=AFTER_COMMIT    # crashes AFTER commit; output already durable
```

The automated tests are the authoritative, deterministic source of
evidence for these four scenarios (a manually-killed background shell job
is not a reliable way to time a Kafka consumer group's join/rebalance
window) — see "Automated tests" below and the conceptual doc, Sections
12-15, for their real results.

## Failure injection

- `DuplicateRiskProducerApp` — real broker kill, manual, timing-dependent
  (Section 1).
- `FencingDemoApp` — deterministic, via a second producer instance sharing
  the same `transactional.id` (Section 11).
- `NonTransactionalConsumeTransformProduceApp` /
  `TransactionalConsumeTransformProduceApp` — deterministic, via
  `crashAfterProduce`/`failurePoint`, using the same
  `SimulatedCrashException` philosophy lab-05 established for WP-06:
  a real process kill cannot be aimed at an exact line of application
  logic; this can.

## Troubleshooting

### `initTransactions()` fails with "The transaction timeout is larger than the maximum value allowed by the broker"

The broker's `transaction.max.timeout.ms` must be `>=` the client's
`transaction.timeout.ms` (60000 by default). This lab's test cluster hit
this exact error during development after an earlier version set
`transaction.max.timeout.ms=30000` to speed up test runs — see the
conceptual doc, Section 3, for the real error and the fix (remove the
override).

### `kafka-transactions.sh describe-producers` shows nothing for a topic

`__transaction_state` and per-partition producer state are only populated
once a producer has actually written to that partition. Produce at least
one record first.

## Cleanup

```bash
cd platform/kafka-cluster
docker compose down
```

This lab does not modify `platform/kafka-cluster/` — `docker compose down`
here does not affect WP-07's own experiments, and vice versa (they simply
cannot both be actively experimented on inside the same container
lifecycle at once, since it's the same environment).

## Automated tests

`./gradlew test` — 10 tests, against a real single-node KRaft cluster
(`TransactionsKafkaCluster`, Testcontainers):

1. `committedTransactionalRecordsAreVisibleToReadCommitted`
2. `abortedRecordsAreHiddenFromReadCommittedButVisibleToReadUncommitted`
3. `oneTransactionCommitsAtomicallyAcrossMultipleTopics`
4. `oneTransactionAbortDiscardsAllInvolvedTopicsTogether`
5. `secondProducerInstanceFencesTheFirstUnderTheSameTransactionalId`
6. `consumeTransformProduceIsExactlyOnceWithNoFailure`
7. `crashBeforeCommitLeavesNoVisibleOutputAndInputIsReprocessedOnRestart`
8. `crashAfterCommitDoesNotReprocessInputOnRestart`
9. `offsetsAndOutputAreCommittedAtomicallyInOneTransaction`
10. `nonTransactionalPipelineDuplicatesOutputWhenCrashingBeforeOffsetCommit`

Every assertion uses a bounded condition-polling loop (or a bounded
absence-check window) rather than a fixed sleep as the synchronization
mechanism, per this repository's established test convention.

## Production considerations

See the conceptual doc's Sections 17-23 for the dual-write problem, why
Kafka EOS does not extend to a database or an external API, the bridge to
the transactional outbox / idempotent consumer / CDC patterns (WP-11,
WP-12), the full terminology comparison, the failure matrix, and the
observability discussion.

## Principal Engineer questions

See the conceptual doc's Section 24 for all 18 questions with detailed,
experimentally-grounded answers.
