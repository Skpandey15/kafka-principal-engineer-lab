# Lab 13 — Kafka Streams

## Quick Summary

- **Why this lab:** To build three real Kafka Streams topologies (stateful aggregation, tumbling windows, a stream-table join) and prove, against a real multi-instance cluster, both of this WP's dedicated failure-matrix rows: a Streams process dying (task migration + changelog-based state restoration) and how standby replicas shrink that restoration cost.
- **How to run:** Reuse `platform/kafka-cluster/`, then `./gradlew runStreamsApp -Ptopology=aggregation|windowed|enriched` (add `-PprocessingGuarantee=exactly_once_v2` for EOS); `./gradlew test` for the 6-test suite (3 fast `TopologyTestDriver` unit tests, 3 real Testcontainers multi-instance failure tests).
- **Expected input:** Docker (for the integration tests only — `TopologyTestDriver` tests need no broker at all), JDK 21+.
- **Expected output:** A running total per customer; sums correctly reset at tumbling-window boundaries; enriched output only for matched join keys; full state restored on a survivor after killing one Streams instance, with measurably less restoration time when a standby replica already held a warm copy.
- **What we learned:** Kafka Streams rebuilds a migrated task's local state from its changelog topic — restoration time is proportional to changelog size, an availability cost, not a data-loss risk. A standby replica reduces that cost by keeping a warm copy elsewhere, not by eliminating the migration itself. A real serde finding: after a key-changing `.map()` in a join topology, `Joined.with(...)` must be supplied explicitly, or the topology fails fast with `ConfigException: Please specify a key serde`.

## Objective

Three real topologies (stateful aggregation, tumbling windows, a
stream-table join), tested two ways (`TopologyTestDriver` for DSL logic,
Testcontainers for real multi-instance failure behavior), covering both
of this WP's dedicated failure-matrix rows: a Streams process dying
(task migration + changelog-based state restoration) and how standby
replicas shrink that restoration cost.

See [`docs/kafka-streams/KAFKA_STREAMS.md`](../../docs/kafka-streams/KAFKA_STREAMS.md)
for the full conceptual depth, real captured evidence, a real serde
finding, and 7 Principal Engineer questions. This README covers setup,
commands, and a condensed experiment walkthrough.

## Prerequisites

- Docker (this lab reuses the WP-07 3-broker cluster; Kafka Streams is
  a client library, so no new platform infrastructure is added)
- JDK 21+

### Why a separate project

Same one-project-per-lab convention every prior lab uses -- see lab-03's
README, "Why a separate project," for the full rationale.

## Environment

Reuses **`platform/kafka-cluster/`** (WP-07) UNCHANGED -- nothing new to
run. This lab's own automated tests run against a dedicated, single-node
Testcontainers Kafka cluster (`StreamsTestCluster`), Kafka only.

## Architecture

```text
platform/kafka-cluster/          (reused, unmodified)

labs/lab-13-kafka-streams/
  support/   OrderEvent, LabConfig
  topology/  OrderAggregationTopology  -- state store, running total per customer
             WindowedOrderTopology     -- tumbling-window sum per customer
             EnrichedOrderTopology     -- stream-table join
  app/       StreamsApp                -- runnable app, picks a topology via -Ptopology=
```

## Setup

```bash
cd platform/kafka-cluster && docker compose up -d
```

## Commands

| Task | What it runs |
|---|---|
| `./gradlew runStreamsApp -Ptopology=aggregation` | Running total per customer (Section 2) |
| `./gradlew runStreamsApp -Ptopology=windowed` | Tumbling-window sums (Section 3) |
| `./gradlew runStreamsApp -Ptopology=enriched` | Stream-table join (Section 4) |
| `./gradlew runStreamsApp -Ptopology=aggregation -PprocessingGuarantee=exactly_once_v2` | Same topology, EOS enabled (Section 7) |

## Implementation

See the conceptual doc for full source-level discussion. One design note
worth calling out: each topology is a plain static `build(StreamsBuilder,
...)` method, not tangled into `StreamsApp` -- both the runnable app and
this lab's `TopologyTestDriver` unit tests build the EXACT same topology,
no test-only reimplementation to drift out of sync.

## Verification

```bash
./gradlew test
```

6 automated tests: 3 fast `TopologyTestDriver` unit tests (no broker at
all) plus 3 real Testcontainers integration tests (multi-instance
Kafka Streams against a real cluster) -- see "Automated tests" below.

## Experiment

### Experiments 1-3 — DSL fundamentals (state, windows, joins)

```bash
./gradlew test --tests "com.kafkalab.streams.topology.*"
```

Real captured evidence for aggregation, window-boundary behavior, and
join semantics (including the real `Joined.with(...)` serde finding):
conceptual doc, Sections 2-4.

### Experiment 4 — a Streams process dies

```bash
./gradlew test --tests "*killingOneStreamsInstance*"
```

Real captured evidence: task migration to the survivor, full state
restored from the changelog topic, a real `StateRestoreListener` count.
Conceptual doc, Section 5.

### Experiment 5 — standby replicas shrink restoration time

```bash
./gradlew test --tests "*StandbyReplica*"
```

Conceptual doc, Section 6.

## Failure injection

- Section 5 and 6's experiments ARE this WP's two dedicated
  failure-matrix rows ("Kafka Streams process dies",
  "Kafka Streams state restoration takes a long time") -- both real,
  automated, in the Testcontainers suite, not manual.

## Troubleshooting

### `ConfigException: Please specify a key serde` when building a join topology

A real finding -- see the conceptual doc, Section 4: after a
key-changing `.map()`, supply `Joined.with(...)` explicitly.

## Cleanup

```bash
cd platform/kafka-cluster && docker compose down
```

## Automated tests

`./gradlew test` -- 6 tests:

**`TopologyTestDriver` (no broker, no Testcontainers):**
1. `OrderAggregationTopologyTest#runningTotalAggregatesAcrossMultipleOrdersForTheSameCustomer`
2. `WindowedOrderTopologyTest#tumblingWindowSumsOrdersWithinAWindowAndStartsFreshInTheNextWindow`
3. `EnrichedOrderTopologyTest#streamTableJoinEnrichesMatchedOrdersAndDropsUnmatchedOnes`

**Testcontainers, real multi-instance Kafka Streams:**
4. `killingOneStreamsInstanceMigratesItsTasksAndRestoresFullStateOnTheSurvivor`
5. `aStandbyReplicaMeansFailoverRequiresLittleOrNoChangelogRestoration`
6. `exactlyOnceProcessingCommitsTransactionallyWithNoDuplicationAcrossMultipleCommits`

Every state-reachability assertion uses a bounded condition-polling loop
rather than a fixed sleep, per this repository's established test
convention -- with one deliberate, documented exception (the standby
replication catch-up wait, Section 6 of the conceptual doc), which has
no cheap public "caught up" signal to poll instead.

## Production considerations

See the conceptual doc's Section 9 for what this lab deliberately does
NOT build (an explicit window grace period, `GlobalKTable`, a
cross-instance interactive-query RPC layer) and why each is a reasonable
scope boundary.

## Principal Engineer questions

See the conceptual doc's Section 10 for all 7 questions with detailed,
experimentally-grounded answers.
