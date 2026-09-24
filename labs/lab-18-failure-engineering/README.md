# Lab 18 — Failure Engineering & Production Simulation

## Quick Summary

- **Why this lab:** To cover the one failure-matrix row not yet demonstrated by an earlier WP (disk pressure, simulated safely) and to prove that two ALREADY-demonstrated failure mechanics (a real broker kill from WP-07, a real consumer-group rebalance from WP-05) still hold their individual guarantees when they happen to overlap in one running system.
- **How to run:** `./gradlew test` — both experiments use dedicated, ephemeral Testcontainers clusters (a size-capped single-node cluster for disk pressure, a real 3-broker cluster for the production simulation), no manual setup needed; run `--tests "*DiskPressureTest*"` or `--tests "*ProductionSimulationTest*"` individually.
- **Expected input:** Docker, JDK 21+; no persistent `platform/` environment is touched.
- **Expected output:** A real 48MB tmpfs genuinely filling and a real producer send failure surfacing once it does; continuous production/consumption surviving a real broker kill and a real consumer-group rebalance overlapping its recovery window, with every acknowledged record eventually consumed.
- **What we learned:** A 3-voter KRaft quorum needs a majority of voters reachable to elect a controller leader at all — starting three broker containers *sequentially* deadlocks, since each blocks waiting for a quorum the others haven't even started forming yet; fixed by starting them concurrently. Docker's default tmpfs mount is root-owned, but this image's broker process runs as a non-root user — fixed with `mode=1777` on the mount. Almost every failure this WP's own name lists was already `Demonstrated` in an earlier WP; cross-referencing the failure matrix directly (not just the roadmap line) is what revealed that and kept this WP's real scope to exactly the one new row plus the composition proof.

## Objective

Two real experiments: the ONE failure-matrix row explicitly assigned
to this WP (disk pressure, simulated safely via a real, size-capped
filesystem), and a production-simulation scenario that combines TWO
previously-separate failure mechanics — a real broker kill (WP-07) and
a real consumer-group rebalance (WP-05) — happening close together in
ONE running system, proving the guarantees each WP proved individually
still hold when multiple real failures overlap.

See [`docs/failure-recovery/KAFKA_FAILURE_ENGINEERING.md`](../../docs/failure-recovery/KAFKA_FAILURE_ENGINEERING.md)
for the full conceptual depth, real captured evidence, real findings
(including why starting a 3-voter KRaft quorum sequentially deadlocks),
and 7 Principal Engineer questions. This README covers the condensed
experiment walkthrough.

## Prerequisites

- Docker
- JDK 21+

### Why a separate project

Same one-project-per-lab convention every prior lab uses — see lab-03's
README, "Why a separate project," for the full rationale.

### Why this WP's scope is narrower than its own roadmap line

Cross-referencing `PRINCIPAL_ENGINEER_FAILURE_MATRIX.md` directly
(rather than treating the roadmap line as an independent to-do list)
shows almost every failure type this WP's name lists is ALREADY
`Demonstrated` in an earlier WP. The failure matrix itself assigns
this WP exactly ONE previously-unscheduled row (disk pressure) plus
the roadmap's own second instruction: a production-simulation scenario
combining prior labs' mechanics. See the conceptual doc, Section 1,
for the full reasoning.

## Environment

No persistent `platform/` environment — both experiments use dedicated,
ephemeral Testcontainers clusters (a size-capped single-node cluster
for disk pressure; a real 3-broker cluster for the production
simulation), torn down automatically after each test run.

## Architecture

```text
labs/lab-18-failure-engineering/
  disk/        DiskConstrainedKafkaContainer, DiskPressureTest
  simulation/  ThreeBrokerSimulationCluster, ProductionSimulationTest
```

## Verification

```bash
./gradlew test
```

2 automated integration tests, against real, dedicated Testcontainers
clusters — see "Automated tests" below.

## Experiment

### Experiment 1 — disk pressure, simulated safely

```bash
./gradlew test --tests "*DiskPressureTest*"
```

Real, captured evidence: a real 48MB tmpfs genuinely fills, and a real
producer send failure surfaces once it does. Conceptual doc, Section
2.

### Experiment 2 — a production simulation: broker kill + rebalance, overlapping

```bash
./gradlew test --tests "*ProductionSimulationTest*"
```

Real, captured evidence: continuous production/consumption, a real
broker kill, a real consumer-group rebalance overlapping its recovery
window, and confirmation that every acknowledged record is eventually
consumed despite both failures. Conceptual doc, Section 3.

## Failure injection

- Experiment 1 IS this WP's one dedicated failure-matrix row ("Disk
  fills on a broker") — real, automated, simulated safely via a
  size-capped tmpfs rather than the host's real disk.
- Experiment 2 combines two ALREADY-demonstrated failure mechanics
  (WP-07's broker failure, WP-05's rebalance) deliberately overlapping
  — not a new failure type, a systems-level composition proof.

## Troubleshooting

### `Timed out waiting for log output matching '.*Kafka Server started.*'` (3-broker cluster)

A real finding — see the conceptual doc, Section 4: a 3-voter KRaft
quorum needs a MAJORITY of voters reachable to elect a controller
leader at all; starting the three broker containers sequentially
deadlocks, since each one blocks waiting for a quorum the others
haven't even started forming yet. Fixed in
`ThreeBrokerSimulationCluster` (concurrent start).

### `java.nio.file.AccessDeniedException` writing the broker's bootstrap checkpoint (disk-pressure test)

A real finding — see the conceptual doc, Section 2: Docker's default
tmpfs mount is root-owned, but this image's broker process runs as a
non-root user. Already fixed (`mode=1777` on the tmpfs mount).

## Cleanup

Nothing to clean up manually — both experiments use ephemeral
Testcontainers clusters torn down automatically when each test
completes.

## Automated tests

`./gradlew test` — 2 tests, against real, dedicated Testcontainers
clusters:

1. `DiskPressureTest#writesFailOnceTheBrokersLogDirectoryGenuinelyFillsUp`
2. `ProductionSimulationTest#noDataIsLostWhenABrokerFailureAndAConsumerRebalanceOverlap`

## Production considerations

See the conceptual doc's Section 5 for what this lab deliberately does
NOT build (live disk-pressure recovery, a network-partition
experiment, exhaustive failure-combination coverage) and why each is a
reasonable scope boundary.

## Principal Engineer questions

See the conceptual doc's Section 6 for all 7 questions with detailed,
experimentally-grounded answers.
