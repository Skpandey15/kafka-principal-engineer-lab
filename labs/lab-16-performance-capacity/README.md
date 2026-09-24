# Lab 16 — Performance & Capacity Engineering

## Quick Summary

- **Why this lab:** To measure batching and compression's real effect on producer throughput with a hand-built benchmark (never a wrapped `kafka-producer-perf-test.sh` call), and turn that MEASURED throughput into a worked capacity workbook — tuning and sizing taught together, never as separate tracks or invented numbers.
- **How to run:** Start `platform/kafka-cluster/` for manual runs, then `./gradlew runBenchmark -Pprofile=unbatched|batched|batched-lz4`; `./gradlew test` for the 7-test suite (3 pure-arithmetic `CapacityCalculator` unit tests, 4 real Testcontainers integration tests).
- **Expected input:** Docker (for manual benchmark runs and the integration tests), JDK 21+.
- **Expected output:** Real records/sec, MB/sec, and p50/p95/p99 latency numbers comparing unbatched vs. batched production; a real, measured `compression-rate-avg` for LZ4; partition count proven as a real ceiling on consumer parallelism; `fetch.min.bytes`/`fetch.max.wait.ms` trading latency for fewer, larger fetches.
- **What we learned:** Every throughput/latency claim in this lab compares two real measurements taken moments apart on the *same* hardware against each other — never against a fixed absolute number, since an absolute figure would be environment-dependent and flaky (and this repository's own rule against invented benchmark numbers forbids publishing one anyway). The `CapacityCalculator` takes a measured per-partition throughput ceiling as its input, not an assumed one — capacity planning grounded in this lab's own evidence, not a formula copied from elsewhere.

## Objective

A real, hand-built producer benchmark measuring the effect of batching
and compression, real consumer-side partition-parallelism and
`fetch.min.bytes`/`fetch.max.wait.ms` experiments, and a "worked
capacity workbook" as real, tested arithmetic that takes MEASURED
throughput as an input rather than an assumed number — tuning and
sizing taught together, per the roadmap's own framing for this WP.

See [`docs/performance/KAFKA_PERFORMANCE_AND_CAPACITY.md`](../../docs/performance/KAFKA_PERFORMANCE_AND_CAPACITY.md)
for the full conceptual depth, real captured evidence, and 7 Principal
Engineer questions. This README covers setup, commands, and a condensed
experiment walkthrough.

## Prerequisites

- Docker (this lab reuses the WP-07 3-broker cluster for manual runs;
  its own automated tests use a dedicated single-node Testcontainers
  cluster)
- JDK 21+

### Why a separate project

Same one-project-per-lab convention every prior lab uses — see lab-03's
README, "Why a separate project," for the full rationale.

## Environment

Reuses **`platform/kafka-cluster/`** (WP-07) for manual runs via
`runBenchmark`. This lab's own automated tests run against a dedicated,
single-node Testcontainers Kafka cluster (`PerformanceTestCluster`).

## Architecture

```text
platform/kafka-cluster/    (reused for manual runs only)

labs/lab-16-performance-capacity/
  support/   PayloadGenerator, LabConfig
  bench/     ProducerBenchmark (real, hand-built), BenchmarkApp
  capacity/  CapacityCalculator (real, tested arithmetic)
```

## Setup

```bash
cd platform/kafka-cluster && docker compose up -d
```

(Not needed to run `./gradlew test` — only for `./gradlew runBenchmark`.)

## Commands

| Task | What it runs |
|---|---|
| `./gradlew runBenchmark -Pprofile=unbatched` | batch.size=1, linger.ms=0 |
| `./gradlew runBenchmark -Pprofile=batched` | linger.ms=20, batch.size=64KB |
| `./gradlew runBenchmark -Pprofile=batched-lz4` | batched + lz4 compression |

## Implementation

See the conceptual doc for full source-level discussion. One design
note worth calling out: `ProducerBenchmark` is a plain class around a
real `KafkaProducer`, not a wrapped `kafka-producer-perf-test.sh` call
— every config knob's real effect is directly attributable, since the
SAME method runs with only the config map changed between calls.

## Verification

```bash
./gradlew test
```

7 automated tests: 3 pure-arithmetic `CapacityCalculator` unit tests
(no cluster) plus 4 real Testcontainers integration tests — see
"Automated tests" below.

## Experiment

### Experiment 1 — batching's real throughput effect

```bash
./gradlew runBenchmark -Pprofile=unbatched
./gradlew runBenchmark -Pprofile=batched
```

Real, measured evidence (records/sec, MB/sec, p50/p95/p99 latency):
conceptual doc, Section 3.

### Experiment 2 — compression's real byte-size effect

```bash
./gradlew runBenchmark -Pprofile=batched-lz4
```

Real `compression-rate-avg` evidence: conceptual doc, Section 4.

### Experiments 3-4 — partition-count ceilings (throughput and parallelism)

```bash
./gradlew test --tests "*partitionCountIsARealCeilingOnConsumerParallelism*"
./gradlew test --tests "*FetchMinBytes*"
```

Real captured evidence: conceptual doc, Sections 5-6.

## Failure injection

No dedicated failure-matrix row names this WP.

## Troubleshooting

No lab-specific issues encountered building this WP — every experiment
passed on its first real run against a live cluster.

## Cleanup

```bash
cd platform/kafka-cluster && docker compose down
```

## Automated tests

`./gradlew test` — 7 tests:

**Pure arithmetic (no cluster):**
1. `CapacityCalculatorTest#estimateStorageAccountsForEveryReplicaSeparately`
2. `CapacityCalculatorTest#requiredPartitionsForThroughputRoundsUpNotDown`
3. `CapacityCalculatorTest#requiredPartitionsForConsumerParallelismMatchesDesiredInstanceCount`

**Testcontainers, real single-node cluster:**
4. `batchingRealMeasurablyImprovesThroughputOverUnbatchedProduction`
5. `compressionRealMeasurablyReducesBytesSentForCompressibleData`
6. `partitionCountIsARealCeilingOnConsumerParallelism`
7. `lowFetchMinBytesReturnsQuicklyWhileHighFetchMinBytesWaitsForTheRealMaxWait`

Every throughput/latency assertion compares two REAL measurements taken
moments apart on the same hardware against EACH OTHER, never against a
fixed absolute number — see the conceptual doc, Section 8, for why
absolute numbers would be environment-dependent and flaky.

## Production considerations

See the conceptual doc's Section 7 for the worked capacity workbook
(storage, throughput-driven partition count, parallelism-driven
partition count) and Section 8 for what this lab deliberately does not
build.

## Principal Engineer questions

See the conceptual doc's Section 9 for all 7 questions with detailed,
experimentally-grounded answers.
