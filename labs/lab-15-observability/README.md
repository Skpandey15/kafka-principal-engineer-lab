# Lab 15 — Observability & Troubleshooting

## Objective

A real JMX exporter → Prometheus → Grafana pipeline against the WP-07
cluster, plus real experiments in consumer lag, hot partitions, and
this WP's dedicated failure-matrix row: sustained consumer lag with no
Kafka-side backpressure, and the actual data loss that follows once
retention catches up to a lagging group.

See [`docs/observability/KAFKA_OBSERVABILITY.md`](../../docs/observability/KAFKA_OBSERVABILITY.md)
for the full conceptual depth, real captured evidence, real findings
(including why a container-wide `KAFKA_OPTS` broke this lab's own
healthchecks), and 7 Principal Engineer questions. This README covers
setup, commands, and a condensed experiment walkthrough.

## Prerequisites

- Docker (this lab reuses the WP-07 3-broker cluster -- now also
  running a real Prometheus JMX exporter agent per broker -- plus a
  new `platform/observability/` environment: Prometheus, Grafana,
  kafka-exporter)
- JDK 21+

### Why a separate project

Same one-project-per-lab convention every prior lab uses -- see lab-03's
README, "Why a separate project," for the full rationale.

### Why not Testcontainers here

Every prior lab's automated tests run against an ephemeral
Testcontainers-managed cluster. This WP's tests run against the
ALREADY-RUNNING, persistent `platform/kafka-cluster/` and
`platform/observability/` environments instead -- deliberately: this
WP's subject IS real, standing infrastructure (a JMX exporter attached
to a real broker process, a real Prometheus scraping it, a real Grafana
dashboard). See the conceptual doc, Section 1, for the full reasoning.

## Environment

Reuses **`platform/kafka-cluster/`** (WP-07), now ALSO running a real
Prometheus JMX exporter Java agent per broker (ports 7071-7073), and
adds **`platform/observability/`** -- Prometheus, Grafana, and
`kafka-exporter` (a real, separate community project that computes
consumer-group lag via the Admin API -- see the conceptual doc, Section
2, for why lag is NOT a broker-side JMX metric at all).

## Architecture

```text
platform/kafka-cluster/       (reused; +JMX exporter agent per broker)
  jmx-exporter/                kafka-broker.yml (real, verified rule set),
                                fetch-jmx-exporter.sh

platform/observability/       (new for this WP)
  prometheus                   scrapes the 3 brokers' JMX exporters + kafka-exporter
  kafka-exporter                real consumer-group lag, via the Admin API
  grafana                       kafka-overview.json (5 panels), auto-provisioned

labs/lab-15-observability/
  support/   LagInspector (native Admin-API lag), PrometheusQueryClient
```

## Setup

```bash
cd platform/kafka-cluster
bash jmx-exporter/fetch-jmx-exporter.sh
docker compose up -d
# wait for all three brokers to report healthy

cd ../observability
docker compose up -d
```

Grafana: http://localhost:3000 (anonymous admin access, local-only --
see "Production considerations"). Prometheus: http://localhost:9090.

## Commands

This lab has no runnable demo apps of its own -- every experiment is a
real, automated integration test (see "Automated tests" below); run
them directly, or inspect the same signals live in Grafana while they
run.

## Implementation

See the conceptual doc for full source-level discussion. One design
note worth calling out:`LagInspector` computes lag NATIVELY via the
Admin API (the same way `kafka-consumer-groups.sh --describe` does),
entirely independent of the Prometheus/kafka-exporter pipeline --
`nativeAdminApiLagMatchesThePrometheusLagMetric` cross-checks the two
independently-computed numbers against each other.

## Verification

```bash
./gradlew test
```

5 automated integration tests, against the real, already-running
platform -- see "Automated tests" below.

## Experiment

### Experiment 1 — consumer lag, natively and through the pipeline

```bash
./gradlew test --tests "*nativeAdminApiLagMatchesThePrometheusLagMetric*"
```

Real, cross-checked evidence: conceptual doc, Section 4.

### Experiment 2 — sustained lag, no backpressure (failure-matrix row)

```bash
./gradlew test --tests "*sustainedConsumerLagGrowsUnboundedWithNoProducerBackpressure*"
```

Real captured evidence: conceptual doc, Section 5.

### Experiment 3 — retention-exceeded data loss (the SAME failure-matrix row's stated consequence)

```bash
./gradlew test --tests "*lagExceedingRetentionCausesRealDataLossForTheLaggingGroup*"
```

Real captured evidence, including a real finding about segment-granular
deletion: conceptual doc, Section 6.

### Experiments 4-5 — hot partition and broker throughput

```bash
./gradlew test --tests "*aHotPartitionShowsRealUnevenPerPartitionThroughput*"
./gradlew test --tests "*brokerJmxMetricsReflectRealProducedThroughput*"
```

Conceptual doc, Sections 7-8.

## Failure injection

- Section 5 (sustained consumer lag, no backpressure) IS this WP's one
  dedicated failure-matrix row -- real, automated, not manual.
- Section 6 (retention-exceeded data loss) is the SAME row's stated
  consequence, also real and automated, using lab-speed
  retention/segment overrides (see the conceptual doc, Section 9, for
  why these are never production settings).

## Troubleshooting

### Broker healthcheck fails / `unhealthy` after adding the JMX exporter

A real finding -- see the conceptual doc, Section 3: a container-wide
`KAFKA_OPTS` is inherited by the healthcheck's own CLI invocation,
causing a real port-bind conflict with the already-running broker.
Already fixed in `platform/kafka-cluster/docker-compose.yml`
(`KAFKA_OPTS=` clears it for just that command).

### `kafka_consumergroup_lag` never appears in Prometheus

Confirm `platform/observability/` is running and `kafka-exporter`'s own
`--kafka.server` flags point at the real broker addresses
(`kafka-broker-1:19092`, etc.) -- check
`curl localhost:9308/metrics` directly first to rule out the exporter
itself before suspecting Prometheus's scrape config.

## Cleanup

```bash
cd platform/observability && docker compose down

cd ../kafka-cluster && docker compose down
```

## Automated tests

`./gradlew test` -- 5 tests, against the real, already-running
platform:

1. `nativeAdminApiLagMatchesThePrometheusLagMetric`
2. `sustainedConsumerLagGrowsUnboundedWithNoProducerBackpressure`
3. `lagExceedingRetentionCausesRealDataLossForTheLaggingGroup`
4. `aHotPartitionShowsRealUnevenPerPartitionThroughput`
5. `brokerJmxMetricsReflectRealProducedThroughput`

Every timing-sensitive assertion uses a bounded condition-polling loop,
per this repository's established test convention.

## Production considerations

See the conceptual doc's Section 9 for the three lab-speed overrides
this environment uses (Prometheus scrape interval, Kafka's retention
check interval, Grafana anonymous access) and why none of them belong
in a real deployment, and Section 10 for the operational diagnostic
sequence (curriculum-map row 23) this WP's own experiments are
structured around.

## Principal Engineer questions

See the conceptual doc's Section 12 for all 7 questions with detailed,
experimentally-grounded answers.
