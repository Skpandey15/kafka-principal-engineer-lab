# Lab 10 — Kafka Connect & CDC

## Objective

Build a hands-on progression through Kafka Connect's worker/task/offset
model (pure Apache Kafka, no vendor plugin) and then real change data
capture with Debezium — with enough real, captured evidence to answer:

1. How does Kafka Connect move data in and out of Kafka without
   hand-written glue code?
2. How do database changes become a real, reliable Kafka event stream?

See [`docs/kafka-connect/KAFKA_CONNECT_AND_CDC.md`](../../docs/kafka-connect/KAFKA_CONNECT_AND_CDC.md)
for the full conceptual depth, every experiment's real captured evidence
(including seven real findings this lab's own build process hit and
fixed), the failure matrix, and 12 Principal Engineer questions. This
README covers setup, commands, and a condensed experiment walkthrough.

## Prerequisites

- Docker (this lab reuses the WP-07 3-broker cluster and adds a
  PostgreSQL + Kafka Connect worker environment — see "Environment"
  below)
- JDK 21+
- Completion of WP-09 (transactions) is useful context for Section 13's
  bridge to WP-12 (transactional outbox), though not a hard prerequisite

### Why a separate project

Same one-project-per-lab convention every prior lab uses — see lab-03's
README, "Why a separate project," for the full rationale.

## Environment

This lab reuses **`platform/kafka-cluster/`** (WP-07's 3-broker cluster,
unmodified) and adds **`platform/kafka-connect/`** — a NEW, separate
compose file running a real PostgreSQL database (configured for logical
replication) and a real Kafka Connect distributed-mode worker, both on
the existing `kafka-cluster_default` network. See the conceptual doc,
Section 1, for why the worker itself is just `apache/kafka:4.3.1` (the
same image as the broker) rather than a separate vendor image.

This lab's own automated tests run against a dedicated, single-node
Testcontainers cluster (`KafkaConnectCluster`) pairing Kafka, PostgreSQL,
and a Connect worker together.

## Architecture

```text
platform/kafka-cluster/    (reused from WP-07, unmodified)
  kafka-broker-1, kafka-broker-2, kafka-broker-3

platform/kafka-connect/    (new for this WP)
  postgres           (postgres:17.6, wal_level=logical)
  connect-worker     (apache/kafka:4.3.1, running connect-distributed.sh)
  fetch-plugins.sh   (downloads connect-file + the Debezium plugin)

labs/lab-10-kafka-connect-cdc/
  support/  LabConfig, ConnectRestClient
  connect/  FileStreamDemoApp, ConnectOffsetsInspectorApp   (Phase 1)
  cdc/      DebeziumConnectorRegistrationApp, OrdersJdbcSeedApp,
            CdcEventConsumerApp                              (Phase 2)
```

## Concepts

See the conceptual doc's Sections 1-3 for the worker/task/offset model
and why Kafka Connect's REST API is the only client interface.

## Setup

```bash
cd platform/kafka-cluster
docker compose up -d
# wait for all three brokers to report healthy

cd ../kafka-connect
bash fetch-plugins.sh
docker compose up -d
# wait for both containers to report healthy: curl http://localhost:8083/connectors
```

## Commands

| Task | What it runs |
|---|---|
| `./gradlew runFileStreamDemo` | Experiment 1 — real FileStream source + sink connectors (Sections 4-5) |
| `./gradlew inspectConnectOffsets` | Experiment 2 — dump the real `connect-offsets` topic |
| `./gradlew registerDebeziumConnector` | Experiment 3 — register the real Debezium PostgreSQL connector (Section 7) |
| `./gradlew seedOrders -Paction=insert\|update\|delete -PorderId=... -Pamount=...` | Drive real CDC events via plain JDBC |
| `./gradlew runCdcConsumer` | Decode Debezium's real envelope (op/before/after/source) (Section 8) |

## Implementation

See the conceptual doc for full source-level discussion. Two design notes
worth calling out here:

- **Every connector name, topic, and file in the runnable demo apps
  includes a fresh, unique suffix per run.** Not cosmetic — a real
  correctness requirement, since Kafka Connect's offset storage outlives
  a connector's own delete/recreate lifecycle (conceptual doc, Section 5).
- **`CdcEventConsumerApp` decodes Debezium's envelope using Kafka
  Connect's own `JsonConverter`/`Struct` API**, not hand-parsed JSON —
  this is what correctly resolves the `Decimal` logical type (the
  `amount` field) into a real `BigDecimal` without reimplementing that
  decoding by hand.

## Expected output

Every runnable app prints its resolved configuration, real connector/task
states as it registers or inspects them, and an explicit narration of
what each step demonstrates. See the conceptual doc for full real
captured output from every experiment below.

## Verification

```bash
./gradlew test
```

9 automated integration tests, against a real Kafka + PostgreSQL + Kafka
Connect cluster (Testcontainers) — see "Automated tests" below for the
full list.

## Experiment

### Experiments 1-2 — Kafka Connect fundamentals

```bash
./gradlew runFileStreamDemo
./gradlew inspectConnectOffsets
```

Real captured output, including a source connector's own offset
representation (a byte position, not a Kafka offset): conceptual doc,
Sections 4-5.

### Experiment 3 — Debezium CDC: insert, update, delete

```bash
./gradlew registerDebeziumConnector
./gradlew seedOrders -Paction=insert -PorderId=O-1 -Pamount=10.00
./gradlew seedOrders -Paction=update -PorderId=O-1 -Pamount=20.00
./gradlew seedOrders -Paction=delete -PorderId=O-1
./gradlew runCdcConsumer
```

Real captured Debezium envelopes for all three operations, including the
`REPLICA IDENTITY FULL` before/after evidence and the post-delete
tombstone: conceptual doc, Sections 6-8.

### Experiment 4 — Connect task failure and source DB unavailability

Manual, operator-driven (matching this repository's established pattern
for infrastructure-level failure experiments — see
`docs/references/REFERENCE_REPOSITORIES.md`'s testing-progression
guidance): with the Debezium connector running,
`docker compose -f platform/kafka-connect/docker-compose.yml stop postgres`,
observe the real logs (`docker logs connect-worker`), then
`docker compose ... start postgres` and confirm resumption. Real captured
evidence, including the exact LSN resume behavior: conceptual doc,
Sections 10-11.

## Failure injection

- Sections 10-11 (Connect task failure behavior, source database
  unavailability) — real, operator-driven, via `docker compose stop`/
  `start` against `platform/kafka-connect/`'s Postgres container.

## Troubleshooting

### `Failed to find any class that implements Connector` for `FileStreamSourceConnector`

Run `bash platform/kafka-connect/fetch-plugins.sh` — `connect-file` is
NOT on the worker's default classpath despite shipping inside the same
image; see the conceptual doc, Section 4, for the real finding.

### `FATAL: invalid value for parameter "TimeZone"` from `seedOrders`

A real, environment-specific finding — see the conceptual doc, Section 14.
Already fixed in `OrdersJdbcSeedApp` (`TimeZone.setDefault(UTC)` before
connecting); if you hit this in your OWN JDBC code against this lab's
Postgres, apply the same fix.

### `FATAL: number of requested standby connections exceeds "max_wal_senders"`

Each registered Debezium connector holds one open WAL sender for as long
as it stays registered. Delete connectors you're done with
(`DELETE /connectors/{name}`), or see the conceptual doc, Section 14, for
why this environment's `max_wal_senders` is already raised to 20.

## Cleanup

```bash
cd platform/kafka-connect
docker compose down

cd ../kafka-cluster
docker compose down
```

Neither this lab nor `platform/kafka-connect/` modifies
`platform/kafka-cluster/` itself.

## Automated tests

`./gradlew test` — 9 tests, against a real Kafka + PostgreSQL + Kafka
Connect cluster (`KafkaConnectCluster`, Testcontainers):

1. `fileStreamAndDebeziumPluginsAreBothDiscoveredOnTheWorker`
2. `fileStreamSourceAndSinkConnectorsMoveDataThroughKafkaEndToEnd`
3. `connectSourceOffsetIsARealFilePositionNotAKafkaOffset`
4. `debeziumConnectorReachesRunningState`
5. `insertProducesACreateEventWithNullBeforeAndPopulatedAfter`
6. `updateProducesAnEventWithFullBeforeAndAfterDueToReplicaIdentityFull`
7. `deleteProducesAnEventThenATombstone`
8. `preExistingRowsAreCapturedAsASnapshotWithFirstAndLastMarkers`
9. `deletingAConnectorDoesNotClearItsStoredOffsets`

Every assertion uses a bounded condition-polling loop rather than a
fixed sleep as the synchronization mechanism, per this repository's
established test convention. The container-stop/restart failure
experiments (Sections 10-11) are deliberately manual, not automated —
see the conceptual doc, Section 15, for why.

## Production considerations

See the conceptual doc's Sections 10-14 for real failure behavior
(Connect task failure, source database unavailability, WAL retention
risk), the polling-vs-CDC architectural comparison, and the bridge to
the transactional outbox pattern (WP-12).

## Principal Engineer questions

See the conceptual doc's Section 17 for all 12 questions with detailed,
experimentally-grounded answers.
