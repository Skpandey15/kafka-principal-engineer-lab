# Lab 09 — Schema Evolution & Schema Governance

## Objective

Build a hands-on progression through serialization, Avro/Protobuf/JSON
Schema, Confluent Schema Registry, schema IDs/subjects/versions,
compatibility modes, and schema governance — with enough real, captured
evidence to answer two questions precisely:

1. How can Kafka producers and consumers be deployed independently while
   their event contracts evolve safely?
2. What actually happens when someone introduces an incompatible schema
   into a production Kafka ecosystem?

See [`docs/schema-evolution/SCHEMA_EVOLUTION_AND_GOVERNANCE.md`](../../docs/schema-evolution/SCHEMA_EVOLUTION_AND_GOVERNANCE.md)
for the full conceptual depth, every experiment's real captured evidence
(including two real, surprising findings this lab's own first attempts
got wrong and corrected), the failure matrix, and 24 Principal Engineer
questions. This README covers setup, commands, and a condensed experiment
walkthrough.

## Prerequisites

- Docker (this lab reuses the WP-07 3-broker cluster and adds a Schema
  Registry container — see "Environment" below)
- JDK 21+
- Completion of WP-06 (delivery semantics) — Section 33's poison-message
  discussion builds on it directly

### Why a separate project

Same one-project-per-lab convention every prior lab uses — see lab-03's
README, "Why a separate project," for the full rationale.

## Environment

This lab reuses **`platform/kafka-cluster/`** (WP-07's 3-broker cluster,
unmodified) and adds **`platform/schema-registry/`** — a NEW, separate
compose file running `confluentinc/cp-schema-registry:7.9.2`, attached to
the EXISTING `kafka-cluster_default` network as an external network. See
the conceptual doc, Section 1, for why Schema Registry is deliberately
kept in its own compose file rather than folded into WP-07's — it is an
ecosystem component layered around Kafka, not part of the broker
protocol, and this repository layout makes that boundary visible.

This lab's OWN automated tests instead run against a dedicated,
single-node Testcontainers cluster (`SchemaRegistryKafkaCluster`) pairing
a single-node Kafka broker with its own Schema Registry container, for
speed and test isolation.

## Architecture

```text
platform/kafka-cluster/    (reused from WP-07, unmodified)
  kafka-broker-1, kafka-broker-2, kafka-broker-3

platform/schema-registry/  (new for this WP)
  schema-registry           (confluentinc/cp-schema-registry:7.9.2)

labs/lab-09-schema-evolution-governance/
  src/main/avro/    order-event-v1.avsc, v2 (compatible), v3 (transitive
                    trap), and 4 deliberately-incompatible variants
  src/main/proto/   order_event.proto
  raw/         RawBytesDemoApp, VersionSkewDemoApp   (no schema/registry)
  avro/        AvroOrderEventProducerApp/ConsumerApp, WireFormatInspectorApp,
               ReaderWriterResolutionDemoApp, NamingStrategyDemoApp,
               RegistryFailureDemoApp
  protobuf/    ProtobufOrderEventProducerApp
  jsonschema/  JsonSchemaOrderEventProducerApp, OrderEventJson
  ci/          SchemaCompatibilityGateApp
```

## Concepts

See the conceptual doc's Sections 1-11 for the full progression (raw
bytes → version skew → Avro → Schema Registry architecture/caching →
wire format → schema ID/subject/version → naming strategies), each with
real captured evidence.

## Setup

```bash
cd platform/kafka-cluster
docker compose up -d
# wait for all three brokers to report healthy

cd ../schema-registry
docker compose up -d
# wait for schema-registry to report healthy: curl http://localhost:8081/subjects
```

## Commands

| Task | What it runs |
|---|---|
| `./gradlew runRawBytesDemo` | Experiment 1 — no schema governance at all (Section 2) |
| `./gradlew runVersionSkewDemo -PproducerVersion=v1\|v2 -PconsumerVersion=v1\|v2` | Experiment 2 — version skew, both directions (Section 3) |
| `./gradlew runAvroProducer -Pschema=v1\|v2\|...` | Experiments 3-5 — real Avro + registry (Sections 4-6) |
| `./gradlew runAvroConsumer` | Consume Avro records (writer-schema-shaped, no reader pinned) |
| `./gradlew checkAvroCompatibility -PschemaFile=...` | The CI compatibility gate (Section 22) |
| `./gradlew inspectWireFormat` | Experiment 6 — byte-level wire format inspection (Section 9) |
| `./gradlew runReaderWriterResolutionDemo` | Experiment 7 — real Avro schema resolution, offline (Section 17) |
| `./gradlew runNamingStrategyDemo` | Experiment 8 — `RecordNameStrategy`, multiple event types on one topic (Section 11) |
| `./gradlew runProtobufProducer` | Experiment 9 — real Protobuf + registry (Section 18) |
| `./gradlew runJsonSchemaProducer` | Experiment 10 — real JSON Schema + registry (Section 19) |
| `./gradlew runRegistryFailureDemo` | Experiment 11 — pauses mid-run for a real registry outage (Sections 29-30) |

## Implementation

See the conceptual doc for full source-level discussion. Two corrections
worth calling out here, since both changed what this lab actually ships:

- **`protobuf-java` is pinned to `3.25.5`, not the newer `4.x` line** —
  using `4.29.3` caused a real `java.lang.VerifyError` the moment a
  Protobuf record was actually produced, because
  `kafka-protobuf-serializer:7.9.2` is binary-incompatible with
  protobuf-java 4.x's internal class hierarchy. See the conceptual doc,
  Section 18.
- **`SchemaCompatibilityGateApp` does NOT use the obvious
  `testCompatibilityVerbose()` convenience method** — that method (and
  the REST endpoint it calls) only ever checks a candidate against the
  LATEST registered version, even under a `*_TRANSITIVE` compatibility
  mode, which would give CI a false PASS for a real transitive
  incompatibility. See the conceptual doc, Section 16, for the real
  evidence this was caught with.

## Expected output

Every runnable app prints its resolved configuration, real schema
IDs/versions/subjects as it registers or resolves them, and an explicit
narration of what each step demonstrates. See the conceptual doc for full
real captured output from every experiment below.

## Verification

```bash
./gradlew test
```

12 automated integration tests, against a real Kafka + Schema Registry
cluster (Testcontainers) — see "Automated tests" below for the full list.

## Experiment

### Experiments 1-2 — No governance, then version skew

```bash
./gradlew runRawBytesDemo
./gradlew runVersionSkewDemo -PproducerVersion=v2 -PconsumerVersion=v1
./gradlew runVersionSkewDemo -PproducerVersion=v1 -PconsumerVersion=v2
```

Real captured output for both skew directions: conceptual doc, Section 3.

### Experiments 3-5 — Avro, compatible and breaking evolution

```bash
./gradlew runAvroProducer -Pschema=v1
./gradlew runAvroProducer -Pschema=v2
./gradlew checkAvroCompatibility -PschemaFile=src/main/avro/order-event-v2-renamed-field.avsc
```

Real registry rejection reasons, including the corrected "amount:
decimal → string" assumption: conceptual doc, Sections 5-6, 21.

### Experiment 6 — Wire format

```bash
./gradlew inspectWireFormat
```

Real magic-byte + schema-ID hex dump: conceptual doc, Section 9.

### Experiment 7 — Reader/writer schema resolution

```bash
./gradlew runReaderWriterResolutionDemo
```

Real `AvroTypeException` and real default-filling: conceptual doc,
Section 17.

### Experiment 8 — Naming strategies

```bash
./gradlew runNamingStrategyDemo
```

Real evidence that the same schema ID can be shared across two different
subjects: conceptual doc, Sections 10-11.

### Experiment 9 — Protobuf

```bash
./gradlew runProtobufProducer
```

### Experiment 10 — JSON Schema

```bash
./gradlew runJsonSchemaProducer
```

Real "open content model" rejection and its fix: conceptual doc, Section
19.

### Experiment 11 — Registry failure

```bash
./gradlew runRegistryFailureDemo
# follow the printed instruction to stop platform/schema-registry, then
# press Enter to continue
```

Real evidence for cached-vs-uncached client behavior: conceptual doc,
Section 29.

## Failure injection

- `checkAvroCompatibility` / direct registry REST calls — deterministic,
  real registry rejections (Sections 6, 16, 19, 21).
- `RegistryFailureDemoApp` — real, operator-driven registry outage
  (Section 29).
- The automated `registryUnavailableStillServesAlreadyCachedSchemasButFailsOnNewLookups`
  test — the same scenario, fully deterministic, using a disposable
  dedicated cluster.

## Troubleshooting

### `java.lang.VerifyError: Bad type on operand stack` when running the Protobuf producer

A `protobuf-java` version mismatch — see the conceptual doc, Section 18,
for the real error and why `3.25.5` (not `4.x`) is pinned in
`build.gradle`.

### `checkAvroCompatibility` passes when you expected it to fail under a transitive compatibility mode

Confirm you're running THIS lab's `SchemaCompatibilityGateApp`, not a
naive implementation built on `testCompatibilityVerbose()` alone — see
the conceptual doc, Section 16, for why that convenience method silently
ignores transitivity.

### `initTransactions`-style "connection refused" errors from Avro apps

Confirm `platform/schema-registry`'s container is healthy:
`curl http://localhost:8081/subjects`.

## Cleanup

```bash
cd platform/schema-registry
docker compose down

cd ../kafka-cluster
docker compose down
```

Neither this lab nor `platform/schema-registry/` modifies
`platform/kafka-cluster/` itself.

## Automated tests

`./gradlew test` — 12 tests, against a real Kafka + Schema Registry
cluster (`SchemaRegistryKafkaCluster`, Testcontainers):

1. `avroV1ProducesAndConsumesSuccessfully`
2. `compatibleAvroV2RegistersSuccessfully`
3. `incompatibleAvroRegistrationIsRejected`
4. `oldWriterDataIsReadableByACompatibleNewReaderSchemaWithDefaultsApplied`
5. `newWriterDataIsReadableByAnOlderCompatibleReaderSchema`
6. `schemaIdIsDistinctFromSubjectAndVersion`
7. `protobufSchemaEvolvesCompatibly`
8. `jsonSchemaValidatesNewOptionalPropertyButRegistryRejectsItByDefault` (a real, corrected finding — see the conceptual doc, Section 19)
9. `compatibilityModeNoneAcceptsWhatBackwardWouldReject`
10. `replayingOldRecordsWithAnEvolvedSchemaSucceeds`
11. `ciCompatibilityGateMechanismDistinguishesPassAndFail`
12. `registryUnavailableStillServesAlreadyCachedSchemasButFailsOnNewLookups`

Every assertion uses a bounded condition-polling loop (or a real
container-stop-and-wait) rather than a fixed sleep as the synchronization
mechanism, per this repository's established test convention.

## Production considerations

See the conceptual doc's Sections 23-33 for schema ownership, migration
strategies for genuinely breaking changes, event versioning, the replay
and consumer-lag connections, the multi-team architectural use case,
registry-failure behavior, security considerations (full depth is
WP-18), observability (full depth is WP-16), and the poison-message
connection to WP-13.

## Principal Engineer questions

See the conceptual doc's Section 35 for all 24 questions with detailed,
experimentally-grounded answers.
