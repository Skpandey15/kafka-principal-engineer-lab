# Lab 19 — Multi-Cluster & Disaster Recovery

## Objective

Real MirrorMaker 2 — the actual `connect-mirror-maker.sh`, not a
hand-rolled replication loop — bridging TWO real, independent Kafka
clusters: real cross-cluster replication with MM2's own topic-renaming
convention, and real consumer-group offset translation for DR failover
via `RemoteClusterUtils.translateOffsets`.

See [`docs/multi-cluster/KAFKA_MULTI_CLUSTER_AND_DR.md`](../../docs/multi-cluster/KAFKA_MULTI_CLUSTER_AND_DR.md)
for the full conceptual depth — multi-cluster architecture trade-offs,
partition lifecycle engineering, cluster rebalancing & Cruise Control,
platform governance, cost engineering, Kafka on Kubernetes/Strimzi (all
conceptual), the three real findings behind this lab's tests, and 7
Principal Engineer questions. This README covers the condensed
experiment walkthrough.

## Prerequisites

- Docker
- JDK 21+

### Why a separate project

Same one-project-per-lab convention every prior lab uses — see lab-03's
README, "Why a separate project," for the full rationale.

### Why this WP's scope is narrower than its own roadmap line

WP-20's roadmap line names nine topics. Only multi-cluster
replication/DR needs genuinely new infrastructure this repository
hasn't built before (a second cluster); the rest are covered
conceptually, each grounded in either already-real prior evidence or an
explicit, pre-existing scope boundary. See the conceptual doc, Section
1, for the full reasoning and the topic-by-topic table.

## Environment

No persistent `platform/` environment — both experiments build their
own ephemeral pair of single-node Kafka clusters plus a real MM2
process via `TwoClusterEnvironment`, torn down automatically after each
test run.

## Architecture

```text
labs/lab-19-multi-cluster-dr/
  support/TwoClusterEnvironment.java   two real Kafka clusters + real MM2
  MultiClusterDrIntegrationTest.java   replication + offset-translation tests
```

```text
primary-kafka  --( MM2: primary -> secondary )-->  secondary-kafka
     |                                                    |
  orders-abc123                          primary.orders-abc123
  (real data)                            (real replicated data)

  consumer group commits offset 10 on primary
              |
              v
  RemoteClusterUtils.translateOffsets(...) on secondary
              |
              v
  translated offset ~10 for the SAME group, on primary.orders-abc123
```

## Verification

```bash
./gradlew test
```

2 automated integration tests, against a real, dedicated two-cluster
plus MM2 Testcontainers environment — see "Automated tests" below.

## Experiment

### Experiment 1 — real MM2 replication, real renamed topics

```bash
./gradlew test --tests "*mirrorMaker2RealReplicatesRecordsFromPrimaryToARenamedSecondaryTopic*"
```

Real, captured evidence: 3 records produced on `primary`'s
`orders-<id>` topic show up on `secondary` under MM2's real, default
remote-topic naming convention, `primary.orders-<id>` — not this lab's
own naming choice, MM2's `DefaultReplicationPolicy`.

### Experiment 2 — real DR offset translation for failover

```bash
./gradlew test --tests "*consumerGroupOffsetsAreRealTranslatedAcrossClustersForDrFailover*"
```

Real, captured evidence: a consumer group commits exactly offset 10 of
20 on `primary`; `RemoteClusterUtils.translateOffsets` returns a
translated offset landing within one record of 10 on `secondary` —
proving a DR failover consumer would resume close to where the primary
group actually left off, not from scratch and not from an arbitrary
position. Conceptual doc, Section 3.2, for the three real bugs found
building this test.

## Failure injection

Neither test kills a broker for real — `TwoClusterEnvironment.killPrimary()`
exists (the same real hard-kill mechanic WP-19's `ThreeBrokerSimulationCluster`
uses) but is not yet exercised by a test; see the conceptual doc's Lab
limitations section (§9) for why, and what a follow-up failover test
would add. This lab's real findings are about correctness under normal
operation (replication naming, offset translation accuracy), not
recovery from an outage — the multi-cluster failure this WP's own
failure-matrix row describes ("Multi-cluster / region loss") is about
*architecture*, not a single broker crash already covered by WP-07/WP-19.

## Troubleshooting

### Translated offset stuck at a much lower value than the real committed offset

A real finding — see the conceptual doc, Section 3.2, Finding 3:
`offset.lag.max` (default 100) controls how densely MM2 samples
upstream/downstream offset pairs for checkpoint translation. At this
lab's small scale, the default left too few sample points to interpolate
accurately. Fixed in `TwoClusterEnvironment`'s `mm2.properties`
(`offset.lag.max = 1`) — confirmed via a raw dump of MM2's own
checkpoint topic (`primary.checkpoints.internal`) showing the actual
`upstream`/`downstream` pair MM2 had recorded.

### Translated offset briefly reflects an early, stale group position

A real finding — see the conceptual doc, Section 3.2, Finding 1: MM2
emits checkpoints continuously, on its own interval; the *first*
observed value is not necessarily the *latest*. Fixed by polling until
the translated value reaches the expected range, the same
bounded-condition-polling convention used everywhere else in this
repository.

### An explicit `commitSync` of offset 10 is silently overwritten back to 20

A real finding — see the conceptual doc, Section 3.2, Finding 2:
`KafkaConsumer`'s default `enable.auto.commit=true` means `close()`
performs its own final auto-commit of the consumer's actual internal
position, after any explicit commit already made. Fixed by disabling
auto-commit entirely for that test's consumer.

## Cleanup

Nothing to clean up manually — both experiments use ephemeral
Testcontainers clusters torn down automatically when each test
completes.

## Automated tests

`./gradlew test` — 2 tests, against a real, dedicated two-cluster plus
MM2 Testcontainers environment:

1. `MultiClusterDrIntegrationTest#mirrorMaker2RealReplicatesRecordsFromPrimaryToARenamedSecondaryTopic`
2. `MultiClusterDrIntegrationTest#consumerGroupOffsetsAreRealTranslatedAcrossClustersForDrFailover`

## Production considerations

- `offset.lag.max` must be sized against a topic's *actual* throughput,
  not left at its default — the default assumes production-scale
  volume; a low-volume topic needs it lowered, or DR failover will
  silently reprocess far more than necessary (conceptual doc, §3.2).
- This lab mirrors one direction only. A real active/active deployment
  mirroring the same topic bidirectionally needs explicit topic
  exclusion or a custom `ReplicationPolicy` to avoid re-mirroring a
  record back to the cluster it came from — see conceptual doc, §2.
- MM2 replication and offset-checkpoint translation are both
  asynchronous and eventually consistent — a real DR runbook must
  define an acceptable RPO *before* an incident, never derive one from
  "however fast MM2 happened to be" during a real failover.
- See the conceptual doc's Sections 4–8 for the remaining WP-20 topics
  (partition lifecycle, cluster rebalancing/Cruise Control, platform
  governance, cost engineering, Kubernetes/Strimzi) — all conceptual,
  not built as infrastructure in this lab.

## Principal Engineer questions

See the conceptual doc's Section 10 for all 7 questions with detailed,
experimentally-grounded answers.
