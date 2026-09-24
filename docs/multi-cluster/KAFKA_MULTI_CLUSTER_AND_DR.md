# Multi-Cluster, Disaster Recovery & the Principal Engineer Capstone (WP-20)

## Prerequisites

- [`lab-07-kraft-controller-quorum-failure`](../../labs/lab-07-kraft-controller-quorum-failure/README.md) — a single cluster's own metadata quorum, before reasoning about two clusters
- [`lab-15-observability`](../../labs/lab-15-observability/README.md) — consumer lag and cross-checked metrics, the same instincts DR monitoring needs
- [`lab-18-failure-engineering`](../../labs/lab-18-failure-engineering/README.md) — broker failure and rebalance mechanics, the single-cluster failures a DR architecture sits on top of
- [`docs/performance/`](../performance/) — WP-17's `CapacityCalculator`, extended here for cross-cluster cost

## 1. What this WP actually scopes — and why

WP-20's own roadmap line names nine topics: multi-cluster architecture,
MirrorMaker 2, disaster recovery, partition lifecycle engineering, cluster
rebalancing/Cruise Control, platform governance, cost engineering, Kafka on
Kubernetes/Strimzi, and the Principal Engineer synthesis material (ADRs,
system design, source-code reading, interview banks, milestone assessments).
Building all nine as real, running infrastructure would violate this
repository's own rule against unnecessary abstraction (`CONTRIBUTING.md`
#4) — several of these topics are legitimately *architectural reasoning*
topics, not infrastructure this curriculum needs to stand up a second time
to teach.

This WP builds exactly **one** piece of genuinely new, real, tested
infrastructure — [`lab-19-multi-cluster-dr`](../../labs/lab-19-multi-cluster-dr/README.md),
two real Kafka clusters bridged by a real MirrorMaker 2 process — and covers
the remaining topics conceptually, each grounded in either this repository's
own prior real evidence or an explicit, already-stated scope boundary:

| Topic | Treatment | Why |
|---|---|---|
| Multi-cluster architecture, MM2, DR (offset translation) | **Real lab** | The one topic that needs new infrastructure this repository hasn't built yet — two clusters — to teach honestly |
| Partition lifecycle engineering | Conceptual (§4) | Builds directly on `docs/partitioning/`'s already-real partition-count trade-offs (WP-04); no new infrastructure needed to reason about what changes at scale |
| Cluster rebalancing & Cruise Control | Conceptual (§5) | Cruise Control is ecosystem tooling, not Apache Kafka itself (`REFERENCE_REPOSITORIES.md`'s Apache-vs-ecosystem boundary) — this curriculum explains what it automates and why Kafka doesn't do it natively, without operating a second control-plane product |
| Platform governance (quotas) | Conceptual, one real config example (§6) | Quota enforcement is a broker config, already inspectable against the existing `platform/` clusters; a dedicated lab would mostly re-exercise WP-18's ACL/authorizer infrastructure |
| Cost engineering | Conceptual, extends WP-17's real `CapacityCalculator` (§7) | The arithmetic (event rate × size × retention × replication × cross-region duplication) is a spreadsheet exercise on top of WP-17's already-measured throughput numbers, not new infrastructure |
| Kafka on Kubernetes / Strimzi | Conceptual only (§8) | The roadmap itself already marks this "Planned — explicitly not introduced into the current implementation" (curriculum-map row 27, before this WP was ever built) — this WP honors that pre-existing scope boundary rather than reversing it under capstone pressure |
| System design, ADRs, source-code reading, interview banks, milestone assessments | Written deliverables (`system-design/`, `adrs/`, `docs/principal-engineer/`, `interview/`) | Synthesis of the 19 prior WPs' real evidence, not new Kafka infrastructure |

## 2. Multi-cluster architecture: the two axes that actually matter

Every multi-cluster Kafka decision reduces to two independent axes — conflating
them is the most common real design mistake:

**Axis 1 — replication topology.** Active/passive (one cluster serves
traffic, the other is a warm standby fed by MM2) vs. active/active (both
clusters serve traffic, replicated bidirectionally). Active/active is not
"active/passive but better" — it introduces a new correctness problem
active/passive never has: two producers on the same logical topic, on two
clusters, can both be "correct" simultaneously, and MM2's own
`primary->secondary.enabled` / `secondary->primary.enabled` pair, if both
set `true` without topic exclusion, will happily replicate a record back to
the cluster it came from under a renamed topic, forever, unless something
(typically `replication.policy.class` filtering, or simply never
bidirectionally mirroring the *same* topic) prevents it. `lab-19` mirrors
one direction only (`secondary->primary.enabled = false`) precisely to keep
this experiment's scope to the mechanics that matter for DR, not to
implicitly endorse active/passive as universally correct.

**Axis 2 — RPO/RTO, not "how fast is replication."** Recovery Point
Objective (how much data can you lose) and Recovery Time Objective (how long
can you be down) are business requirements stated *before* the architecture,
never derived from whatever a chosen tool happens to achieve. MM2 replication
lag is one input to RPO — the acceptable one is a decision, not a
measurement. Kafka itself provides no cross-cluster failover mechanism at
all (the failure matrix's "Multi-cluster / region loss" row is explicit
about this): a DR runbook is entirely the operator's responsibility, MM2's
job is only to keep the secondary's data and consumer-group state close
enough to the primary that a failover is affordable.

## 3. The real lab: MirrorMaker 2 replication and DR offset translation

`lab-19-multi-cluster-dr` runs two real, independent single-node Kafka
clusters (`primary`, `secondary`) plus a real MM2 process — the actual
`connect-mirror-maker.sh` (confirmed present in the pinned
`apache/kafka:4.3.1` image, alongside `connect-mirror-4.3.1.jar` and
`connect-mirror-client-4.3.1.jar`), not a hand-rolled replication loop —
mirroring `primary -> secondary` only.

### 3.1 Experiment 1 — real replication, real renamed topics

A topic created and produced to on `primary` shows up on `secondary` under
MM2's real, default remote-topic naming convention:
`<source-cluster-alias>.<original-topic-name>` — e.g. `orders-abc123` on
`primary` becomes `primary.orders-abc123` on `secondary`. This is not this
lab's own naming choice; it's MM2's `DefaultReplicationPolicy`, confirmed by
the test actually finding data under that name.

### 3.2 Experiment 2 — DR failover needs offset translation, not just data

Replicating records alone does not make DR failover cheap. A consumer group
that has committed offset 10 of 20 on `primary` needs the *secondary*
cluster's equivalent offset — not 0 (which would mean reprocessing
everything) and not "whatever the raw record count is" — for a failover
consumer to resume correctly. `org.apache.kafka.connect.mirror.RemoteClusterUtils.translateOffsets(Map<String,Object> properties, String remoteClusterAlias, String consumerGroupId, Duration timeout)`
is MM2's own real client API for this: it reads the checkpoint records MM2's
checkpoint connector writes to the secondary cluster's internal checkpoints
topic and returns the translated `Map<TopicPartition, OffsetAndMetadata>`.

Three real, sequential bugs surfaced building this experiment, each
confirmed via an actual test run against real Docker infrastructure — not
guessed and not simulated:

**Finding 1 — MM2 emits checkpoints continuously; the first one observed
can be stale.** `translateOffsets` reflects whatever the checkpoint
connector last wrote, on its own `emit.checkpoints.interval.seconds` cadence
— it is not synchronous with the commit that produced it. Polling for the
*first* non-null result produced a real, confirmed false failure (translated
offset 1, when the real commit was 10). Fixed by polling in the same bounded
loop this repository uses everywhere else, but applied to a value that
changes over time rather than one that simply appears — see
`waitForTranslatedOffset` in
[`MultiClusterDrIntegrationTest.java`](../../labs/lab-19-multi-cluster-dr/src/test/java/com/kafkalab/multicluster/MultiClusterDrIntegrationTest.java).

**Finding 2 — `KafkaConsumer`'s default `enable.auto.commit=true` silently
overwrites an explicit commit.** The test intentionally commits exactly
offset 10 (not 0, not 20) to prove DR failover doesn't require full
reprocessing. With auto-commit left at its default, `consumer.close()`
performs its own final auto-commit of the consumer's actual internal fetch
position — which, since a single `poll()` can return every available record
at once, had already advanced to 20. The explicit `commitSync(Map.of(...,
new OffsetAndMetadata(10)))` was correct; it was being overwritten
*afterward*. Fixed by disabling auto-commit entirely, so only the deliberate
commit ever writes anything — the same class of bug WP-16's own consumer-lag
test hit first (an uncontrolled poll loop consuming more than intended), now
seen from a different angle (commit control, not consumption control).

**Finding 3 — `offset.lag.max` (default 100) makes checkpoint translation
imprecise at small scale.** Even with the auto-commit bug fixed, the
translated offset stayed stuck at exactly `1` for the full 90-second test
timeout — not a timing race (confirmed by first waiting for all 20 records
to be fully replicated to `secondary` *before* the group ever committed, and
the symptom persisted identically). Ground truth, captured by consuming
MM2's raw checkpoint topic (`primary.checkpoints.internal`) directly via
`org.apache.kafka.connect.mirror.Checkpoint.deserializeRecord`: the
checkpoint record itself read `upstream=10, downstream=1` — MM2 had
correctly read the real committed offset (10) but translated it to a wildly
imprecise downstream offset. The root cause is `offset.lag.max`
(default 100), the record-count threshold controlling how densely MM2's
replication task writes the upstream→downstream offset-sync pairs
checkpoint translation interpolates against. At real production volume,
sampling roughly every 100 records is a reasonable precision/overhead
trade-off; at this lab's scale (20 records total, near-zero replication
lag), it meant MM2 wrote essentially one early sync pair for the entire
topic and never revisited it, since lag never grew past the threshold that
would trigger another. Fixed by lowering `offset.lag.max = 1` in
[`TwoClusterEnvironment`](../../labs/lab-19-multi-cluster-dr/src/test/java/com/kafkalab/multicluster/support/TwoClusterEnvironment.java)'s
`mm2.properties` — a genuine lab-speed tuning knob, the same class as the
5-second interval overrides already used for `sync.topic.configs.interval.seconds`
and friends, and a real production lesson in its own right: **the default
checkpoint precision assumes production-scale throughput; low-volume topics
need `offset.lag.max` tuned down, or DR failover will silently reprocess far
more than necessary.**

None of these three findings were predictable from MM2's documentation
alone — each required running the real system, observing the real
(sometimes wrong-looking) result, and tracing it to a real, verifiable
mechanism, exactly the loop `CONTRIBUTING.md` describes.

## 4. Partition lifecycle engineering (conceptual)

Choosing an initial partition count is a one-time decision with permanent
consequences; changing it later is possible but not free:

- **Increasing partition count** never re-keys existing data. Every record
  already written keeps its original partition assignment; only *new*
  records hash against the new partition count (`DefaultPartitioner`'s
  `murmur2(key) % numPartitions`). This means a key that mapped to partition
  2 under the old count can map to a different partition under the new
  count — **ordering-per-key across the transition is not preserved**,
  which is exactly the partitioning trade-off `docs/partitioning/` (WP-04)
  already covers, now revisited at the "what happens when this changes
  later" layer.
- **Decreasing partition count is not supported by Kafka at all** — the only
  path is create-a-new-topic-with-fewer-partitions-and-migrate, which is a
  full re-publish, not a broker operation.
- **Consumer parallelism is capped by partition count**, not by consumer
  count — a group with more consumers than partitions leaves the excess
  idle. WP-17's capacity work already measures a per-partition throughput
  ceiling; partition count is the lever that scales total throughput against
  that per-partition ceiling, at the cost of more replica/leader-election
  overhead and (per this WP's own DR lesson, §3) coarser default checkpoint
  precision per additional partition MM2 must track.
- Replica placement (rack awareness, `broker.rack`) is also decided at
  partition-creation time, not continuously rebalanced — which is exactly
  the seam into §5.

## 5. Cluster rebalancing & Cruise Control (conceptual)

Two entirely different "rebalancing" problems share a name in casual Kafka
conversation:

- **Consumer-group rebalancing** (WP-05, `lab-04`) — redistributing
  *partition ownership among consumers* when group membership changes. This
  is a Kafka broker/client protocol feature, always active.
- **Cluster (replica) rebalancing** — redistributing *where partition
  replicas physically live* across brokers, e.g. after adding a broker.
  Kafka does **not** do this automatically (failure matrix, "Broker resource
  imbalance after adding/removing brokers" row): partition assignment is
  decided when a partition/topic is created or explicitly reassigned via
  `kafka-reassign-partitions.sh`, never continuously rebalanced by the
  broker itself.

Cruise Control (LinkedIn-originated, community-governed — ecosystem
tooling, not Apache Kafka itself, per `REFERENCE_REPOSITORIES.md`'s
Apache-vs-ecosystem boundary) exists specifically to close this gap: it
observes real per-broker resource metrics, computes a target replica
distribution against pluggable goals (CPU, disk, leader count, rack
awareness), and drives `kafka-reassign-partitions`-equivalent moves
automatically. This WP does not deploy Cruise Control as a second
control-plane product in a curriculum whose infrastructure is otherwise
entirely `apache/kafka` itself — the concept it automates (manual replica
reassignment, which every one of this repository's own single-node labs is
too small to meaningfully demonstrate) is the thing worth understanding
precisely, not operating a specific vendor tool for its own sake.

## 6. Platform governance: quotas (conceptual, one real config anchor)

At platform scale, one noisy tenant can starve every other tenant sharing a
cluster — Kafka has no cross-tenant fairness by default. Client quotas
(`quota.producer.default`, `quota.consumer.default`, or per-`client.id`/
per-user overrides under `StandardAuthorizer`-secured clusters, WP-18)
throttle a client's effective byte rate once it exceeds its allotment,
rather than rejecting requests outright — the broker delays the response,
applying backpressure the client's own retry/timeout configuration then has
to absorb. This is the same governance instinct WP-18's ACL work already
established for *access* (who can read/write what) extended to *rate*
(how much, how fast) — both are platform-team levers a single-tenant lab
cluster never needs to exercise for real, which is why this section stays
conceptual rather than adding a dedicated quota lab.

## 7. Cost engineering (extends WP-17's real `CapacityCalculator`)

WP-17 already produces a real, tested capacity workbook taking a *measured*
per-partition throughput ceiling as input. Multi-cluster cost engineering is
the same arithmetic, extended by two multipliers a single-cluster capacity
plan never needs:

```text
monthly storage cost  = event_rate × avg_event_size × retention_seconds
                         × replication_factor × storage_$/GB
cross-cluster cost     = event_rate × avg_event_size × mirrored_topic_count
                         × cross-region_transfer_$/GB
DR duplication cost    = monthly storage cost × (1 if active/passive with
                          shorter secondary retention, else ~1x again for
                          active/active's symmetric storage)
```

Every term must trace back to an explicit workload assumption — this
repository's own non-goal against "universal configuration formulas" (the
roadmap's Non-goals section) applies here identically: a cost figure without
its rate/size/retention assumptions stated is a guess, not an estimate.
Cross-region transfer pricing and active/active's doubled steady-state
storage are the two costs a single-cluster plan structurally cannot
surface — which is precisely why they belong in this WP, once a second
cluster is part of the picture at all.

## 8. Kafka on Kubernetes / Strimzi (conceptual only, by pre-existing scope decision)

The roadmap's own curriculum map (row 27) already marked this "Planned —
explicitly not introduced into the current implementation" before this WP
was ever built — a deliberate, stated boundary, not a cut made under
capstone time pressure. The conceptual question worth answering is not
"how do you run Kafka on Kubernetes" but **"should this organization run it
there"**: Strimzi (a CNCF-hosted operator, ecosystem tooling per the same
Apache-vs-ecosystem boundary as Cruise Control) manages Kafka's own
StatefulSet-unfriendly realities — persistent per-broker storage identity,
rolling restarts that respect ISR safety, and KRaft quorum membership — as
Kubernetes custom resources. The trade-off is operational: Kubernetes gives
elastic compute scheduling Kafka itself has no native concept of, at the
cost of an additional operator/CRD layer between an operator and the actual
broker process, exactly the kind of "does this org actually need the
elasticity" question the Principal Engineer decision lens (§10 below) is
built to force.

## 9. Lab limitations

- `lab-19` mirrors `primary -> secondary` only — active/active's
  bidirectional-mirroring correctness problem (§2) is described, not built,
  since demonstrating it safely needs topic-level exclusion rules this
  repository's other labs don't yet require.
- No real failover is performed against `killPrimary()` — the method exists
  (modeling WP-19's own real broker-kill mechanics) but this WP's two tests
  focus on replication and offset-translation mechanics, the prerequisites a
  real failover experiment would depend on; a future WP could extend this
  lab with an actual kill-and-resume-on-secondary test.
- Cruise Control, quotas, and Strimzi are conceptual only, per §5/§6/§8 —
  none is deployed as running infrastructure in this repository.
- No real cross-region network latency or cost figures are measured; §7's
  formula is presented with its assumptions explicit, per this repository's
  non-goal against invented performance/cost numbers.

## 10. Principal Engineer questions

1. **Why does `primary->secondary.enabled` alone not make a cluster pair
   DR-ready?** Because data replication and consumer-group *position*
   replication are separate mechanisms (§3.2) — a secondary cluster with
   all the right records but no translated offsets forces every failover
   consumer to choose between reprocessing everything or picking an
   arbitrary starting point.

2. **A consumer group's translated offset lands well short of the real
   committed offset on a low-volume topic. What's the actual cause, and
   what's the fix?** `offset.lag.max`'s default (100) is tuned for
   production-scale throughput; on a low-volume topic it produces too few
   offset-sync sample points to interpolate accurately (§3.2, Finding 3).
   Lower `offset.lag.max` for that mirror source connector — understanding
   this increases the offset-syncs topic's write volume, a real
   precision/overhead trade-off, not a free fix.

3. **When should this organization choose active/active over
   active/passive?** Only when the business genuinely needs both regions
   serving live write traffic simultaneously — every additional cost (§7),
   correctness risk (§2's same-topic bidirectional mirroring hazard), and
   operational complexity active/active adds over active/passive should be
   justified against that specific requirement, not adopted by default
   because it sounds more resilient.

4. **Why doesn't adding a broker to a cluster automatically rebalance
   load?** Kafka decides partition replica placement at creation/reassignment
   time only (§5); it has no continuous rebalancing loop. This is exactly
   why Cruise Control exists as separate tooling rather than a Kafka broker
   feature.

5. **What determines whether a Kafka platform needs client quotas?** Whether
   the cluster is genuinely multi-tenant with no cooperative rate
   discipline assumed — a single-team cluster with well-behaved producers
   may never need them; a shared platform serving unrelated teams almost
   always does, for the same reason WP-18's ACLs exist: trust boundaries
   that outgrow "everyone is well-behaved."

6. **Why does this repository not deploy Strimzi even in the capstone WP?**
   Because the roadmap decided that boundary explicitly, before this WP was
   scoped (§8) — and because the actual Principal Engineer question
   ("should we run Kafka on Kubernetes") is answerable through architectural
   reasoning about StatefulSet/ISR/quorum constraints without operating the
   operator, which is the more honest use of this repository's remaining
   scope than adding infrastructure for its own sake.

7. **What real, measurable evidence from this repository would you show a
   skeptical reviewer to defend a DR architecture choice, rather than just
   asserting it?** The `lab-19` test suite itself: real replication with a
   real renamed-topic convention, and a real, MM2-native offset translation
   landing within one record of the true committed position — plus the
   `offset.lag.max` finding as evidence the reviewer should ask "at what
   volume was this validated," since the same mechanism is imprecise at
   different scale.
