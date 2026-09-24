# Principal Engineer Kafka Interview Notes

Study notes for a Principal Engineer Kafka interview, built from this
repository's own real, hands-on 20-work-package Kafka curriculum — every
claim below traces to an actual lab, a real bug found and fixed, or a
documented architecture decision, not generic Kafka trivia.

## 1. Core architecture & fundamentals

Kafka is a distributed **log**, not a queue: a consumer reading a record
never deletes it; only a partition's retention policy (age, size, or
compaction) does. This is the single fact almost every other answer below
builds on.

| Concept | Precise answer |
|---|---|
| Partition | The real unit of ordering, storage, and parallelism. A topic is just a name for one or more partitions. |
| Offset | A record's position within ONE partition. Not globally unique — `(topic, partition, offset)` is the only complete address. |
| Ordering | Guaranteed within a partition only. There is no cross-partition ordering, ever. |
| Broker vs. controller | Broker = data plane (produce/fetch). Controller = control plane (KRaft metadata quorum). A node can hold both roles (combined mode) or just one (dedicated mode). |
| KRaft | Kafka's own Raft-based metadata consensus, replacing ZooKeeper entirely in Kafka 4.x. One controller quorum, `floor(n/2)+1` majority to keep making metadata decisions. |
| Consumer position vs. committed offset | Position = this consumer instance's in-memory "next offset to fetch," advances the instant `poll()` returns records. Committed offset = what the broker durably remembers for the whole group in `__consumer_offsets`. Only the committed offset survives a restart. |

**Interview-ready line:** "Kafka guarantees ordering per partition, not per
topic — if an interviewer asks for topic-wide ordering, the honest answer
is either use one partition (caps throughput and consumer parallelism at 1)
or key records so everything that must stay ordered lands on the same
partition."

Source: `labs/lab-01-first-kafka-cluster`, `labs/lab-02-native-java-producer-consumer`, `docs/architecture/KAFKA_MENTAL_MODEL.md`.

## 2. Delivery semantics, idempotence & transactions

| Pattern | What happens | Risk |
|---|---|---|
| Commit before processing (at-most-once) | Offset advances, then the app crashes before the side effect runs | Silent, permanent data loss — confirmed in `lab-05`: the record is gone, not delayed |
| Process before committing (at-least-once) | Side effect runs, then the app crashes before the offset commits | Exactly one record reprocessed on restart — the default, safest posture |
| Idempotent producer (`enable.idempotence=true`, default since a recent client version) | Broker tracks producer ID + sequence number per partition, drops a retried duplicate | Prevents duplicates from a lost-ACK retry — does NOT prevent business-level duplicates from a crash after commit |
| Kafka transactions | Atomic multi-partition writes + `read_committed` isolation | Guarantees no duplicate records from producer retries and no partially-visible transactional reads — **entirely about Kafka's own state** |

**The single most important PE-level distinction:** Kafka's exactly-once
semantics (idempotent producer + transactions + `read_committed`) never
extends to an external side effect — a database write, an email, a
charge. That is the **dual-write problem**, and it is solved by the
**idempotent-consumer pattern** (a `processed_events` table with a UNIQUE
constraint on `(group_id, event_id)`, `lab-12`) or the **transactional
outbox pattern** (writing the business row and an outbox row in ONE
database transaction, then using CDC via Debezium to publish it,
`lab-11`) — never by Kafka's own EOS.

A real, confirmed exception naming: a fenced-out producer under a shared
`transactional.id` fails its next operation with
`InvalidProducerEpochException`, not the commonly-assumed
`ProducerFencedException` — verified against the real class hierarchy in
`kafka-clients:4.3.1`, not assumed from tutorials (`lab-08`).

Source: `labs/lab-05-offset-management-delivery-semantics`, `labs/lab-08-transactions-exactly-once`, `labs/lab-11-transactional-outbox`, `labs/lab-12-retry-dlq-idempotency`.

## 3. Replication, ISR, KRaft quorum & failure recovery

**Replicas ≠ ISR.** Replicas is the FIXED list of brokers assigned to
hold a copy of a partition, decided at creation and unchanged when a
broker dies. ISR (in-sync replicas) is the SUBSET currently caught up
enough to be trusted for `acks=all`, and it changes continuously. Real,
measured evidence from `lab-06`: killing a leader broker elects a new one
from ISR within ~10 seconds; `Replicas` stays constant, `Isr` shrinks then
re-expands once the broker rejoins.

| Setting | What it actually controls |
|---|---|
| `acks=0/1/all` | Fire-and-forget / leader-only / full-ISR acknowledgment before the producer considers a write successful |
| `min.insync.replicas` | The minimum ISR size `acks=all` requires before ACCEPTING a write at all — enforced against ISR size, not replication factor |
| `unclean.leader.election.enable` | Whether a leader can be elected from OUTSIDE ISR (default false) — trades availability for possible data loss |

**A real structural finding** (`lab-06`): in a combined broker+controller
topology, crossing `min.insync.replicas` can ALSO break controller-quorum
majority, since the same nodes serve both roles — a coupling that doesn't
exist once controllers and brokers are on dedicated nodes.

**KRaft quorum specifics** (`lab-07`, dedicated-role cluster): a 3-voter
quorum tolerates exactly 1 controller failure (majority = 2). Ordinary
produce/consume traffic is COMPLETELY indifferent to a controller
failure, as long as no partition leader also changes. But "existing
traffic continues" during quorum loss does NOT mean the cluster can react
to a NEW failure — electing a new partition leader is itself a metadata
mutation, so a broker dying while quorum is unavailable produces a
sustained, non-self-healing outage for that partition until quorum
returns. This is the exact seam between replication (WP-07) and KRaft
quorum (WP-08) — a strong PE-level answer names this seam explicitly.

Source: `labs/lab-06-replication-isr-broker-failure`, `labs/lab-07-kraft-controller-quorum-failure`.

## 4. Partitioning, consumer groups & rebalancing

**Partition key strategy is an architecture decision, not a config
value.** Real, measured distributions from `lab-03`:

| Key strategy | Real measured result (max/avg ratio) |
|---|---|
| High cardinality (10k distinct keys) | 1.02 — close to even |
| Null key | 3.11 — sticky-batch (KIP-794), NOT round-robin; two partitions got 0 records |
| Low cardinality (3 values, 12 partitions) | 4.09 — 9 of 12 partitions sat completely empty |
| Hot key (one key = 90% of traffic) | 5.46 — one partition carried 91% of all traffic on an otherwise healthy cluster |

**Increasing partition count never re-keys existing data.** Real
evidence: after doubling a topic from 3 to 6 partitions, 14 of 30
identical keys mapped to a DIFFERENT partition for future records
(`murmur2(key) % N` changing), while already-written records never moved.
Decreasing partition count is not supported by Kafka at all.

**Consumer-group mechanics** (`lab-04`, real measured timings):
- Partition count is a hard ceiling on group parallelism — a 4th/5th
  consumer against a 3-partition topic gets a real, correctly-delivered
  EMPTY assignment.
- Graceful departure costs near-nothing; an abruptly-killed consumer's
  partitions aren't reassigned until `session.timeout.ms` elapses
  (measured: 9.7s and 10.9s against a 10s config).
- Under the eager `RangeAssignor` protocol (the one actually negotiated by
  default, confirmed via `protocol='range'` in real logs), EVERY member
  revokes its ENTIRE assignment on EVERY rebalance — not just the
  partition changing hands. This is why frequent rebalances hurt
  throughput even when each one resolves correctly.
- `session.timeout.ms` (heartbeat liveness) and `max.poll.interval.ms`
  (processing-loop liveness) are DIFFERENT mechanisms, checked by
  different threads — the background heartbeat thread can detect and act
  on a `max.poll.interval.ms` violation independently of whether the
  foreground processing loop has even returned.

Source: `labs/lab-03-partitioning-ordering`, `labs/lab-04-consumer-groups-rebalancing`.

## 5. Schema evolution, Kafka Connect/CDC & the outbox pattern

**Schema Registry is an ecosystem component, not part of the Kafka
broker protocol** — important to say explicitly in an interview, since
it's a Confluent-originated concept layered on top of Kafka.

Real, corrected finding (`lab-09`): the registry's convenient
`testCompatibilityVerbose()` check only ever compares a candidate against
the LATEST registered version, even under a `*_TRANSITIVE` compatibility
mode — giving CI a false PASS for a genuinely transitive-incompatible
schema unless the full version history is checked explicitly. Schema ID,
subject, and version are three independent axes; the same schema ID can
be shared across two different subjects.

**Kafka Connect** is part of Apache Kafka itself
(`connect-distributed.sh`); Debezium is a separately-governed CDC project
on top of it. Key facts: a source connector's own offset is whatever its
plugin defines (a byte file position for FileStream, a WAL LSN for
Debezium) — never a Kafka offset. Deleting a connector does NOT clear its
stored offsets. `REPLICA IDENTITY FULL` is what makes an UPDATE's CDC
event carry a full `before` image, not just the primary key.

**The dual-write problem, solved two ways:**
- **Idempotent consumer** (`lab-12`): a `processed_events` table with a
  UNIQUE constraint does the duplicate-detection atomically — the
  database constraint, not application logic, is what makes concurrent
  duplicate claims safe.
- **Transactional outbox** (`lab-11`): the business row and an outbox row
  commit in ONE database transaction; Debezium's `EventRouter` SMT
  publishes the outbox row via CDC. The application never imports
  `KafkaProducer` at all — publishing is entirely Debezium's job. A real
  gotcha: PostgreSQL's `jsonb` column re-serializes with a space after
  `:`/`,`, breaking naive exact-substring test assertions.

Source: `labs/lab-09-schema-evolution-governance`, `labs/lab-10-kafka-connect-cdc`, `labs/lab-11-transactional-outbox`, `labs/lab-12-retry-dlq-idempotency`.

## 6. Kafka Streams & Spring Kafka

**Kafka Streams** (`lab-13`): local state stores are rebuilt from a
CHANGELOG topic on task migration — restoration time is proportional to
changelog size, an availability cost, not a data-loss risk. A standby
replica reduces that cost by keeping a warm copy elsewhere; it does not
eliminate the migration. Real, confirmed evidence: killing one Streams
instance migrates its tasks and fully restores state on the survivor; a
standby replica measurably shrinks that restoration window.

**Spring Kafka** (`lab-14`, per `CONTRIBUTING.md` §18's rule — every
framework abstraction gets mapped to the native mechanism it wraps):
listener containers and acknowledgment modes map onto manual offset
commits; error handling with dead-letter publishing and non-blocking
retry topics map onto the same retry/DLQ mechanics `lab-12` built by
hand. Two real Spring Boot 4.x findings: Kafka autoconfiguration moved
into its own required artifact (`spring-boot-kafka`), and defining ANY
custom `KafkaTemplate` bean silently suppresses Spring Boot's own
autoconfigured default (matches by raw type, ignoring generics).

**Interview framing:** "I learn the native client before any framework,
specifically so I can evaluate whether a given Spring Kafka default
actually matches what the application needs, rather than trusting it
blindly."

Source: `labs/lab-13-kafka-streams`, `labs/lab-14-spring-kafka`.

## 7. Observability, performance & capacity engineering

**Consumer lag is not a broker-side JMX metric at all** — there is no
MBean for it. It has to be actively computed by comparing committed
offset against log-end offset, either natively via the Admin API or by a
separate tool (`kafka-exporter`). `lab-15` cross-checks both computations
against each other and confirms they match.

**Kafka has no built-in backpressure toward producers based on consumer
lag.** Production continues regardless of how far behind consumers fall,
up to retention limits — past that, unconsumed records are silently
deleted. This is the failure matrix's headline lag row, and its real,
measured consequence (data loss once lag exceeds retention) is
demonstrated, not asserted, in `lab-15`.

**Performance** (`lab-16`, real hand-built benchmark, never
`kafka-producer-perf-test.sh`): every throughput/latency claim compares
two real measurements taken moments apart on the SAME hardware against
EACH OTHER — never a fixed absolute number, since an absolute figure is
environment-dependent (and this repo's own rule forbids publishing
invented benchmark numbers). The capacity workbook (`CapacityCalculator`)
takes a MEASURED per-partition throughput ceiling as input, not an
assumed one.

**Interview-ready line:** "Partition count is a real ceiling on both
throughput and consumer parallelism — any capacity number I give has to
be tied to a measured per-partition ceiling and stated workload
assumptions, or it's a guess wearing a number's clothing."

Source: `labs/lab-15-observability`, `labs/lab-16-performance-capacity`.

## 8. Security

**`StandardAuthorizer`, not the legacy `AclAuthorizer`**, for any
KRaft-only cluster — `AclAuthorizer` stores ACLs in ZooKeeper, which
doesn't exist in a KRaft deployment. `StandardAuthorizer` stores ACLs in
the same Raft-replicated metadata log already securing topic/partition
metadata (`lab-17`).

**SASL/SCRAM-SHA-512 over SASL_SSL** — SCRAM's challenge-response design
means the broker never receives or stores the plaintext password, a
real, independent layer of protection beyond what TLS alone provides.

Three real findings worth citing directly:
- **TLS hostname verification checks the SAN (Subject Alternative Name)
  extension, not just the CN** — a cert with only `CN=security-broker`
  failed real handshakes from a client connecting via `localhost`, with
  `SSL handshake failed` as the only symptom.
- **`StandardAuthorizer` applies to EVERY listener, including the
  internal, PLAINTEXT-only CONTROLLER listener** — a broker's own
  self-registration with the controller runs as `User:ANONYMOUS`, and
  without `ANONYMOUS` in `super.users`, the broker's own internal
  bootstrap traffic is denied by the authorizer meant to protect client
  traffic.
- **ACL checks are ordered** — group authorization is checked before
  topic authorization, so a write-only ACL alone lets a client produce
  but not consume, even with no explicit group-ACL denial configured.
- Kafka's own authorization model hides resource existence from
  unauthorized principals: an unauthorized client asking to describe a
  real, existing topic gets `UNKNOWN_TOPIC_OR_PARTITION`, not an explicit
  access-denied error — a deliberate anti-information-leakage design, not
  a bug.

Source: `labs/lab-17-security`.

## 9. Multi-cluster, DR & platform engineering

**MirrorMaker 2 replicates data; it does NOT by itself make DR failover
cheap.** A consumer group's committed offset on the primary cluster needs
translating to the secondary cluster's equivalent offset — Kafka's own
client API for this is `RemoteClusterUtils.translateOffsets`, which reads
MM2's checkpoint records.

Three real, sequential findings from building a real two-cluster + MM2
environment (`lab-19`):
- MM2 emits checkpoints continuously on its own interval — the FIRST
  observed translated offset can be stale; poll until it reaches the
  expected value, not just non-null.
- `KafkaConsumer`'s default `enable.auto.commit=true` means `close()`
  performs its own final auto-commit that can silently overwrite an
  already-correct explicit commit.
- **`offset.lag.max` (default 100)** governs how densely MM2 samples
  offset-sync pairs for checkpoint translation — at LOW volume, the
  default leaves too few sample points, making DR failover offset
  translation wildly imprecise (confirmed: a committed offset of 10
  translated to 1). Fixed by lowering the value for low-throughput
  topics.

**Active/active vs. active/passive is a requirements question, never a
default.** Active/active adds a real correctness hazard active/passive
never has: without topic exclusion, a record mirrored back to its origin
cluster can loop. Choose active/active only when the business genuinely
needs simultaneous dual-region WRITES, not just dual-region
survivability.

**Cluster (replica) rebalancing ≠ consumer-group rebalancing.** Kafka
does NOT automatically move existing partition replicas onto new brokers
when you scale the cluster — partition placement is decided at
creation/reassignment time only. Cruise Control (ecosystem tooling, not
Apache Kafka itself) exists specifically to close this gap.

**Partition lifecycle:** increasing partition count changes where FUTURE
records for a key land; it never moves existing data, and decreasing
partition count isn't supported at all — so partition count is
capacity-planned state, not a free-to-flip autoscaling knob.

Source: `labs/lab-19-multi-cluster-dr`, `docs/multi-cluster/KAFKA_MULTI_CLUSTER_AND_DR.md`, `adrs/0005-active-passive-dr-default-not-active-active.md`.

## 10. The Principal Engineer decision lens

A generic checklist to walk through OUT LOUD against a specific decision
(partition count, DR architecture, security model) — never answered in
the abstract, and never recited as a memorized list:

```text
What problem are we solving?            What happens during broker failure?
What are the workload characteristics?  What happens during consumer failure?
What are the SLOs?                      How do we observe failure?
What can fail?                          How do we recover?
What data loss is acceptable?           How do we test recovery?
What duplicate processing acceptable?   What does it cost?
What ordering is required?              What security boundary exists?
What recovery time is required?         What alternatives exist?
What scale do we expect?                What would make us change this decision?
What happens at 10x?                    What happens during deployment?
```

**How to use it live in an interview:** pick ONE of the decisions below
and walk through 5-6 of the most load-bearing questions for THAT
decision, out loud, stating the trade-off you're accepting and why:

1. Active/passive vs. active/active DR — driven by whether the business
   needs simultaneous dual-region writes, not by "resilience" alone.
2. `orderId` vs. `customerId` vs. `merchantId` as a partition key —
   driven by what must actually stay ordered together and each key's REAL
   (not assumed) traffic distribution.
3. `StandardAuthorizer` + SASL/SCRAM vs. mutual TLS — driven by which
   credential model the organization already operates, not which is
   abstractly "more secure."
4. Testcontainers-backed integration tests vs. mocked brokers — driven by
   whether the team needs evidence about ACTUAL Kafka behavior or just
   confirmation the code calls the client API correctly.

**The tell of a strong PE-level answer:** it names the alternative NOT
chosen and the specific condition that would flip the decision ("if the
org later needs true dual-region writes, that changes the DR topology
entirely") — never a single "best practice" asserted without a condition
attached.

Source: `docs/roadmap/KAFKA_ZERO_TO_PRINCIPAL_ENGINEER.md` (Principal Engineer decision lens), `docs/principal-engineer/KAFKA_ARCHITECTURE_DECISION_FRAMEWORK.md`.

## 11. Real findings worth citing as evidence

A Principal Engineer interview rewards concrete debugging stories over
textbook answers. These are real, root-caused, fixed — each one is a
ready-made "tell me about a time you debugged something hard" answer.

| Symptom | Real root cause | Fix |
|---|---|---|
| A 3-voter KRaft quorum deadlocks on startup | Starting 3 broker containers SEQUENTIALLY means each blocks waiting for a quorum the others haven't started forming yet | Start all voters concurrently |
| DR offset translation stuck at a value far below the real commit | `offset.lag.max` default (100) too coarse for a low-volume topic's offset-sync sampling | Lower `offset.lag.max` for that topic's actual throughput |
| A producer's `min.insync.replicas` test hung instead of failing fast | The idempotent producer's `InitProducerId` handshake blocks on `max.block.ms` if the broker it needs is unreachable | Test with `enable.idempotence=false` for that specific experiment |
| CI passed a schema that was actually transitively incompatible | `testCompatibilityVerbose()` only checks against the LATEST version, even under `*_TRANSITIVE` mode | Check the full version history explicitly in the compatibility gate |
| Two Kubernetes pods of the same Schema-Registry-like app collided on identity | Deployment's default `RollingUpdate` strategy runs old and new pods side by side; both advertised the identical hardcoded hostname | `strategy: Recreate` |
| A KRaft quorum's own peers couldn't reach each other to bootstrap, in Kubernetes | Kubernetes Services gate endpoint inclusion on readiness by default — a circular dependency for quorum bootstrap Docker Compose never has | `publishNotReadyAddresses: true` on the Service |

**Interview framing for any of these:** state the SYMPTOM first, then the
hypothesis you tested and ruled out, then the confirmed root cause, then
the fix and why it's correct — not a fix that happened to make the
symptom go away.

## 12. Practice question bank

Answer each out loud, from evidence (a lab, a measured result, a real
fix) — not from memory of a definition.

**Warm-up (fundamentals):**

1. Why is Kafka a distributed log rather than a message queue?
2. Why can a single `poll()` return more records than you intended to
   process before your next commit?
3. What is the dual-write problem, and what pattern addresses it without
   distributed transactions?
4. With `acks=all` and `min.insync.replicas=2` on a 3-replica topic, a
   broker dies mid-write — was the last acknowledged write necessarily
   durable?

**Principal Engineer level:**

5. A stakeholder asks for "active/active for resilience." What
   follow-up questions determine whether that's the right choice?
6. Why doesn't replicating data between two clusters with MirrorMaker 2
   alone make DR failover cheap?
7. Why doesn't adding a broker to a cluster automatically rebalance
   partition placement?
8. When does a shared Kafka platform need client quotas, and what's the
   actual enforcement mechanism?
9. A partition count decision made at topic creation turns out too low a
   year later — what are the real options, and what does each one cost?
10. Why choose `StandardAuthorizer` over the legacy `AclAuthorizer` for a
    KRaft-only cluster, and what would have to be true for that decision
    to be wrong?
11. Why does this curriculum insist on real integration tests over
    mocked brokers, and what specific bugs would a mock have hidden?
12. Trace `enable.auto.commit`'s final commit-on-close to the actual
    client-internals code path — what real bug can it cause?

**System design (pick one, walk the PE lens live):**

13. Design a multi-region order-processing platform where per-order
    ordering matters but a regional outage must not lose accepted
    orders.
14. Design a real-time fraud-scoring platform at 10x the volume of (13),
    where a slightly-stale score is worse than a delayed one.

Full worked answers, each traced to real lab evidence:
[`interview/fundamentals/QUESTIONS.md`](../../interview/fundamentals/QUESTIONS.md),
[`interview/principal/QUESTIONS.md`](../../interview/principal/QUESTIONS.md),
[`system-design/01-multi-region-order-platform.md`](../../system-design/01-multi-region-order-platform.md),
[`system-design/02-real-time-fraud-detection-platform.md`](../../system-design/02-real-time-fraud-detection-platform.md).
