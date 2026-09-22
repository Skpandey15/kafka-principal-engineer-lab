# Kafka: Zero to Principal Engineer — Curriculum Roadmap

This document is the map of the entire repository. It describes the full curriculum,
the milestones an engineer progresses through, and the work packages (WPs) used to
build the repository incrementally. If you only read one document before starting,
read this one and [`KAFKA_MENTAL_MODEL.md`](../architecture/KAFKA_MENTAL_MODEL.md).

Two companion documents extend this one and are referenced throughout:
[`docs/references/REFERENCE_REPOSITORIES.md`](../references/REFERENCE_REPOSITORIES.md)
(the external repositories this curriculum checks itself against, and exactly
what each one is and isn't authoritative for) and
[`PRINCIPAL_ENGINEER_FAILURE_MATRIX.md`](PRINCIPAL_ENGINEER_FAILURE_MATRIX.md)
(the long-term map of every failure this repository deliberately injects, across
every topic area, not just the ones already built).

## How this repository teaches

Every non-trivial Kafka topic in this repository is taught through the same loop.
Skipping steps produces developers who can call an API; completing the loop produces
engineers who can operate, troubleshoot, and design with Kafka.

```text
Concept
   ↓
Architecture
   ↓
Internal mechanism
   ↓
Java implementation
   ↓
Run it
   ↓
Observe it
   ↓
Break it intentionally
   ↓
Understand the failure
   ↓
Troubleshoot it
   ↓
Fix it
   ↓
Measure it
   ↓
Production considerations
   ↓
Architecture trade-offs
   ↓
ADR / decision
   ↓
Principal Engineer questions
```

Kafka is taught here as a **distributed system with a Java client**, not as a Java
client that happens to talk to Kafka. Every lab that introduces a configuration or
API explains the distributed-systems problem it solves before showing the code.

This repository is also explicit about a boundary that's easy to blur: **Apache
Kafka** (the broker, the protocol, KRaft, the Java client, Kafka Connect's
framework, Kafka Streams) is not the same thing as the **ecosystem** around it
(Confluent's tooling, community operators, community UIs, community-governed
projects like Debezium and Cruise Control). See
[`docs/references/REFERENCE_REPOSITORIES.md`](../references/REFERENCE_REPOSITORIES.md#apache-kafka-vs-the-ecosystem-around-it--read-this-before-the-matrix)
for that distinction spelled out — it matters every time this curriculum reaches
for a tool and needs to be precise about whose guarantee it's actually relying on.

## Milestones

Milestones are checkpoints, not chapters. Each one has an explicit assessment before
moving on. A milestone is "reached" when you can answer its assessment without
looking anything up.

| Milestone | Title | You can... | Assessed by |
|---|---|---|---|
| M0 | Foundation | Explain Kafka's architecture: brokers, topics, partitions, replication, KRaft, and why Kafka exists relative to queues/DBs. | `docs/principal-engineer/` M0 checkpoint + `interview/fundamentals/` (both added once the fundamentals content lands) |
| M1 | Developer | Implement reliable Java producers and consumers, reason about serialization, and read consumer group state. | `lab-02` through `lab-04` |
| M2 | Senior Engineer | Design topics, partition keys, retry/DLQ strategy, and schema evolution policy; explain delivery semantics precisely, including exactly what Kafka's own exactly-once semantics do and do not guarantee about your business logic. | `lab-05`, `lab-08` through `lab-13`, `lab-16`, `lab-19` |
| M3 | Staff Engineer | Operate a cluster under failure, benchmark it, evolve schemas safely, and diagnose incidents from metrics and logs alone. | `lab-20` through `lab-24`, `docs/troubleshooting/`, [`PRINCIPAL_ENGINEER_FAILURE_MATRIX.md`](PRINCIPAL_ENGINEER_FAILURE_MATRIX.md) |
| M4 | Principal Engineer | Design organization-scale event platforms and defend architecture decisions with trade-offs, not opinions — including partition lifecycle, cluster rebalancing, multi-cluster/DR, platform governance, and cost. | `docs/principal-engineer/`, `adrs/`, `system-design/`, the Level 5/6 topics below |
| M5 | Kafka Deep Dive | Reason about Kafka internals, failure modes, capacity, multi-cluster/multi-region architecture, and source-level behavior. | `docs/principal-engineer/KAFKA_SOURCE_CODE_READING_GUIDE.md`, `interview/principal/` |

Each milestone's formal assessment content is added in the work package that
completes the material it depends on — see the WP plan below. This avoids assessing
knowledge the repository hasn't taught yet.

## Curriculum levels — a second lens on the same work packages

The curriculum map and work-package plan below are organized by topic and by
build sequence. This section adds a third, coarser lens — useful for seeing
where you stand at a glance — without renumbering anything: every level maps
onto specific, already-numbered WPs and lab-numbered labs. Treat these as
descriptive groupings, not rigid certification boundaries; dependencies
between topics matter more than which level label something carries.

```text
LEVEL 0 — Distributed-systems foundation
  Distributed systems, distributed logs, the Kafka mental model
  → WP-01, WP-02

LEVEL 1 — Kafka core
  Broker, topic, partition, offset, producer, consumer, consumer groups
  → WP-02, WP-03

LEVEL 2 — Kafka internals
  Partitioning, replication, ISR, leader election, KRaft, storage,
  retention, compaction, delivery semantics, transactions
  → WP-04 through WP-09

LEVEL 3 — Application & data integration
  Serialization, Avro/Protobuf, Schema Registry, Kafka Connect, CDC,
  Debezium, transactional outbox, idempotent consumer, Kafka Streams,
  Spring Kafka
  → WP-10 through WP-15

LEVEL 4 — Production engineering
  Client resilience, retry/DLQ, observability, performance, security,
  failure engineering, troubleshooting, capacity planning
  → WP-13 (retry/DLQ), WP-16 through WP-19, plus the client-resilience
    topic threaded through WP-03, WP-06, and WP-09 rather than owning a
    single dedicated WP

LEVEL 5 — Fleet / platform engineering
  Partition lifecycle, replica reassignment, cluster balancing, Cruise
  Control, multi-cluster, disaster recovery, RPO/RTO, Kafka on Kubernetes,
  Strimzi
  → folded entirely into WP-20, the Principal Engineer capstone (see the
    consolidation note under the work package plan) — no longer a
    separate WP range under the 20-WP consolidated plan

LEVEL 6 — Principal Engineer
  Platform governance, cost engineering, system design, ADRs, architecture
  reviews, source-code reading, migration strategy, upgrade strategy, SLO
  design, Principal Engineer failure reviews
  → also folded entirely into WP-20
```

Level 5 and 6 are no longer just related in practice — under the 20-WP
consolidated plan they are literally the same work package. Governance and
cost engineering (Level 6) are meaningless without the fleet-operations
concepts (Level 5) they're applied to, and both ultimately feed the same
ADRs and system designs, which is exactly why consolidating them into one
capstone work package (WP-20) reflects how this material is actually built
and used, not an arbitrary numbering convenience.

## Curriculum map

The curriculum is organized by topic area, not strictly by milestone, because real
Kafka expertise is built by revisiting the same concepts (partitioning, failure,
delivery semantics) at increasing depth. Each row below will become one or more
`docs/<area>/` documents and one or more `labs/lab-NN-*` directories. Rows without
an assigned lab number yet are **planned topics with a deliberate place in the
curriculum, not implemented material** — see the Work package plan for what each
one's future WP is expected to cover.

| # | Area | Core questions it answers | Primary docs | Primary labs / WP |
|---|---|---|---|---|
| 0 | Distributed systems foundation | Why does Kafka exist? Queue vs. log? What does "durable" actually mean? | `docs/fundamentals/` | — |
| 1 | Kafka fundamentals | What is a broker, partition, offset, ISR, controller? | `docs/fundamentals/` | `lab-01`, `lab-02` |
| 2 | Producer internals | What happens inside `producer.send()`? | `docs/producer/` | `lab-02`, `lab-08` |
| 3 | Consumer internals | How does polling, committing, and rebalancing actually work? | `docs/consumer/`, `docs/consumer-groups/` | `lab-02`, `lab-04` |
| 4 | Partitioning | Why do partition keys matter more than almost any other decision? | `docs/partitioning/` | `lab-03` |
| 5 | Replication & failure | How does Kafka survive broker loss without losing data? | `docs/replication/` | `lab-06` (WP-07) |
| 6 | KRaft | How does the cluster agree on metadata without ZooKeeper? | `docs/kraft/` | `lab-07` (WP-08) |
| 7 | Storage internals | Why is Kafka fast? What is a segment, index, and compaction? | `docs/storage/` | `lab-01` (inspection) |
| 8 | Delivery semantics | What does "exactly-once" really mean, and when does it lie? | `docs/delivery-semantics/`, `docs/transactions/` | `lab-05` (offset-commit-driven at-most-once/at-least-once fundamentals, plus an introductory idempotent-consumer experiment, WP-06); `lab-08` (Kafka-native idempotent producers, transactions, and EOS, built in WP-09) |
| 9 | Serialization & schema governance | How do you evolve a schema without breaking consumers? (Note: Schema Registry itself is a Confluent-ecosystem concept layered on top of Kafka, not a Kafka broker feature — see the reference-repositories boundary note above.) | `docs/schema-evolution/` | `lab-09` (WP-10) |
| 10 | Kafka Connect | How do you move data in/out of Kafka without hand-written glue? | `docs/kafka-connect/` | `lab-10` (WP-11) |
| 11 | CDC + Debezium | How do database changes become an event stream? | `docs/kafka-connect/` | `lab-10` (WP-11) |
| 12 | Transactional outbox | How do you avoid the dual-write problem? | `docs/outbox/` | `lab-11` (WP-12) |
| 13 | Idempotent consumer & business exactly-once | Kafka delivery guarantees ≠ business exactly-once processing — where is *your* idempotency boundary? | `docs/patterns/` | Introduced early, at the level of a single `eventId` check against a durable store, in `lab-05` (WP-06); conceptually anchored directly after outbox (row 12) for its full production treatment, with the hands-on lab delivered in `lab-19` alongside retry/DLQ (WP-13) |
| 14 | Kafka Streams | How do you build stateful stream processing on top of Kafka? | `docs/kafka-streams/` | `lab-17` |
| 15 | Spring Kafka | How does a production framework map onto the primitives you already know? | `docs/spring-kafka/` | `lab-18` |
| 16 | Event-driven patterns (broader taxonomy) | Which pattern fits which problem, and when should you avoid Kafka entirely? | `docs/patterns/` | `lab-16`, `lab-19` |
| 17 | Client resilience engineering | What actually prevents (or fails to prevent) duplicates and lost work when a producer's ACK is lost, or a consumer's processing runs long? | `docs/producer/`, `docs/consumer/` (revisited through a failure lens) | Threaded through `lab-02` through `lab-09`; see [`PRINCIPAL_ENGINEER_FAILURE_MATRIX.md`](PRINCIPAL_ENGINEER_FAILURE_MATRIX.md) for the specific failure scenarios this topic is structured around |
| 18 | Observability | What do you monitor, and what does each signal mean? | `docs/observability/` | `lab-20` |
| 19 | Performance engineering | What actually limits throughput and latency? | `docs/performance/` | `lab-21` |
| 20 | Capacity planning | How do you size a cluster from a workload description? | `docs/performance/` | `lab-24` |
| 21 | Security | How do you secure a cluster without breaking it? | `docs/security/` | `lab-22` |
| 22 | Failure engineering | What actually happens when you kill a broker, a consumer, or the network? | `docs/failure-recovery/` | `lab-09`, `lab-23`; indexed across every topic area in [`PRINCIPAL_ENGINEER_FAILURE_MATRIX.md`](PRINCIPAL_ENGINEER_FAILURE_MATRIX.md) |
| 23 | Troubleshooting | Given symptoms and metrics, what is the diagnostic sequence? | `docs/troubleshooting/` | all failure labs |
| 24 | Partition lifecycle engineering | How do you choose an initial partition count, and what actually happens — to ordering, to consumer concurrency, to replica placement — when you change it later? | `docs/partitioning/` (deep-dive) | Folded into WP-20 (Principal Engineer capstone); Planned |
| 25 | Cluster rebalancing & Cruise Control | Consumer-group rebalancing and cluster data/replica rebalancing are different problems — why doesn't adding a broker automatically balance a cluster? | `docs/replication/` or a future `docs/cluster-operations/` | Folded into WP-20; Planned |
| 26 | Multi-cluster & disaster recovery | Active/passive vs. active/active, RPO/RTO, producer/consumer failover — architecture first, product choice second | A future `docs/multi-cluster/` | Folded into WP-20; Planned |
| 27 | Kafka on Kubernetes / Strimzi | Just because Kafka *can* run on Kubernetes, should this organization run it there? | A future `docs/kubernetes/` | Folded into WP-20; Planned — explicitly not introduced into the current implementation |
| 28 | Platform governance | Topic/schema/security governance, quotas, SLOs — Kafka as an enterprise platform, not just a broker | A future `docs/governance/` | Folded into WP-20; Planned |
| 29 | Cost engineering | Event rate × size × retention × replication, plus network/cross-AZ/DR duplication — with assumptions always explicit | `docs/performance/` (extended) | Folded into WP-20; Planned |
| 30 | System design | How do you design a real platform end to end, forced through partition key, replication, semantics, schema, retention, retry/DLQ, ordering, scaling, DR, security, and cost decisions together? | `system-design/` | `system-design/` (WP-20) |
| 31 | Principal Engineer decisions | Why this choice and not that one? | `docs/principal-engineer/`, `adrs/` | all — see the Principal Engineer decision lens below |
| 32 | Interview preparation | Can you reason under pressure, out loud, from first principles? | `interview/` | — |
| 33 | Source-code reading | Where in `apache/kafka` does this behavior actually live? | `docs/principal-engineer/KAFKA_SOURCE_CODE_READING_GUIDE.md` | — |

This table's row numbers are positions in this list, not stable identifiers
the way `WP-NN` and `lab-NN` are — nothing elsewhere in this repository
cites "curriculum map row 12," so inserting new rows here doesn't break any
external reference. The topics newly added in this document (work package
"WP-02A," the curriculum enhancement this section belongs to) are:
idempotent consumer & business exactly-once (row 13), client resilience
engineering (row 17), partition lifecycle engineering, cluster rebalancing
& Cruise Control, multi-cluster & disaster recovery, Kafka on Kubernetes /
Strimzi, platform governance, and cost engineering (rows 24–29). Every
other row's content carries over unchanged from before WP-02A, aside from
a clarifying note added to the schema-governance row (9) about the
Confluent/Apache boundary. None of the new rows are implemented yet; they
exist here specifically so they have a deliberate, named place before
implementation reaches them, rather than being invented ad hoc later or
never scoped at all.

## Principal Engineer decision lens

This is the question set every major topic in Levels 4–6 should eventually
be answerable against, and the set every ADR in `adrs/` should trace back
to. It is not a form to fill in mechanically — it's the shape of the
reasoning a Principal Engineer is expected to walk through, out loud, before
committing to an architecture decision.

```text
What problem are we solving?
What are the workload characteristics?
What are the SLOs?
What can fail?
What data loss is acceptable?
What duplicate processing is acceptable?
What ordering is required?
What recovery time is required?
What scale do we expect?
What happens at 10x?
What happens during deployment?
What happens during broker failure?
What happens during consumer failure?
How do we observe failure?
How do we recover?
How do we test recovery?
What does it cost?
What security boundary exists?
What alternatives exist?
What would make us change this decision?
```

This lens is deliberately generic — it is meant to be applied to a specific
topic (a partition-count choice, a DR architecture, a security model), not
answered in the abstract. `docs/principal-engineer/KAFKA_ARCHITECTURE_DECISION_FRAMEWORK.md`
(planned for WP-20, the Principal Engineer capstone) will apply it
explicitly to the named decision points listed under WP-20 above; until
then, use it directly when working through an ADR or a system-design
exercise.

## Source-code reading track

A parallel track, not a separate phase — it runs alongside whichever topic
you're on, and its exercises only make sense once a lab has already shown
you the *observed* behavior you're trying to trace to source. The subsystems
below are named by role, deliberately, rather than by exact class name:
Kafka's internal package layout changes between releases, and hard-coding a
class name here would go stale exactly the way parts of
`confluentinc/kafka-streams-examples`'s infrastructure examples already have
(see [`REFERENCE_REPOSITORIES.md`](../references/REFERENCE_REPOSITORIES.md)).
Before treating any specific class or package name as current, check it
against the `apache/kafka` tag matching the version this repository has
pinned.

Subsystems worth eventually reading, by role:

- The producer's batching and send path (the accumulator and sender
  components introduced conceptually in
  [`KAFKA_MENTAL_MODEL.md`](../architecture/KAFKA_MENTAL_MODEL.md)).
- The client's network layer (how a client discovers and talks to brokers).
- Cluster metadata handling, client-side (how a producer or consumer reacts
  to a leadership change).
- The consumer's internals (the fetch path and the group-coordination
  client logic).
- The broker's group-coordinator implementation (how rebalancing decisions
  are actually made and communicated).
- The log/storage subsystem (segments, indexes, retention, compaction).
- Replica management (how followers fetch from leaders, how the ISR is
  tracked).
- The controller / KRaft implementation (the metadata Raft log itself).
- Transaction coordination (how idempotence and transactions are tracked
  server-side).

Every source-reading exercise, once built, should follow the same shape:

```text
Question
   ↓
Relevant subsystem
   ↓
Locate the implementation (verified against the current source tree)
   ↓
Trace the call path
   ↓
Relate it to behavior you actually observed in a lab
   ↓
Write down what you found
```

The target outcome: given a behavior you observed in any lab in this
repository, you can say specifically where in Kafka's own source code that
behavior is implemented — not "somewhere in the producer," but the actual
subsystem and, ideally, the actual method.

## Work package plan

The repository is built incrementally, as exactly **20 work packages**. Each
work package (WP) is a reviewable unit of work — never a single giant commit
— and each is scoped to represent a meaningful, Principal-Engineer-level
engineering capability rather than a narrow feature slice. This list is the
current plan and will be refined as earlier WPs surface new information; it
is not a fixed contract.

| WP | Scope | Status |
|---|---|---|
| WP-01 | Kafka mental model & fundamentals: repository foundation (README, this roadmap, reference repositories, contributing guide, `.gitignore`, license), the Kafka mental model, and core concepts — brokers, topics, partitions, records, offsets, producers, consumers. | Done |
| WP-02 | KRaft & local Kafka cluster: KRaft architecture, controllers, brokers, Docker Compose cluster startup, topic/partition/offset inspection, CLI operations. (`lab-01-first-kafka-cluster`) | Done |
| WP-03 | Java producer & consumer: native Kafka Java client (no Spring), producer/consumer lifecycle, serialization, polling. (`lab-02-native-java-producer-consumer`) | Done |
| WP-04 | Partitioning & ordering: keys, partition assignment, ordering scope, partition skew, hot partitions, the parallelism ceiling. (`lab-03-partitioning-ordering`) | Done |
| WP-05 | Consumer groups & rebalancing: partition assignment, consumer membership, rebalancing, cooperative/sticky behavior, static membership. (`lab-04-consumer-groups-rebalancing`) | Done |
| WP-06 | Offset management & delivery semantics: consumer position vs. committed offset, `commitSync`/`commitAsync`, auto-commit vs. manual commit, at-most-once, at-least-once, an introductory idempotent-consumer check, batch-commit boundaries, per-partition offset tracking, and the rebalance/commit-strategy interaction. Deterministic crash injection throughout, never a random process kill. (`lab-05-offset-management-delivery-semantics`) | Done |
| WP-07 | Replication, ISR & broker failure: replication factor, leaders/followers, ISR, leader election, `acks`, `min.insync.replicas`, broker failure, broker recovery. Real 3-node KRaft cluster (`platform/kafka-cluster/`). (`lab-06-replication-isr-broker-failure`) | Done |
| WP-08 | KRaft controller quorum & failure: dedicated-role controller quorum, metadata quorum inspection, controller leader failure/election, quorum-loss behavior, control-plane-vs-data-plane traffic continuity, and the connection back to WP-07 (a partition-leader failure during quorum loss). Real dedicated controller/broker cluster (`platform/kraft-quorum/`). (`lab-07-kraft-controller-quorum-failure`) | Done |
| WP-09 | Transactions & exactly-once semantics: producer idempotence (PID/epoch/sequence), Kafka transactions, `transactional.id`, commit/abort, `read_committed`/`read_uncommitted`, atomic multi-partition writes, producer fencing, atomic consume-transform-produce, and precisely what Kafka's own EOS does and does not guarantee versus true end-to-end business exactly-once (the dual-write problem, the distinction WP-06 already establishes for offset commits alone). Reuses the WP-07 3-broker cluster. (`lab-08-transactions-exactly-once`) | Done |
| WP-10 | Schema evolution & governance: serialization, Avro/Protobuf/JSON Schema, Confluent Schema Registry, schema ID/subject/version, naming strategies, compatibility modes (including transitive), reader/writer schema resolution, a CI compatibility gate, and schema governance (ownership, migration strategies, replay/consumer-lag implications). Reuses the WP-07 cluster plus a new Schema Registry container (`platform/schema-registry/`). (`lab-09-schema-evolution-governance`) | Done |
| WP-11 | Kafka Connect & CDC: Kafka Connect architecture (worker/task/offset model), source/sink connectors (the FileStream connectors that ship inside Apache Kafka itself), change data capture with Debezium's PostgreSQL connector (envelope format, snapshot vs. streaming, `REPLICA IDENTITY FULL`), and real failure behavior (connector task retry, source-database-unavailable resume from WAL position). Connect fundamentals sequenced before Debezium-specific CDC within this same work package. Reuses the WP-07 cluster plus a new PostgreSQL + Connect worker environment (`platform/kafka-connect/`). (`lab-10-kafka-connect-cdc`) | Done |
| WP-12 | Transactional outbox: the dual-write problem, the outbox pattern, CDC-based publishing via Debezium's `EventRouter` single message transform (built directly on WP-11's CDC pipeline), and the reliability guarantees the pattern does and does not deliver. Reuses `platform/kafka-connect/` unchanged, plus two new tables. (`lab-11-transactional-outbox`) | Current |
| WP-13 | Retry, DLQ & idempotency: retry strategies, poison messages, dead-letter queues, backoff, and the idempotent-consumer/deduplication topic (curriculum-map row 13) in full production depth — idempotency keys, processed-event tables, unique constraints, and the crash scenarios that make them necessary, building on WP-06's introductory version. (`lab-19-retry-dlq`) | Next |
| WP-14 | Kafka Streams: stream processing, state stores, joins, windows, `TopologyTestDriver`, exactly-once processing. (`lab-17-kafka-streams`) | Planned |
| WP-15 | Spring Kafka: listener containers, producer/consumer configuration, error handling, retries, transactions, in production style. (`lab-18-spring-kafka`) | Planned |
| WP-16 | Kafka observability & troubleshooting: consumer lag, partition throughput, broker metrics, JMX → Prometheus → Grafana, production dashboards, hot-partition and lag diagnosis, the operational diagnostic sequence. (`lab-20-observability`) | Planned |
| WP-17 | Performance & capacity engineering: throughput, latency, batching, compression, producer/consumer tuning, partition sizing, and capacity planning from a workload description — a benchmarking harness plus a worked capacity workbook, so tuning and sizing are taught together rather than as separate tracks. (`lab-21-performance`) | Planned |
| WP-18 | Kafka security: TLS, SASL/SCRAM, ACLs, authentication, authorization, secrets, production security architecture (local-development-only shortcuts clearly marked). (`lab-22-security`) | Planned |
| WP-19 | Failure engineering & production simulation: broker failures, consumer failures, network issues, rebalances, lag spikes, partition skew, disk pressure, recovery experiments, and a production-simulation lab combining prior labs into one running system — implements the applicable rows of [`PRINCIPAL_ENGINEER_FAILURE_MATRIX.md`](PRINCIPAL_ENGINEER_FAILURE_MATRIX.md). (`lab-23-failure-injection`, `lab-25-production-simulation`) | Planned |
| WP-20 | Multi-cluster, DR & Principal Engineer capstone: multi-cluster architecture, MirrorMaker 2, disaster recovery, RPO/RTO, active-active vs. active-passive, partition lifecycle engineering, cluster rebalancing and Cruise Control, platform governance, cost engineering, Kafka on Kubernetes/Strimzi, system design labs, the ADR set, the Principal Engineer architecture-decision framework, the source-code reading guide, interview question banks, and milestone assessments — architecture decisions and trade-offs across capacity, security, and reliability, culminating in final production-grade system design. This is the consolidation of every previously-separate Level 5/6 capstone topic into one substantial, final work package. (`system-design/`, `adrs/`) | Planned |

WPs are not strictly sequential milestones — several can proceed once their
prerequisites exist — but the order above reflects genuine dependencies: for
example, transactions (WP-09) before the outbox pattern (WP-12), and Kafka
Connect fundamentals before Debezium-specific CDC within WP-11, since this
repository's outbox lab is built directly on top of the CDC pipeline rather
than as a standalone pattern. The idempotent-consumer topic (curriculum-map
row 13) follows the same conceptual chain — introduced early, in WP-06, at
the level of a single durable `eventId` check, then given its full
production treatment in WP-13 alongside retry/DLQ, because a realistic
treatment benefits from the retry/DLQ mechanics WP-13 also covers.

**Consolidation note: exactly 20 work packages.** This roadmap was
originally planned as 31+ separate work packages (plus a lettered insertion,
WP-02A) before any of WP-07 onward were built. To keep every WP a
substantial, Principal-Engineer-level capability rather than a small feature
slice, and to avoid an ever-growing tail of thin WPs, this document
consolidates that original plan into exactly 20 work packages. **No useful
learning material was deleted** — every topic the original plan named still
has a deliberate place, either as its own WP or grouped into a related one.
The full mapping from the original numbering to this one:

| Original plan | Now |
|---|---|
| WP-01, WP-02 | Unchanged (WP-01, WP-02) |
| WP-02A (this document, the reference-repository matrix, the failure matrix — no implementation) | Folded into the Foundation phase's documentation set (WP-01/WP-02); it produced documentation, not a lab, so it does not need its own numbered slot in a 20-WP plan |
| WP-03, WP-04, WP-05 | Unchanged (WP-03, WP-04, WP-05) |
| WP-06 (offset management & delivery semantics) | Unchanged number — **this was always the intent**; see the historical note below on how this number was contested and resolved |
| The original WP-06 (replication and broker failure, never implemented) | **WP-07** |
| Original WP-07 (KRaft controller quorum) | **WP-08** |
| Original WP-08 (idempotent producers and transactions) | **WP-09** |
| Original WP-09 (schema evolution) | **WP-10** |
| Original WP-10 (Kafka Connect) + WP-11 (Debezium CDC) | Merged into **WP-11** (Kafka Connect & CDC) |
| Original WP-12 (transactional outbox) | Unchanged (WP-12) |
| Original WP-13 (Kafka Streams) | **WP-14** |
| Original WP-14 (Spring Kafka) | **WP-15** |
| Original WP-15 (retry/DLQ + idempotent consumer) | **WP-13** (moved earlier, ahead of Streams/Spring Kafka) |
| Original WP-16 (observability) | Unchanged number, **WP-16**, now explicitly merged with troubleshooting (curriculum-map row 23, which never had its own dedicated WP) |
| Original WP-17 (performance benchmarking) + WP-20 (capacity planning) | Merged into **WP-17** (Performance & Capacity Engineering) |
| Original WP-18 (security) | Unchanged (WP-18) |
| Original WP-19 (failure-injection suite) + WP-21 (production simulation) | Merged into **WP-19** (Failure Engineering & Production Simulation) |
| Original WP-22 (system design), WP-23 (ADRs), WP-24 (PE decision framework, source-code reading, interview banks, milestone assessments), WP-26 (partition lifecycle), WP-27 (cluster rebalancing/Cruise Control), WP-28 (multi-cluster/DR), WP-29 (platform governance), WP-30 (cost engineering), WP-31 (Kubernetes/Strimzi) | All merged into **WP-20**, the Principal Engineer capstone |
| Original WP-25+ (CI: build, unit tests, integration tests, lint, doc-link checks) | Not a curriculum WP in the same sense as the others — it is ongoing engineering practice threaded through every WP's own validation (`./gradlew build`/`test` per lab, plus regression checks on prior labs), not a separate numbered slot in this 20-WP plan |

This mapping is the authoritative reference for any historical PR, commit
message, or discussion that cites an old WP number for content at or after
the original WP-06 — treat this table as the translation key.

**A note on WP-06's number itself.** A later work-package request explicitly
asked for "WP-06 — Offset Management & Delivery Semantics" at a time when
this roadmap still had WP-06 assigned to the never-implemented replication
content above. That request was fulfilled by a prior revision of this
document via a temporary lettered insertion (`WP-06A`, holding the
displaced replication scope) rather than a full renumbering — a reasonable
choice at the time, given the cost of renumbering WP-06 through WP-31 for
content that didn't exist yet in either case. This document now completes
that resolution properly: **WP-06 is confirmed as Offset Management &
Delivery Semantics** (no longer a temporary repurposing), the displaced
replication content becomes a permanent, non-lettered **WP-07**, and the
`WP-06A` label is retired entirely — it no longer appears anywhere in this
roadmap. WP-06's own implementation, experiments, and tests
(`lab-05-offset-management-delivery-semantics`) are unaffected by this
renumbering; only this document's bookkeeping around it changed.

**A note on WP-03 through WP-05's lab numbers.** Earlier drafts of this
roadmap implied separate `lab-03-java-producer` and `lab-04-java-consumer`
labs, with partitioning arriving later as `lab-06-partitioning`, and
consumer groups and rebalancing split across two further labs,
`lab-05-consumer-groups` and `lab-07-rebalancing`. WP-03 instead delivered
one combined lab, `lab-02-native-java-producer-consumer`, covering both
producer and consumer roles together — they share a single central
question (what happens between `producer.send()` and a Java consumer
receiving the record) and splitting them into two labs would have meant
re-deriving that same end-to-end story twice. WP-04 then claimed
`lab-03-partitioning-ordering`, the next sequential unclaimed number,
rather than the originally-planned `lab-06`. WP-05 applies the identical
reasoning a third time: consumer groups and rebalancing are one coherent
question (how does a group divide work, and what happens when that
division changes), so they land together in one lab,
`lab-04-consumer-groups-rebalancing` — the next sequential unclaimed
number — rather than as the originally-planned `lab-05` and `lab-07`
split around partitioning's old `lab-06` slot. In every case, no
*already-created* lab or WP number changed to make room — `lab-03`/`lab-04`
(as separate producer/consumer labs), `lab-05`, `lab-06`, and `lab-07` (as
originally planned) are simply retired as planned-but-never-created slots,
the same pattern established when WP-03 landed.

## Recommended learning order

1. Read this roadmap and the [mental model](../architecture/KAFKA_MENTAL_MODEL.md) document.
2. Stand up the local cluster (`lab-01`) before writing any code.
3. Implement the native Java producer and consumer (`lab-02`) before
   touching Spring Kafka. Spring Kafka is introduced only after you understand what
   it is wrapping.
4. Work through partitioning, consumer groups, and replication in that order —
   each failure lab depends on understanding the mechanism it breaks.
5. Do not skip the failure-injection labs. They are not optional extras; they are
   where the actual engineering judgment is built. See
   [`PRINCIPAL_ENGINEER_FAILURE_MATRIX.md`](PRINCIPAL_ENGINEER_FAILURE_MATRIX.md)
   for the full map of failures this repository builds toward, including the
   ones not yet implemented.
6. From WP-03 onward, prefer real (Testcontainers-backed) Kafka integration
   tests over mocked brokers — see the testing progression in
   [`REFERENCE_REPOSITORIES.md`](../references/REFERENCE_REPOSITORIES.md#testing-strategy--first-class-from-wp-03-onward).
   A mock can confirm your code calls the client API correctly; it cannot
   confirm your understanding of Kafka's actual behavior is correct.
7. Read ADRs only after doing the lab or design exercise they correspond to. An
   ADR read before the evidence exists is just an opinion.
8. Use `interview/` continuously, not just before an interview — the questions are
   a diagnostic for gaps in your own reasoning.

## Non-goals

- This is **not** a ZooKeeper-based curriculum. ZooKeeper is covered historically,
  in context, so you can understand legacy production environments — it is never
  the primary path. This extends to reference material: an older, ZooKeeper-oriented
  example from any external repository (see
  [`REFERENCE_REPOSITORIES.md`](../references/REFERENCE_REPOSITORIES.md)) is never
  allowed to influence this project's own KRaft-first architecture, no matter how
  otherwise well-regarded that external repository is.
- This is **not** a Spring-Kafka-only curriculum. Spring Kafka is taught, but never
  as a substitute for understanding the native Kafka client and protocol.
- This repository does not publish invented benchmark numbers. Any performance
  figure that appears in this repository is reproducible from a documented lab, on
  documented hardware, or it does not appear at all.
- This repository does not publish universal configuration formulas — for
  partition count, replication factor, retention, or anything else. Every such
  recommendation that appears here is tied to explicit workload and SLO
  assumptions, per the Principal Engineer decision lens above; a number without
  its assumptions is not a recommendation, it's a guess wearing a number's
  clothing.
- This repository never represents Kafka's own exactly-once semantics (idempotent
  producers, transactions, `read_committed`) as making an arbitrary external
  business side effect exactly-once. Kafka's EOS is precise and real *within
  Kafka* (no duplicate records from producer retries, no partial transactional
  reads); what your application does with a record once delivered — a database
  write, an email, a charge — is a separate reliability problem this curriculum
  treats explicitly as its own topic (curriculum-map row 13), not as something
  Kafka solves for you.
- This repository does not treat Confluent-ecosystem tooling (Schema Registry,
  Confluent's example repositories, Confluent Cloud) as Apache Kafka itself —
  see the ecosystem-boundary note near the top of this document and in
  [`REFERENCE_REPOSITORIES.md`](../references/REFERENCE_REPOSITORIES.md).
