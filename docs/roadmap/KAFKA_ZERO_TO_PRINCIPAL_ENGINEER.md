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
| M1 | Developer | Implement reliable Java producers and consumers, reason about serialization, and read consumer group state. | `lab-02` through `lab-07` |
| M2 | Senior Engineer | Design topics, partition keys, retry/DLQ strategy, and schema evolution policy; explain delivery semantics precisely, including exactly what Kafka's own exactly-once semantics do and do not guarantee about your business logic. | `lab-07` through `lab-13`, `lab-16`, `lab-19` |
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
  → WP-09 through WP-15

LEVEL 4 — Production engineering
  Client resilience, retry/DLQ, observability, performance, security,
  failure engineering, troubleshooting, capacity planning
  → WP-15 through WP-21, plus the client-resilience topic threaded through
    WP-03, WP-06, and WP-08 rather than owning a single dedicated WP

LEVEL 5 — Fleet / platform engineering
  Partition lifecycle, replica reassignment, cluster balancing, Cruise
  Control, multi-cluster, disaster recovery, RPO/RTO, Kafka on Kubernetes,
  Strimzi
  → WP-26 through WP-31 (new in this document — see the work package plan;
    all currently Unscheduled beyond being named and scoped)

LEVEL 6 — Principal Engineer
  Platform governance, cost engineering, system design, ADRs, architecture
  reviews, source-code reading, migration strategy, upgrade strategy, SLO
  design, Principal Engineer failure reviews
  → WP-22 through WP-24, plus WP-29/WP-30 (governance, cost) from Level 5's
    new WP range
```

Level 5 and 6 overlap in practice — governance and cost engineering (Level 6)
are meaningless without the fleet-operations concepts (Level 5) they're
applied to, and both ultimately feed the same ADRs and system designs. The
split above is for navigation, not a claim that Level 6 strictly follows
Level 5 in every learner's actual path.

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
| 2 | Producer internals | What happens inside `producer.send()`? | `docs/producer/` | `lab-02`, `lab-10` |
| 3 | Consumer internals | How does polling, committing, and rebalancing actually work? | `docs/consumer/`, `docs/consumer-groups/` | `lab-02`, `lab-05`, `lab-07` |
| 4 | Partitioning | Why do partition keys matter more than almost any other decision? | `docs/partitioning/` | `lab-03` |
| 5 | Replication & failure | How does Kafka survive broker loss without losing data? | `docs/replication/` | `lab-08`, `lab-09` |
| 6 | KRaft | How does the cluster agree on metadata without ZooKeeper? | `docs/kraft/` | `lab-08`, `lab-09` |
| 7 | Storage internals | Why is Kafka fast? What is a segment, index, and compaction? | `docs/storage/` | `lab-01` (inspection) |
| 8 | Delivery semantics | What does "exactly-once" really mean, and when does it lie? | `docs/delivery-semantics/` | `lab-10`, `lab-11`, `lab-12` |
| 9 | Serialization & schema governance | How do you evolve a schema without breaking consumers? (Note: Schema Registry itself is a Confluent-ecosystem concept layered on top of Kafka, not a Kafka broker feature — see the reference-repositories boundary note above.) | `docs/serialization/`, `docs/schema-registry/` | `lab-13` |
| 10 | Kafka Connect | How do you move data in/out of Kafka without hand-written glue? | `docs/kafka-connect/` | `lab-14` |
| 11 | CDC + Debezium | How do database changes become an event stream? | `docs/kafka-connect/` | `lab-15` |
| 12 | Transactional outbox | How do you avoid the dual-write problem? | `docs/patterns/` | `lab-16` |
| 13 | Idempotent consumer & business exactly-once | Kafka delivery guarantees ≠ business exactly-once processing — where is *your* idempotency boundary? | `docs/patterns/` | Conceptually anchored directly after outbox (row 12); hands-on lab delivered in `lab-19` alongside retry/DLQ (WP-15) |
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
| 24 | Partition lifecycle engineering | How do you choose an initial partition count, and what actually happens — to ordering, to consumer concurrency, to replica placement — when you change it later? | `docs/partitioning/` (deep-dive) | WP-26, Unscheduled |
| 25 | Cluster rebalancing & Cruise Control | Consumer-group rebalancing and cluster data/replica rebalancing are different problems — why doesn't adding a broker automatically balance a cluster? | `docs/replication/` or a future `docs/cluster-operations/` | WP-27, Unscheduled |
| 26 | Multi-cluster & disaster recovery | Active/passive vs. active/active, RPO/RTO, producer/consumer failover — architecture first, product choice second | A future `docs/multi-cluster/` | WP-28, Unscheduled |
| 27 | Kafka on Kubernetes / Strimzi | Just because Kafka *can* run on Kubernetes, should this organization run it there? | A future `docs/kubernetes/` | WP-31, Unscheduled — explicitly not introduced into the current implementation |
| 28 | Platform governance | Topic/schema/security governance, quotas, SLOs — Kafka as an enterprise platform, not just a broker | A future `docs/governance/` | WP-29, Unscheduled |
| 29 | Cost engineering | Event rate × size × retention × replication, plus network/cross-AZ/DR duplication — with assumptions always explicit | `docs/performance/` (extended) | WP-30, Unscheduled |
| 30 | System design | How do you design a real platform end to end, forced through partition key, replication, semantics, schema, retention, retry/DLQ, ordering, scaling, DR, security, and cost decisions together? | `system-design/` | `lab-25` |
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
(planned for WP-24) will apply it explicitly to the named decision points
listed under WP-24 below; until then, use it directly when working through
an ADR or a system-design exercise.

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

The repository is built incrementally. Each work package (WP) is a reviewable unit
of work — never a single giant commit. This list is the current plan and will be
refined as earlier WPs surface new information; it is not a fixed contract.

| WP | Scope | Status |
|---|---|---|
| WP-01 | Repository foundation: README, this roadmap, reference repositories, Kafka mental model, contributing guide, `.gitignore`, license. | Done |
| WP-02 | Local KRaft environment: Docker Compose cluster, topic/partition/offset inspection, CLI walkthrough. (`lab-01-first-kafka-cluster`) | Done |
| WP-02A | Curriculum, reference-architecture, and Principal Engineer learning enhancement (this document, the reference-repository matrix, and the failure matrix). No implementation. | Done |
| WP-03 | Native Java producer/consumer fundamentals (no Spring). (`lab-02-native-java-producer-consumer`) | Done |
| WP-04 | Partitioning experiments: good vs. bad keys, hot partitions. (`lab-03-partitioning-ordering`) | **This work package.** |
| WP-05 | Consumer groups and rebalancing, including cooperative rebalancing and static membership. | Planned |
| WP-06 | Replication and broker failure experiments; ISR and `min.insync.replicas`. | Planned |
| WP-07 | KRaft controller quorum and controller failure. | Planned |
| WP-08 | Idempotent producers and transactions; delivery semantics experiments. | Planned |
| WP-09 | Schema evolution: Avro/Protobuf + Schema Registry, compatibility modes. | Planned |
| WP-10 | Kafka Connect pipeline (source + sink). (`lab-14-kafka-connect`) | Planned |
| WP-11 | Debezium CDC pipeline against PostgreSQL. (`lab-15-debezium-cdc`) | Planned |
| WP-12 | Transactional outbox pattern, built directly on the CDC pipeline from WP-11. (`lab-16-outbox`) | Planned |
| WP-13 | Kafka Streams applications (DSL + Processor API, joins, windows, state stores, `TopologyTestDriver`, failure and state-restoration behavior). (`lab-17-kafka-streams`) | Planned |
| WP-14 | Spring Kafka in production style: listener containers, retry topics, DLQ, transactions. (`lab-18-spring-kafka`) | Planned |
| WP-15 | Retry/DLQ patterns and the idempotent-consumer/deduplication topic (curriculum-map row 13): idempotency keys, processed-event tables, unique constraints, and the crash scenarios that make them necessary. (`lab-19-retry-dlq`) | Planned |
| WP-16 | Observability stack: JMX → Prometheus → Grafana, key dashboards. (`lab-20-observability`) | Planned |
| WP-17 | Performance benchmarking harness and documented results. (`lab-21-performance`) | Planned |
| WP-18 | Security: TLS, SASL/SCRAM, ACLs (local-development-only patterns clearly marked). (`lab-22-security`) | Planned |
| WP-19 | Failure-injection lab suite (broker kill, network partition, slow consumer, disk pressure) — implements the applicable rows of [`PRINCIPAL_ENGINEER_FAILURE_MATRIX.md`](PRINCIPAL_ENGINEER_FAILURE_MATRIX.md). (`lab-23-failure-injection`) | Planned |
| WP-20 | Capacity planning workbook with worked examples. (`lab-24-capacity-planning`) | Planned |
| WP-21 | Production simulation lab combining prior labs into one running system. (`lab-25-production-simulation`) | Planned |
| WP-22 | System design labs: e-commerce, payments, fraud detection, notifications, clickstream, inventory, CDC/data-integration, and high-volume/booking-style demand spikes. (`system-design/`) | Planned |
| WP-23 | ADR set (ADR-001 through ADR-018), written only after the supporting lab/design evidence exists. (`adrs/`) | Planned |
| WP-24 | Principal Engineer decision framework (`docs/principal-engineer/KAFKA_ARCHITECTURE_DECISION_FRAMEWORK.md`, applying the decision lens above), source-code reading guide, interview question banks, milestone assessments. | Planned |
| WP-25+ | CI (build, unit tests, integration tests, lint, doc-link checks), introduced incrementally. | Planned |
| WP-26 | Partition lifecycle engineering: growing partition count, key-to-partition remapping implications, what does and doesn't move when you reassign replicas. Lab number to be assigned when scheduled. | Unscheduled |
| WP-27 | Cluster rebalancing & Cruise Control: broker addition/removal, leader/replica/disk/network skew, rack awareness, optimization goals. Lab number to be assigned when scheduled. | Unscheduled |
| WP-28 | Multi-cluster architecture and disaster recovery: active/passive vs. active/active, RPO/RTO, producer/consumer failover, schema availability during failover, testing DR rather than only documenting it. Lab number to be assigned when scheduled. | Unscheduled |
| WP-29 | Kafka platform governance: topic/schema/security governance, quotas, noisy-neighbor controls, SLOs, change/upgrade management. | Unscheduled |
| WP-30 | Cost engineering: worked capacity/cost models with explicit assumptions (event rate, size, retention, replication, cross-AZ/region traffic, DR duplication). | Unscheduled |
| WP-31 | Kafka on Kubernetes via Strimzi: KRaft on Kubernetes, `KafkaNodePool`-era architecture, rolling upgrades, and the "should we" trade-off discussion, not just the "how." Explicitly deferred — no Kubernetes in the current implementation. | Unscheduled |

WPs are not strictly sequential milestones — several can proceed once their
prerequisites exist — but the order above reflects genuine dependencies: for
example, transactions (WP-08) before the outbox pattern, and Kafka Connect
(WP-10) before Debezium CDC (WP-11) before the transactional outbox (WP-12),
since this repository's outbox lab is built directly on top of the CDC pipeline
rather than as a standalone pattern. Lab numbers therefore run
`lab-14-kafka-connect` → `lab-15-debezium-cdc` → `lab-16-outbox` consecutively,
even though the outbox *pattern* is conceptually closer to the event-driven
patterns covered later — the numbering follows build dependency order, not
topic difficulty. The idempotent-consumer topic (curriculum-map row 13)
follows the same conceptual chain — it is the direct continuation of "what do
we do with reliably-published events once a consumer has to act on them
exactly once" — but its hands-on lab is delivered later, in WP-15, because a
realistic treatment benefits from the retry/DLQ mechanics WP-15 also covers;
the roadmap says so explicitly here rather than leaving the gap unexplained.

WP-26 through WP-31 are appended after the existing plan rather than
interleaved into it, specifically so that adding them required renumbering
nothing — every existing WP number, lab number, and file path in this
repository remains exactly what it was before this document was extended.

**A note on WP-03 and WP-04's lab numbers.** Earlier drafts of this
roadmap implied separate `lab-03-java-producer` and `lab-04-java-consumer`
labs, with partitioning arriving later as `lab-06-partitioning`. WP-03
instead delivered one combined lab, `lab-02-native-java-producer-consumer`,
covering both roles together — they share a single central question (what
happens between `producer.send()` and a Java consumer receiving the
record) and splitting them into two labs would have meant re-deriving that
same end-to-end story twice. WP-04 then claimed `lab-03-partitioning-ordering`,
the next sequential unclaimed number, rather than the originally-planned
`lab-06` — partitioning follows producer/consumer fundamentals directly in
this repository's actual delivery order, and there is no reason to leave a
numbering gap (`lab-03` through `lab-05`) just to preserve a slot
(`lab-06`) from an earlier draft's plan. In both cases, no *already-created*
lab or WP number changed to make room — `lab-03`/`lab-04` (as
producer/consumer labs) and `lab-06` (as the partitioning lab) are simply
retired as planned-but-never-created slots, the same pattern established
when WP-03 landed.

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
