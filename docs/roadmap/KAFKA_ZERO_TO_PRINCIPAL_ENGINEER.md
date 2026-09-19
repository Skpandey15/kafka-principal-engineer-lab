# Kafka: Zero to Principal Engineer — Curriculum Roadmap

This document is the map of the entire repository. It describes the full curriculum,
the milestones an engineer progresses through, and the work packages (WPs) used to
build the repository incrementally. If you only read one document before starting,
read this one and [`KAFKA_MENTAL_MODEL.md`](../architecture/KAFKA_MENTAL_MODEL.md).

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

## Milestones

Milestones are checkpoints, not chapters. Each one has an explicit assessment before
moving on. A milestone is "reached" when you can answer its assessment without
looking anything up.

| Milestone | Title | You can... | Assessed by |
|---|---|---|---|
| M0 | Foundation | Explain Kafka's architecture: brokers, topics, partitions, replication, KRaft, and why Kafka exists relative to queues/DBs. | `docs/principal-engineer/` M0 checkpoint + `interview/fundamentals/` (both added once the fundamentals content lands) |
| M1 | Developer | Implement reliable Java producers and consumers, reason about serialization, and read consumer group state. | `lab-03` through `lab-06` |
| M2 | Senior Engineer | Design topics, partition keys, retry/DLQ strategy, and schema evolution policy; explain delivery semantics precisely. | `lab-07` through `lab-13`, `lab-16`, `lab-19` |
| M3 | Staff Engineer | Operate a cluster under failure, benchmark it, evolve schemas safely, and diagnose incidents from metrics and logs alone. | `lab-20` through `lab-24`, `docs/troubleshooting/` |
| M4 | Principal Engineer | Design organization-scale event platforms and defend architecture decisions with trade-offs, not opinions. | `docs/principal-engineer/`, `adrs/`, `system-design/` |
| M5 | Kafka Deep Dive | Reason about Kafka internals, failure modes, capacity, multi-cluster/multi-region architecture, and source-level behavior. | `docs/principal-engineer/KAFKA_SOURCE_CODE_READING_GUIDE.md`, `interview/principal/` |

Each milestone's formal assessment content is added in the work package that
completes the material it depends on — see the WP plan below. This avoids assessing
knowledge the repository hasn't taught yet.

## Curriculum map

The curriculum is organized by topic area, not strictly by milestone, because real
Kafka expertise is built by revisiting the same concepts (partitioning, failure,
delivery semantics) at increasing depth. Each row below will become one or more
`docs/<area>/` documents and one or more `labs/lab-NN-*` directories.

| # | Area | Core questions it answers | Primary docs | Primary labs |
|---|---|---|---|---|
| 0 | Distributed systems foundation | Why does Kafka exist? Queue vs. log? What does "durable" actually mean? | `docs/fundamentals/` | — |
| 1 | Kafka fundamentals | What is a broker, partition, offset, ISR, controller? | `docs/fundamentals/` | `lab-01`, `lab-02` |
| 2 | Producer internals | What happens inside `producer.send()`? | `docs/producer/` | `lab-03`, `lab-10` |
| 3 | Consumer internals | How does polling, committing, and rebalancing actually work? | `docs/consumer/`, `docs/consumer-groups/` | `lab-04`, `lab-05`, `lab-07` |
| 4 | Partitioning | Why do partition keys matter more than almost any other decision? | `docs/partitioning/` | `lab-06` |
| 5 | Replication & failure | How does Kafka survive broker loss without losing data? | `docs/replication/` | `lab-08`, `lab-09` |
| 6 | KRaft | How does the cluster agree on metadata without ZooKeeper? | `docs/kraft/` | `lab-08`, `lab-09` |
| 7 | Storage internals | Why is Kafka fast? What is a segment, index, and compaction? | `docs/storage/` | `lab-01` (inspection) |
| 8 | Delivery semantics | What does "exactly-once" really mean, and when does it lie? | `docs/delivery-semantics/` | `lab-10`, `lab-11`, `lab-12` |
| 9 | Serialization & schema governance | How do you evolve a schema without breaking consumers? | `docs/serialization/`, `docs/schema-registry/` | `lab-13` |
| 10 | Kafka Connect | How do you move data in/out of Kafka without hand-written glue? | `docs/kafka-connect/` | `lab-14` |
| 11 | CDC + Debezium | How do database changes become an event stream? | `docs/kafka-connect/` | `lab-15` |
| 12 | Transactional outbox | How do you avoid the dual-write problem? | `docs/patterns/` | `lab-16` |
| 13 | Kafka Streams | How do you build stateful stream processing on top of Kafka? | `docs/kafka-streams/` | `lab-17` |
| 14 | Spring Kafka | How does a production framework map onto the primitives you already know? | `docs/spring-kafka/` | `lab-18` |
| 15 | Event-driven patterns | Which pattern fits which problem, and when should you avoid Kafka entirely? | `docs/patterns/` | `lab-16`, `lab-19` |
| 16 | Observability | What do you monitor, and what does each signal mean? | `docs/observability/` | `lab-20` |
| 17 | Performance engineering | What actually limits throughput and latency? | `docs/performance/` | `lab-21` |
| 18 | Capacity planning | How do you size a cluster from a workload description? | `docs/performance/` | `lab-24` |
| 19 | Security | How do you secure a cluster without breaking it? | `docs/security/` | `lab-22` |
| 20 | Failure engineering | What actually happens when you kill a broker, a consumer, or the network? | `docs/failure-recovery/` | `lab-09`, `lab-23` |
| 21 | Troubleshooting | Given symptoms and metrics, what is the diagnostic sequence? | `docs/troubleshooting/` | all failure labs |
| 22 | System design | How do you design a real platform end to end? | `system-design/` | `lab-25` |
| 23 | Principal Engineer decisions | Why this choice and not that one? | `docs/principal-engineer/`, `adrs/` | all |
| 24 | Interview preparation | Can you reason under pressure, out loud, from first principles? | `interview/` | — |
| 25 | Source-code reading | Where in `apache/kafka` does this behavior actually live? | `docs/principal-engineer/KAFKA_SOURCE_CODE_READING_GUIDE.md` | — |

## Work package plan

The repository is built incrementally. Each work package (WP) is a reviewable unit
of work — never a single giant commit. This list is the current plan and will be
refined as earlier WPs surface new information; it is not a fixed contract.

| WP | Scope | Status |
|---|---|---|
| WP-01 | Repository foundation: README, this roadmap, reference repositories, Kafka mental model, contributing guide, `.gitignore`, license. | **This work package.** |
| WP-02 | Local KRaft environment: Docker Compose cluster, topic/partition/offset inspection, CLI walkthrough. | Planned |
| WP-03 | Native Java producer/consumer fundamentals (no Spring). | Planned |
| WP-04 | Partitioning experiments: good vs. bad keys, hot partitions. | Planned |
| WP-05 | Consumer groups and rebalancing, including cooperative rebalancing and static membership. | Planned |
| WP-06 | Replication and broker failure experiments; ISR and `min.insync.replicas`. | Planned |
| WP-07 | KRaft controller quorum and controller failure. | Planned |
| WP-08 | Idempotent producers and transactions; delivery semantics experiments. | Planned |
| WP-09 | Schema evolution: Avro/Protobuf + Schema Registry, compatibility modes. | Planned |
| WP-10 | Kafka Connect pipeline (source + sink). (`lab-14-kafka-connect`) | Planned |
| WP-11 | Debezium CDC pipeline against PostgreSQL. (`lab-15-debezium-cdc`) | Planned |
| WP-12 | Transactional outbox pattern, built directly on the CDC pipeline from WP-11. (`lab-16-outbox`) | Planned |
| WP-13 | Kafka Streams applications (DSL + Processor API, joins, windows, state stores). (`lab-17-kafka-streams`) | Planned |
| WP-14 | Spring Kafka in production style: listener containers, retry topics, DLQ, transactions. (`lab-18-spring-kafka`) | Planned |
| WP-15 | Retry/DLQ patterns and idempotent-consumer/deduplication patterns. (`lab-19-retry-dlq`) | Planned |
| WP-16 | Observability stack: JMX → Prometheus → Grafana, key dashboards. (`lab-20-observability`) | Planned |
| WP-17 | Performance benchmarking harness and documented results. (`lab-21-performance`) | Planned |
| WP-18 | Security: TLS, SASL/SCRAM, ACLs (local-development-only patterns clearly marked). (`lab-22-security`) | Planned |
| WP-19 | Failure-injection lab suite (broker kill, network partition, slow consumer, disk pressure). (`lab-23-failure-injection`) | Planned |
| WP-20 | Capacity planning workbook with worked examples. (`lab-24-capacity-planning`) | Planned |
| WP-21 | Production simulation lab combining prior labs into one running system. (`lab-25-production-simulation`) | Planned |
| WP-22 | System design labs: e-commerce, payments, fraud detection, notifications, clickstream, booking spikes. (`system-design/`) | Planned |
| WP-23 | ADR set (ADR-001 through ADR-018), written only after the supporting lab/design evidence exists. (`adrs/`) | Planned |
| WP-24 | Principal Engineer decision framework, source-code reading guide, interview question banks, milestone assessments. | Planned |
| WP-25+ | CI (build, unit tests, integration tests, lint, doc-link checks), introduced incrementally. | Planned |

WPs are not strictly sequential milestones — several can proceed once their
prerequisites exist — but the order above reflects genuine dependencies: for
example, transactions (WP-08) before the outbox pattern, and Kafka Connect
(WP-10) before Debezium CDC (WP-11) before the transactional outbox (WP-12),
since this repository's outbox lab is built directly on top of the CDC pipeline
rather than as a standalone pattern. Lab numbers therefore run
`lab-14-kafka-connect` → `lab-15-debezium-cdc` → `lab-16-outbox` consecutively,
even though the outbox *pattern* is conceptually closer to the event-driven
patterns covered later — the numbering follows build dependency order, not
topic difficulty.

## Recommended learning order

1. Read this roadmap and the [mental model](../architecture/KAFKA_MENTAL_MODEL.md) document.
2. Stand up the local cluster (`lab-01`) before writing any code.
3. Implement the native Java producer and consumer (`lab-03`, `lab-04`) before
   touching Spring Kafka. Spring Kafka is introduced only after you understand what
   it is wrapping.
4. Work through partitioning, consumer groups, and replication in that order —
   each failure lab depends on understanding the mechanism it breaks.
5. Do not skip the failure-injection labs. They are not optional extras; they are
   where the actual engineering judgment is built.
6. Read ADRs only after doing the lab or design exercise they correspond to. An
   ADR read before the evidence exists is just an opinion.
7. Use `interview/` continuously, not just before an interview — the questions are
   a diagnostic for gaps in your own reasoning.

## Non-goals

- This is **not** a ZooKeeper-based curriculum. ZooKeeper is covered historically,
  in context, so you can understand legacy production environments — it is never
  the primary path.
- This is **not** a Spring-Kafka-only curriculum. Spring Kafka is taught, but never
  as a substitute for understanding the native Kafka client and protocol.
- This repository does not publish invented benchmark numbers. Any performance
  figure that appears in this repository is reproducible from a documented lab, on
  documented hardware, or it does not appear at all.
