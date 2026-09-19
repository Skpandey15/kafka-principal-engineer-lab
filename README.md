# Kafka Principal Engineer Lab

```text
Zero
  ↓
Developer
  ↓
Senior
  ↓
Staff
  ↓
Principal
```

A structured path from "I have never touched Kafka" to "I can defend Kafka
architecture decisions at Principal Engineer level," built as a curriculum, a
hands-on engineering lab, a production reference implementation, a failure-injection
lab, a system-design lab, and an interview-preparation resource — all in one
repository.

## Objective

Most Kafka material teaches an API. This repository teaches Kafka as a
**distributed system** that happens to have a Java client. Every non-trivial topic
is covered end to end: concept → architecture → internal mechanism → Java
implementation → run it → observe it → **break it intentionally** → troubleshoot it
→ measure it → production trade-offs → an Architecture Decision Record → Principal
Engineer questions. See
[`docs/roadmap/KAFKA_ZERO_TO_PRINCIPAL_ENGINEER.md`](docs/roadmap/KAFKA_ZERO_TO_PRINCIPAL_ENGINEER.md)
for the full philosophy and curriculum.

## Audience

An experienced Java engineer who wants genuine Kafka depth — not a Kafka novice
looking for a "hello world," and not someone looking for a Spring Kafka cheat
sheet. Comfort with Java, basic distributed-systems vocabulary, and Docker is
assumed; Kafka-specific knowledge is not.

## Learning philosophy

1. **Kafka is a distributed system, not just an API.** Every lab explains the
   mechanism before the code.
2. **Understanding failure is the curriculum, not an appendix.** Every major
   topic has a corresponding failure-injection experiment.
3. **No unexplained configuration.** Every important config value is taught with
   its trade-off, not just its name.
4. **Decisions need alternatives.** Every architecture decision (ADRs, system
   designs, the Principal Engineer framework) is documented with the options that
   were considered and rejected, not just the answer chosen.
5. **Modern Kafka first.** Labs target KRaft-based clusters. ZooKeeper is covered
   historically, for engineers who will encounter it in existing production
   systems, but it is never the primary path.
6. **No invented numbers.** Any performance figure in this repository is
   reproducible from a documented lab on documented hardware, or it does not
   appear.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the full set of engineering rules
that keep this repository honest as it grows.

## Milestones

| Milestone | You can... |
|---|---|
| **M0 — Foundation** | Explain Kafka's architecture and why it exists relative to queues and databases. |
| **M1 — Developer** | Implement reliable producers and consumers in Java. |
| **M2 — Senior Engineer** | Design topics, partitions, retries, schemas, and consumer groups. |
| **M3 — Staff Engineer** | Operate, troubleshoot, benchmark, and evolve Kafka systems in production. |
| **M4 — Principal Engineer** | Design organization-scale event platforms and defend the decisions with trade-offs. |
| **M5 — Kafka Deep Dive** | Reason about Kafka internals, failure modes, capacity, and multi-cluster/multi-region architecture. |

Full milestone definitions and assessments are in
[`docs/roadmap/KAFKA_ZERO_TO_PRINCIPAL_ENGINEER.md`](docs/roadmap/KAFKA_ZERO_TO_PRINCIPAL_ENGINEER.md).

## Repository map

```text
docs/            Curriculum documentation, organized by topic area.
labs/            Hands-on, numbered labs — the primary way you learn here.
services/        Example microservices used across multiple labs and system designs.
platform/        Shared Docker/Kafka/monitoring/schema infrastructure for labs.
system-design/   End-to-end system design exercises (e-commerce, payments, fraud, etc.).
adrs/            Architecture Decision Records, written only after supporting evidence exists.
interview/       Question banks by level: fundamentals, senior, staff, principal.
scripts/         Operational and utility scripts supporting the labs.
```

This structure grows incrementally — directories are added when a work package
needs them, not pre-created as empty scaffolding. See the roadmap for the full
planned structure and the work-package plan that builds it out.

## How to start

1. Read [`docs/roadmap/KAFKA_ZERO_TO_PRINCIPAL_ENGINEER.md`](docs/roadmap/KAFKA_ZERO_TO_PRINCIPAL_ENGINEER.md)
   in full — it is the map for everything else here.
2. Read [`docs/architecture/KAFKA_MENTAL_MODEL.md`](docs/architecture/KAFKA_MENTAL_MODEL.md),
   which answers: what actually happens between `producer.send()` and a consumer
   receiving the record?
3. Stand up the local cluster and work through
   [`labs/lab-01-first-kafka-cluster`](labs/lab-01-first-kafka-cluster/README.md)
   (see the roadmap's work-package plan for what's available now vs. planned).
4. Skim [`docs/references/REFERENCE_REPOSITORIES.md`](docs/references/REFERENCE_REPOSITORIES.md)
   for the external repositories this curriculum checks itself against (and
   what each one is and isn't authoritative for), and
   [`docs/roadmap/PRINCIPAL_ENGINEER_FAILURE_MATRIX.md`](docs/roadmap/PRINCIPAL_ENGINEER_FAILURE_MATRIX.md)
   for the full map of failures this repository builds toward across every
   topic area, not just the ones already implemented.

## Prerequisites

- Working Java experience (any recent LTS JDK; labs will specify exact versions
  as they are added).
- Docker and Docker Compose, for running a local Kafka cluster and supporting
  infrastructure.
- Comfort with the command line.
- No prior Kafka experience required.

## Recommended learning order

Native Java producer/consumer fundamentals **before** Spring Kafka. Partitioning
and consumer groups **before** replication and failure experiments. Every
failure-injection lab **before** moving to the next topic area — they are not
optional. ADRs **after** the lab or design exercise they correspond to, never
before. The full rationale is in the roadmap document.

## Status

This repository is being built incrementally, work package by work package, so
that each addition is reviewable on its own. See the work-package plan in the
roadmap for what exists today and what's next.
