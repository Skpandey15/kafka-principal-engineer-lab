# Milestone Assessments (WP-20)

## Purpose

The roadmap defines six milestones (`KAFKA_ZERO_TO_PRINCIPAL_ENGINEER.md`,
"Milestones") as checkpoints, not chapters — each "reached" when its
assessment can be answered without looking anything up. This document is
that assessment content, one section per milestone, written now that
every WP it depends on (WP-01 through WP-20) actually exists. Each
question is answerable from this repository's own labs and docs; none
requires outside material.

A milestone is genuinely reached only when you can also point to *which
lab* demonstrated the answer, not just state the fact — that's the
difference this repository draws everywhere between "can call an API"
and "understands the system" (`CONTRIBUTING.md`'s learning loop).

## M0 — Foundation

You can explain Kafka's architecture and why it exists relative to
queues/databases, without looking anything up.

1. Why is Kafka a distributed log, not a queue? What does a consumer
   "removing" a message even mean in a log, versus in a traditional
   queue? (`docs/fundamentals/`, `KAFKA_MENTAL_MODEL.md`)
2. What is a partition, and why is it the unit of both ordering and
   parallelism? (`docs/partitioning/`, `lab-03`)
3. What replaced ZooKeeper, and what problem was ZooKeeper solving that
   still needs solving? (`docs/kraft/`, `lab-07`)
4. What is an offset, and whose responsibility is tracking "how far a
   consumer has gotten" — the broker's, or the consumer's? (`lab-02`)

## M1 — Developer

You can implement reliable Java producers and consumers and reason about
serialization and consumer group state.

1. What is the actual difference between `acks=0`, `acks=1`, and
   `acks=all` — traced through what the broker does before responding
   in each case? (`lab-02`, `lab-06`)
2. Walk through `producer.send()` to a record actually being durable —
   name every place data could still be lost along the way, and which
   configuration closes each gap.
3. What does `kafka-consumer-groups.sh --describe` actually show you,
   and how would you tell a genuinely stuck consumer from one that's
   just idle? (`lab-04`)

## M2 — Senior Engineer

You can design topics, partition keys, retry/DLQ strategy, and schema
evolution policy, and explain delivery semantics precisely.

1. Design a partition key for an order-processing topic where
   per-customer ordering matters but a few large customers would
   otherwise create hot partitions. What trade-off are you accepting?
   (`lab-03`, `docs/partitioning/`)
2. What does Kafka's own exactly-once semantics (idempotent producer +
   transactions + `read_committed`) actually guarantee, and what does it
   explicitly *not* guarantee about a downstream database write?
   (`lab-08`, `docs/delivery-semantics/` — the dual-write problem)
3. A consumer hits a record it can never successfully process. Walk
   through what happens with no DLQ, then with one. (`lab-12`)
4. What breaks a schema evolution that looks backward-compatible on
   paper? Give a concrete example your compatibility-mode choice would
   catch. (`lab-09`)
5. (WP-20) A consumer group's committed offset on a primary cluster
   needs to be usable on a secondary cluster after a mirrored failover.
   What Kafka-native mechanism makes that possible, and what does it
   depend on being accurate? (`lab-19`, `RemoteClusterUtils.translateOffsets`)

## M3 — Staff Engineer

You can operate a cluster under failure, benchmark it, evolve schemas
safely, and diagnose incidents from metrics and logs alone.

1. A broker dies mid-write. Given `acks=all` and
   `min.insync.replicas=2` on a 3-replica topic, was the last
   acknowledged write definitely durable? What if it wasn't
   acknowledged yet? (`lab-06`, failure matrix)
2. Consumer lag is climbing and retention is 24 hours. What is the
   actual failure mode if nothing changes, and what's the very first
   thing you'd check to find the bottleneck? (`lab-15`)
3. What did this repository actually measure about batching and
   compression's effect on throughput — and why does that number only
   apply to the hardware and workload it was measured on? (`lab-16`,
   roadmap Non-goals — no invented benchmark numbers)
4. Two failures overlap: a broker dies while a consumer group is
   rebalancing. What guarantees from each individual failure lab still
   need to hold, and how would you prove they do? (`lab-18`)

## M4 — Principal Engineer

You can design organization-scale event platforms and defend
architecture decisions with trade-offs, not opinions.

1. Walk through the Principal Engineer decision lens applied to a
   multi-cluster DR architecture choice for a specific (stated) RPO/RTO
   requirement. Defend active/passive vs. active/active from the actual
   requirement, not from general resilience intuition.
   (`docs/principal-engineer/KAFKA_ARCHITECTURE_DECISION_FRAMEWORK.md`,
   Decision 1)
2. Why doesn't adding a broker to a cluster rebalance load
   automatically, and what closes that gap — as a concept, not a
   specific product pitch? (`KAFKA_MULTI_CLUSTER_AND_DR.md`, §5)
3. Write the cost formula for a cross-region DR setup, with every term's
   assumption stated explicitly. What happens to that formula under
   active/active versus active/passive? (`KAFKA_MULTI_CLUSTER_AND_DR.md`, §7)
4. When does a Kafka platform need client quotas, and what's the actual
   mechanism (throttling vs. rejection) once a client exceeds one?
   (`KAFKA_MULTI_CLUSTER_AND_DR.md`, §6)

## M5 — Kafka Deep Dive

You can reason about Kafka internals, failure modes, capacity, and
source-level behavior.

1. Trace `enable.auto.commit`'s final commit at `consumer.close()` to
   the actual client-internals class responsible, and explain how it
   silently overwrote an explicit prior commit in a real test.
   (`docs/principal-engineer/KAFKA_SOURCE_CODE_READING_GUIDE.md`,
   Exercise 3; `lab-19`)
2. Explain, at the mechanism level (not just "it's approximate"), why
   MM2's checkpoint offset translation can be inaccurate on a low-volume
   topic, and name the specific configuration that controls the
   precision/overhead trade-off. (Source-code guide, Exercise 2)
3. Given a behavior you observed in any lab in this repository, locate
   where in `apache/kafka`'s own source that behavior is actually
   implemented — not "somewhere in the producer," the actual subsystem.
   (Source-code guide, general standard)

## How to use this document

Work through a milestone's questions only after its prerequisite labs
are done — an assessment answered by guessing or by reading the linked
lab immediately beforehand does not mean the milestone is reached. Per
the roadmap's recommended learning order, treat `interview/fundamentals/`
and `interview/principal/` as continuous practice alongside these
milestone checkpoints, not a separate final step.
