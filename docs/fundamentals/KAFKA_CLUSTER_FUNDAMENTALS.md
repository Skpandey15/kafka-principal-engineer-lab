# Kafka Cluster Fundamentals

This document gives you the vocabulary and conceptual model behind
[`labs/lab-01-first-kafka-cluster`](../../labs/lab-01-first-kafka-cluster/README.md).
Read it before or alongside the lab — the lab is where these concepts stop
being definitions and start being things you can prove to yourself on your
own machine.

This document deliberately stays introductory. Producer internals, consumer
internals, replication, and KRaft's Raft mechanics each get their own,
much deeper document in a later work package (`docs/producer/`,
`docs/consumer/`, `docs/replication/`, `docs/kraft/`). Where this document
says "later," that is a real pointer, not a dismissal.

## Kafka cluster

```text
Kafka Cluster
   |
   +-- Broker(s)
          |
          +-- Topic partitions
```

A Kafka **cluster** is a set of cooperating Kafka nodes that together store
and serve topics. "Cluster" is a claim about coordination, not just about
counting machines: the nodes in a cluster share a `cluster.id`, agree
(via KRaft) on what topics and partitions exist and who leads each one, and
present themselves to clients as a single logical system reachable through
any of several bootstrap addresses. The lab you're about to run has a
cluster of exactly one node — a valid, minimal cluster, not an
approximation of one.

## Broker

A **broker** is a Kafka node in its data-plane role: it accepts
produce requests, appends records to the partitions it leads, serves fetch
requests from consumers, and replicates data with other brokers. When
someone says "connect to the Kafka broker," they mean exactly this role,
even when (as in this lab) the same process is also acting as a controller.

## Controller

The **controller** role is Kafka's control plane: deciding and durably
recording cluster metadata — which topics and partitions exist, their
configuration, which broker leads each partition, and which brokers are
currently considered part of the cluster. In a KRaft cluster, the nodes
acting as controllers form a Raft-replicated quorum that keeps this metadata
as its own internal, durably-replicated log, instead of delegating it to an
external system. This lab's single node acts as the only controller voter,
which is enough to have a functioning control plane, but not enough to
survive losing it — see the KRaft section below.

## KRaft

**KRaft** (Kafka Raft) is Kafka's built-in metadata-consensus protocol,
and the reason this lab needs no ZooKeeper. Before KRaft, Kafka relied on
an external ZooKeeper ensemble to store cluster metadata and elect a
controller; KRaft replaces that with a Raft-based quorum made of Kafka nodes
themselves (`process.roles=controller`, possibly combined with `broker` on
the same node, as in this lab).

Why this matters beyond "one fewer thing to install": ZooKeeper was a
separate distributed system with its own operational model, failure modes,
and scaling limits (notably around the number of partitions a cluster could
practically hold, since ZooKeeper's own write throughput became a
bottleneck for metadata changes at scale). KRaft folds metadata consensus
into Kafka's own log-based architecture, giving Kafka one operational model
and one consensus mechanism instead of two. Kafka 4.0 and later ship with
KRaft as the only supported mode — there is no ZooKeeper fallback to reach
for.

This document does not explain Raft's leader-election or log-replication
algorithm in detail — that belongs to `docs/kraft/`, once a multi-node
controller quorum exists to make the failure scenarios concrete (WP-08).
What matters here is the shape of the idea: metadata is a log, the same way
topic data is a log, and a quorum of nodes agrees on that log the same way
Kafka's replication protocol gets brokers to agree on a partition's data.

## Topic

A **topic** is a named, logical event stream — the unit application code
thinks in terms of ("the `orders` topic"). A topic has no size or ordering
guarantee of its own; both come from its partitions.

## Partition

A **partition** is an ordered, append-only log. It is the actual unit of
storage, ordering, and parallelism in Kafka:

- **Storage** — every partition is a physically separate log on disk (a
  sequence of segment files), independent of every other partition, even
  within the same topic.
- **Ordering** — Kafka guarantees order only *within* a single partition.
  There is no meaningful concept of "global order" across a topic's
  partitions; two records in different partitions have no defined relative
  order, no matter when they were produced.
- **Parallelism** — a partition can only ever be actively consumed by one
  member of a given consumer group at a time, so the number of partitions
  is the hard ceiling on how many consumers in one group can do useful work
  in parallel. A one-partition topic (as `orders` will be in the lab) has
  exactly one unit of read parallelism, by construction.

A topic is created with a chosen number of partitions; that number can
later be increased, but doing so does not repartition existing data — a
detail covered properly once the partitioning labs get to key-based hot
partitions and rebalancing.

## Record

A Kafka record is more than "a message." At minimum it carries:

- **Key** — optional; used for partition selection and for log compaction
  (`docs/storage/`, a later WP) if enabled. Not every record has a
  meaningful key.
- **Value** — the payload. Kafka treats it as an opaque byte array; nothing
  about its structure is enforced by the broker itself (see
  `docs/serialization/`, a later WP, for how schema governance is bolted on
  top of this fact rather than being a broker feature).
- **Headers** — optional key/value metadata attached to the record,
  separate from the value, often used for cross-cutting concerns like
  tracing IDs.
- **Timestamp** — either set by the producer or by the broker on receipt,
  depending on the topic's timestamp configuration; not necessarily "when
  the business event occurred."
- **Partition** — which partition of the topic the record was written to
  (chosen by the producer's partitioner, or explicitly specified).
- **Offset** — assigned by the broker, not the application; see below.

Some of these fields are supplied by your application (key, value,
optionally headers and an explicit partition); some are assigned by Kafka
itself (offset, and timestamp in the common default configuration). Knowing
which is which matters the first time a record shows up with a partition or
offset you didn't ask for.

## Offset

```text
topic = orders
partition = 0

offset 0
offset 1
offset 2
...
```

An **offset** is a record's position within one specific partition —
nothing more. It is assigned by the broker, sequentially, starting at 0,
and is only ever unique and ordered *within that partition*. This is
precise enough to be worth stating as a rule: the true logical location of
any record is the triple

```text
(topic, partition, offset)
```

not the offset alone. A topic with 20 partitions has twenty different
"offset 100"s, one per partition, referring to twenty unrelated records —
the lab's Principal Engineer questions ask you to work through exactly this
directly.

## Producer

A **producer** is a client that appends records to topic partitions. It
decides — via its partitioner — which partition each record without an
explicit partition goes to, and it interacts with the broker leading that
partition to get the record appended and (depending on its configuration)
acknowledged. The full internal path a produced record takes is the subject
of [`docs/architecture/KAFKA_MENTAL_MODEL.md`](../architecture/KAFKA_MENTAL_MODEL.md)
and, in far more depth, `docs/producer/` (a later WP). In this lab, the
"producer" is the `kafka-console-producer.sh` CLI tool rather than
application code, which is deliberate: it lets you see the effect of
producing without yet writing or debugging a Java client.

## Consumer

A **consumer** retrieves records by **pulling** them from the broker —
issuing fetch requests for records at and after a given offset on each
partition it is assigned — rather than having records pushed to it. This is
why a slow consumer does not cause the broker to buffer anything extra on
its behalf: the broker simply answers whatever offset it's asked for, from
the log it already maintains for every consumer. The cost of this design is
that "how far behind is this consumer" (lag) becomes something you must
actively monitor rather than something the system prevents.

## Consumer group

A **consumer group** is a named set of consumers that divide up a topic's
partitions among themselves, so that each partition is actively consumed by
at most one member of the group at a time. Consumer groups are introduced
here only enough to make Experiment 7 legible — group membership, the
coordinator, and rebalancing are the subject of `docs/consumer-groups/` and
a dedicated failure lab (WP-05).

## Retention

Kafka does not delete a record because it was read. A partition is a log,
and consuming a record only ever advances a *consumer's* position in that
log — it never mutates the log itself. Records are removed only by the
topic's own **retention** policy (by age, by size, or — for compacted
topics, a later-WP topic — by key). This is the property that makes replay
(Experiment 5) possible at all, and it is a real behavioral difference from
a destructive queue, where a consumed message is typically gone.

## Ordering

Ordering in Kafka is a partition-level guarantee, not a topic-level one, for
the same reason a partition is the unit of both appending and fetching (see
Partition, above). If you need strict ordering among a set of related
records, they must land in the same partition — usually by giving them the
same key — and a topic with only one partition trivially orders everything,
at the cost of that topic having no read parallelism.

## Durability

"Kafka is durable" is not, by itself, a complete claim. Durability in Kafka
is a function of several independent choices working together: whether a
record was actually written to disk before being acknowledged, how many
replicas had to acknowledge it, and whether the storage holding those
replicas survives whatever failure occurs. This lab's single-node cluster
demonstrates only one dimension of that: that data written to a Docker
volume survives the *container* being stopped, removed, and recreated
(Experiment 9), and that deleting the volume itself removes the data
(Experiment 10).

**This single-node lab is not highly available, and it is not a production
durability story.** With one broker, replication factor is forced to 1 —
there is no second copy of any record anywhere. If the underlying disk (or,
here, the Docker volume) is lost, the data is lost, full stop; nothing about
"Kafka is durable" changes that arithmetic when there is only one copy.
Real durability — replication, the in-sync replica set, `acks`, and
`min.insync.replicas` working together — is the subject of
`docs/replication/` and the broker-failure lab (WP-06), once there is more
than one broker for those mechanisms to mean anything.

## Where this leaves you

You now have the vocabulary Lab 01 uses without re-defining it inline. The
lab's job is to turn each of these terms into something you have personally
produced, inspected, broken, and recovered — which is a different, and much
more durable, kind of understanding than having read the definition once.
