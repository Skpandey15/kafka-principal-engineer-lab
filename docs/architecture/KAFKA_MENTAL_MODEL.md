# The Kafka Mental Model

**Question this document answers:** what exactly happens between
`producer.send(record)` and a consumer receiving that record?

This is a conceptual walkthrough, not an API tutorial. Every subsequent document in
`docs/producer/`, `docs/consumer/`, `docs/replication/`, and `docs/storage/` expands
one stage of this walkthrough in depth, with Java implementations and failure
experiments. Read this first so you have a map before you get the detail.

## Why start here instead of with the API

Most Kafka tutorials start with `KafkaProducer<K, V>` and a `send()` call and treat
everything past that line as "the network." That framing produces engineers who can
write a producer but cannot explain a latency spike, a duplicate message, or a lost
record. Kafka's guarantees — ordering, durability, delivery semantics — are all
consequences of what happens in the steps below. If you understand this path, you
can derive most of Kafka's configuration surface instead of memorizing it.

## The end-to-end path

```mermaid
flowchart TB
    A[Application code] --> B["KafkaProducer.send(record)"]
    B --> C[Interceptors]
    C --> D[Serializer]
    D --> E[Partitioner]
    E --> F[RecordAccumulator]
    F --> G["Batch (per partition)"]
    G --> H["Sender thread"]
    H --> I["Network client"]
    I --> J["Broker: partition leader"]
    J --> K["Append to log segment"]
    K --> L["Replicate to followers (ISR)"]
    L --> M{"acks satisfied?"}
    M -->|yes| N["Acknowledgement to producer"]
    M -->|no / timeout| O["Retry or fail"]
    K --> P["Consumer fetch request"]
    P --> Q["Deserializer"]
    Q --> R["Application code (consumer)"]
```

### 1. Application calls `send()`

The call is **asynchronous**. It returns a `Future<RecordMetadata>` (or invokes a
callback) immediately; it does not wait for the broker. This single fact explains
why producer throughput can be high even though each record eventually requires a
network round trip: the application is not blocked per-record. See
`docs/producer/` for `Future` vs. callback usage and why blocking on every `send()`
destroys throughput.

### 2. Interceptors and serialization

Before anything is sent, any configured `ProducerInterceptor`s run, and then the
key and value are converted from Java objects to bytes by the configured
serializers. Kafka itself only ever stores and moves bytes — it has no idea what a
"customer" or an "order" is. This is why schema governance (`docs/schema-registry/`)
is a separate, explicit concern: nothing in the broker enforces a schema unless you
build that enforcement into the serialization layer.

### 3. Partitioning

The partitioner decides which partition of the topic the record goes to. If a key
is present, the default partitioner hashes it — the same key always maps to the
same partition, which is what makes per-key ordering possible. If no key is
present, records are distributed across partitions (the "sticky" partitioner
batches several keyless records onto the same partition before moving on, to
improve batching efficiency). See `docs/partitioning/` for why this single decision
determines your ordering guarantees, your scalability ceiling, and your exposure to
hot partitions.

### 4. Batching in the `RecordAccumulator`

The record is not sent immediately. It is appended to an in-memory batch specific
to its destination partition, buffered in the `RecordAccumulator`. A batch is sent
when it fills up (`batch.size`) or after a bounded wait (`linger.ms`), whichever
happens first. This is a deliberate latency-for-throughput trade: a small
`linger.ms` minimizes per-record latency; a larger one lets more records
accumulate into fewer, larger, more efficient (and more compressible) requests.
See `docs/producer/` for the full batching and `buffer.memory` back-pressure story.

### 5. The `Sender` thread and the network

A background `Sender` thread drains ready batches and sends them to the broker
that leads each partition. The producer maintains its own view of cluster
metadata (which broker leads which partition) and refreshes it as leadership
changes. This is why a producer can keep working across a leader election without
the application code knowing anything happened.

### 6. The broker appends to the partition leader's log

Every partition is an **append-only log** — records are only ever added at the
end, never edited in place. The leader broker appends the batch to its local log
segment (`docs/storage/`). This append is a sequential disk write, which is a large
part of why Kafka achieves throughput that random-access systems cannot: sequential
I/O is dramatically faster than random I/O on both spinning disks and, to a lesser
extent, SSDs, and the OS page cache absorbs most of the read traffic for recent
data.

### 7. Replication to the ISR

The partition's follower replicas fetch newly appended records from the leader,
the same way a consumer would. A follower that is sufficiently caught up is a
member of the **in-sync replica set (ISR)**. Durability is a function of how many
ISR members must acknowledge a write before it is considered committed —
controlled by the producer's `acks` setting together with the topic's
`min.insync.replicas`. See `docs/replication/` for exactly how the high watermark,
ISR membership, and `acks` interact, and what happens to each combination when a
broker dies mid-write.

### 8. Acknowledgement (or retry, or failure)

Once the durability condition implied by `acks` is met, the broker responds and
the producer's `Future`/callback completes successfully. If the condition is not
met in time, the producer retries (subject to `retries`, `retry.backoff.ms`, and
`delivery.timeout.ms`) or ultimately fails the record. Whether retries can
introduce duplicates — and how idempotent producers and transactions prevent
that — is covered in `docs/delivery-semantics/`.

### 9. The consumer fetches, not receives

Kafka does not push records to consumers. A `KafkaConsumer` **polls**, issuing
fetch requests for records after a given offset on each partition it is assigned.
This pull model is what allows a slow consumer to fall behind without the broker
needing to buffer per-consumer state — the broker just serves whatever offset is
requested, from the log it already has. The trade-off is that consumer lag
becomes a first-class, must-monitor concept: nothing stops a slow consumer from
falling arbitrarily far behind except retention. See `docs/consumer/`.

### 10. Offsets, commits, and "receiving"

A record is not "received" in any strong sense until the consumer has processed
it and **committed** the offset past it — and even that guarantee depends on
whether the commit happens before or after processing, and whether it is
synchronous. This single design point is the entire reason at-least-once,
at-most-once, and effectively-once are three different, chooseable outcomes
rather than one fixed behavior. See `docs/delivery-semantics/`.

## What this model does and does not guarantee

Understanding the path above lets you derive the following without memorizing
them as separate facts:

- **Ordering** is only guaranteed within a partition, because a partition is the
  unit of both appending and fetching. Ordering across partitions is not
  meaningful — there is no global sequence.
- **Durability** is a spectrum controlled by `acks` and `min.insync.replicas`,
  not a boolean. "Kafka is durable" is not a complete sentence.
- **"Exactly-once"** describes what happens when idempotent producers and
  transactions eliminate the specific duplication windows created by retries and
  by the separation between processing and committing — it is not a property
  Kafka has by default.
- **Throughput** comes from batching, sequential I/O, and the pull model, all of
  which trade some latency and some client-side complexity to get there.

## Where to go next

- [`docs/roadmap/KAFKA_ZERO_TO_PRINCIPAL_ENGINEER.md`](../roadmap/KAFKA_ZERO_TO_PRINCIPAL_ENGINEER.md)
  for the full curriculum this document is the entry point to.
- `docs/producer/` (added in a later work package) for the producer internals in
  full depth, including the exact configuration knobs at each stage above.
- `docs/consumer/` and `docs/consumer-groups/` for the consumer side, including
  rebalancing and lag.
- `docs/storage/` for why the log/segment/index design makes Kafka fast.
- `docs/replication/` and `docs/kraft/` for how the cluster stays durable and
  agrees on metadata without a single point of failure.
