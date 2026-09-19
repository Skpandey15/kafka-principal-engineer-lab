# Partitioning and Ordering

This document is the conceptual and Principal Engineer depth behind
[`labs/lab-03-partitioning-ordering`](../../labs/lab-03-partitioning-ordering/README.md).
It exists to answer one question precisely, with evidence:

> **How do I choose a Kafka partitioning strategy that preserves the
> ordering my business requires without creating hot partitions or
> unnecessarily limiting scalability?**

A Kafka partition is not "something Kafka uses for parallelism." The
partition key you choose determines record placement, which determines
ordering scope, which determines consumer parallelism, which determines
load distribution, which determines hot-partition risk, which determines
capacity, which determines how painful a future topology change will be.
Partitioning is a business architecture decision wearing a configuration
knob's clothing.

Every technical claim below was verified against the actual
`kafka-clients:4.3.1` source (downloaded from Maven Central and read
directly, not assumed from an older version's documentation) before being
written down, and every number is a real result captured while building
and validating the lab — see each section for exactly what was run.

## The partition mental model

```text
Topic
 |
 +-- Partition 0
 |     offset 0
 |     offset 1
 |     offset 2
 |
 +-- Partition 1
 |     offset 0
 |     offset 1
 |
 +-- Partition 2
       offset 0
       offset 1
```

Offsets are local to a partition. There is no topic-wide offset sequence
— `(topic, partition, offset)` is what identifies a record's location,
never `(topic, offset)` alone. This is the same fact
[`docs/fundamentals/KAFKA_CLUSTER_FUNDAMENTALS.md`](../fundamentals/KAFKA_CLUSTER_FUNDAMENTALS.md)
established for the CLI; this document is where it becomes a Java-client,
architecture-level concern.

## Partition selection: public semantic → current implementation → version caveat

**Public semantic** (stable across versions, safe to build a mental model
on): a record with a key is routed to a partition deterministically and
consistently, based on that key, for as long as the topic's partition
count doesn't change. A record without a key has no such affinity. An
explicitly-addressed record goes exactly where it's told.

**Current implementation, verified against `kafka-clients:4.3.1` source**
(`KafkaProducer.partition(...)`, `org.apache.kafka.clients.producer.internals.BuiltInPartitioner`):

```java
// KafkaProducer.partition(...) — the actual decision order:
if (record.partition() != null) return record.partition();               // explicit wins, always
if (partitionerPlugin.get() != null) return customPartitioner.partition(...); // custom partitioner, if configured
if (serializedKey != null && !partitionerIgnoreKeys) {
    return BuiltInPartitioner.partitionForKey(serializedKey, currentPartitionCount);
}
return RecordMetadata.UNKNOWN_PARTITION; // decided later, adaptively — see "Null keys" below
```

For a **keyed** record, `BuiltInPartitioner.partitionForKey` is exactly:

```java
Utils.toPositive(Utils.murmur2(serializedKeyBytes)) % numPartitions
```

Murmur2, applied to the **serialized key bytes**, modulo the **current**
partition count for that topic. Three consequences follow directly from
reading that one line, not from folklore:

1. **Same key, same bytes, same partition count → same partition, every
   time.** This is what "affinity" means mechanically.
2. **The modulus is the topic's CURRENT partition count, read fresh at
   send time.** If that count changes, the result can change for the
   exact same key — see "Increasing partitions" below.
3. **Hashing operates on bytes, not on your Java object.** See "Key
   serialization" below.

**Version caveat:** there is no `DefaultPartitioner` class in
`kafka-clients:4.3.1` — a repository-wide check of the published sources
jar confirms it does not exist as a separate file. Its logic was folded
into `BuiltInPartitioner`, as part of KIP-794's adaptive/sticky
partitioning work. Do not go looking for `DefaultPartitioner` in current
source, and do not trust an explanation of it from an older tutorial as a
description of 4.3.1's actual behavior — this document's claims came from
reading the 4.3.1 source directly, not from that lineage.

## Key serialization matters

```text
Java key
   ↓
Serializer
   ↓
byte[]
   ↓
BuiltInPartitioner.partitionForKey(bytes, numPartitions)
   ↓
partition
```

Partition selection operates on **serialized bytes**, not on your key
object's `equals()`/`hashCode()`. Two producers that represent "the same"
logical key differently at the byte level — a different `String` encoding,
a different number formatting, a schema change that alters field order or
representation — can hash to different partitions for what a human would
call the same key. This is exactly why changing how a key is serialized
(including future schema changes, once WP-10 introduces Schema Registry)
has partitioning consequences, not just a data-format concern. This
document does not go further into schema mechanics — that's WP-10's job —
but the causal link (serialization changes key bytes changes hash changes
partition) is established here because it follows directly from the
`partitionForKey` signature above.

## Ordering guarantee — precise wording

> Kafka provides ordering **within a partition**, subject to the relevant
> producer/delivery semantics (in-order delivery from a single producer to
> a single partition; more nuanced under retries without idempotence,
> which is WP-09's topic).

Do not write, or believe, "Kafka guarantees message ordering" as a
complete sentence — it is not one.
[`labs/lab-03-partitioning-ordering`](../../labs/lab-03-partitioning-ordering/README.md)'s
`BusinessOrderingApp` demonstrates this directly: producing order-lifecycle
events for three orders and reading them back showed every single order's
four events in perfect order **within their partition**, while the raw,
as-consumed order across the whole topic was an arbitrary interleaving —
`order-1003`'s entire sequence appeared before `order-1001`'s in one real
run, simply because of which partition `poll()` happened to return first.
Neither order is "wrong"; there is no topic-wide order to be right or
wrong about.

## Business ordering is something you build, not something Kafka gives you

Kafka does not know what an "order" is. It sees a topic name, a partition
number, and bytes. Business ordering — "these four events for
`order-1001` must be seen in sequence" — exists only because a producer
chose a partition key (here, the order id) that keeps one order's events
on one partition, for as long as the topic's partition count is stable.
Change the key strategy, or the partition count, and that property can
silently stop holding. This is why partitioning is a business
architecture decision: the guarantee your consumer relies on is
constructed by a producer-side choice, not enforced by Kafka independent
of that choice.

## Null keys: measured, not assumed

Verified against `kafka-clients:4.3.1`: a null-key record does **not**
round-robin. `KafkaProducer.partition(...)` returns
`RecordMetadata.UNKNOWN_PARTITION` for it, deferring the actual choice to
`BuiltInPartitioner`, inside `RecordAccumulator`, which implements
KIP-794's **adaptive sticky partitioning**:

- Records without partition affinity are "stuck" to one partition at a
  time, switching after enough bytes accumulate for an efficient batch
  (`nextPartition(Cluster)` in `BuiltInPartitioner.java`), not per-record.
- Which partition to switch *to* is either uniform-random among currently
  available partitions, or — once the client has observed per-partition
  load statistics, and with `partitioner.adaptive.partitioning.enable`
  at its default of `true` — weighted toward less-loaded partitions.

This is genuinely adaptive, load-aware behavior, not a fixed distribution
rule. `NullKeyDistributionApp`'s real, captured result (10,000 null-key
records, async sends, 6-partition topic) makes the point sharply:

```text
Partition  Records    Percentage
0          1338        13.38%
1          1384        13.84%
2          0            0.00%
3          2088        20.88%
4          0            0.00%
5          5190        51.90%

Max/Avg     3.11
```

Two partitions got nothing at all, and one got over half the traffic, in
one real run. **Do not describe null-key behavior as round-robin, and do
not assume it produces even distribution either.** The durable lesson is
narrower and more defensible than any specific distribution claim:

```text
keyed records    -> partition affinity, deterministic
null-key records -> no business-key affinity guarantee, distribution is
                     an adaptive implementation detail, not an API contract
```

## Explicit partition and custom partitioners

**Explicit partition** (`new ProducerRecord<>(topic, partition, key, value)`)
bypasses every other mechanism — checked first, before even a configured
custom partitioner. Powerful, and usually the wrong default for business
code: it couples the application to the topic's current layout, doesn't
adapt when partition count changes, and is an easy way to manufacture a
hot partition (see `labs/lab-03-partitioning-ordering`'s
`ProducerExplicitPartitionApp`, inherited from WP-03, plus that lab's own
comparison table below).

**Custom partitioners** (`partitioner.class`) let you express routing
logic Kafka's hash-based default cannot — `labs/lab-03-partitioning-ordering`'s
`CustomPartitionerDemoApp` and `FirstLetterPartitioner` route by a key's
first letter, something `murmur2(key) % partitions` has no way to encode.
The `Partitioner` interface itself (verified against 4.3.1) is small:

```java
public interface Partitioner extends Configurable, Closeable {
    int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster);
    void close();
}
```

(Older Kafka versions' `Partitioner` interface included an `onNewBatch`
callback for sticky-partition bookkeeping; that hook does not exist in
4.3.1's interface — stickiness is now handled entirely inside
`BuiltInPartitioner`, not by the pluggable interface. Verify this yourself
against whatever version you're actually running before assuming either
shape.)

The real cost is not implementation difficulty — the demo class is under
40 lines — it's that a custom partitioner becomes a **long-lived
compatibility contract** the moment any data depends on it:

- Every producer application writing to the topic must use the exact same
  routing logic, or different producers disagree about where "the same"
  logical key goes.
- A change to the routing logic is a coordinated migration across every
  producer, with no built-in versioning mechanism.
- It has exactly the same partition-count-change remapping behavior as
  the built-in partitioner — it does not exempt you from "Increasing
  partitions," below.
- A hand-written rule can create skew just as easily as a bad key choice
  (an alphabet split is not obviously uniform).

Do not reach for a custom partitioner unless a specific business
requirement — not convenience — justifies owning that contract.

## Cardinality, distribution, and skew

**Good cardinality does not mean mathematically perfect balance — it gives
the partitioner enough distinct key values to distribute load across
partitions at all.** `GoodCardinalityDistributionApp`'s real result
(10,000 records, 10,000 distinct order-id keys, 6-partition topic):

```text
Partition  Records    Percentage
0          1700        17.00%
1          1699        16.99%
2          1619        16.19%
3          1663        16.63%
4          1687        16.87%
5          1632        16.32%

Max/Avg     1.02
```

Close to even, not perfectly even — `1.02` means the hottest partition in
this sample had about 2% more than the sample average, which is a
reasonable outcome for a well-chosen key, not a mathematical guarantee.

**High cardinality alone does not guarantee low skew.** A key space with
millions of possible values can still be dominated by one or a few of
them in practice — see "Hot keys," next. Evaluate cardinality **together
with** the actual frequency distribution and the ordering requirement,
never cardinality in isolation.

**Low cardinality caps how many partitions can ever be used, regardless of
partition count.** `LowCardinalityDistributionApp`'s real result (10,000
records, key = country, only `IN`/`US`/`UK`, against a **12-partition**
topic):

```text
Partition  Records    Percentage
0          3318        33.18%
1          3277        32.77%
2..7,9..11 0            0.00%  (9 partitions total)
8          3405        34.05%

Max/Avg     4.09
```

**3 of 12 partitions received any traffic at all.** The other 9 were
provisioned, paying whatever operational cost partitions carry, and doing
nothing. The lesson:

```text
many partitions + low-cardinality key != parallelism
```

## Hot keys and hot partitions

A **hot key** is a single key value that dominates traffic even though the
overall key space might otherwise look fine. Because a keyed record maps
to exactly one partition, a hot key mechanically becomes a **hot
partition** — this doesn't require a bug or a misconfiguration, it follows
directly from the hashing rule above. `HotKeyDistributionApp`'s real
result (10,000 records, 90% targeted at a single `customer-VIP` key, 10%
spread across ~5,000 distinct normal-customer keys, 6-partition topic):

```text
Records by key kind:
  normal (customer-normal-*)     1052 (10.5%)
  hot (customer-VIP)             8948 (89.5%)

Partition  Records    Percentage
0          165          1.65%
1          185          1.85%
2          160          1.60%
3          197          1.97%
4          192          1.92%
5          9101        91.01%

Max/Avg     5.46
```

One partition carried 91% of all traffic. Every one of the other five
partitions, and every broker that isn't leading partition 5, can be
nearly idle at the same time.

**This can happen on a completely healthy cluster.** No broker crashed, no
disk filled, no network partitioned. The problem is architectural, not
operational:

```text
cluster healthy + bad key distribution = uneven capacity / bottleneck
```

**Diagnostic mindset** (see
[`docs/roadmap/PRINCIPAL_ENGINEER_FAILURE_MATRIX.md`](../roadmap/PRINCIPAL_ENGINEER_FAILURE_MATRIX.md)
for where this sits in the repository's broader failure catalogue):

```text
high consumer lag
      ↓
check partition-level lag (not just the topic-level average — see below)
      ↓
check throughput by partition
      ↓
check key distribution
      ↓
identify hot key / hot partition
```

**A topic-level average can hide a partition-level hotspot.** If P0 and P1
each carry 5 MB/s and P2 carries 80 MB/s, the topic-level average
throughput looks unremarkable while one broker is doing most of the real
work. Always look one level below the topic-wide number before concluding
the cluster is (or isn't) the problem.

**Adding a broker does not, by itself, fix a hot key.** A hot key still
maps to one partition; that partition still has one leader; moving that
leader to a different (now-idle) broker moves the bottleneck, it doesn't
remove it. Fixing this requires addressing the key strategy itself — key
salting (below) or a redesigned key — not adding capacity elsewhere.

## Partition count: what it actually controls

More partitions can enable more producer distribution, more consumer
parallelism, and more aggregate throughput. They also cost real things:

```text
more partitions
      ↓
more broker-side metadata
      ↓
more open file handles
      ↓
more replication work (once replication exists — a later WP)
      ↓
more leader-management and controller overhead
      ↓
longer recovery/rebalance work after a broker or controller change
      ↓
more operational surface area generally
```

**"More partitions are always better" is false.** So is its cousin, "if
you need more throughput, just add partitions" — see the next section for
why that advice is operationally easy but semantically significant enough
to deserve its own Principal Engineer discussion.

### Consumer parallelism

```text
maximum active consumers usefully working in one consumer group
≈
number of partitions on the topic(s) that group is assigned
```

`labs/lab-03-partitioning-ordering`'s `ConsumerParallelismApp`, run as five
real instances (`consumer-a` through `consumer-e`) in one group against a
**3-partition** topic, produced exactly the textbook result:

```text
consumer-a: assigned partitions [orders-lifecycle-0]
consumer-b: assigned partitions [orders-lifecycle-1]
consumer-c: assigned partitions [orders-lifecycle-2]
consumer-d: assignment is EMPTY -- idle, no partitions owned.
consumer-e: assignment is EMPTY -- idle, no partitions owned.
```

Partition count is a **ceiling**, not a target — two of five consumer
processes were fully provisioned, running, and doing nothing, because
there was no partition left to give them. **Not**, however:
"the number of consumers should always equal the number of partitions" —
fewer consumers than partitions is completely normal (each just owns
more than one partition); the ceiling only bites when consumers exceed
partitions. Exactly *how* that assignment is decided, and what happens to
it under a rebalance, is WP-05's territory — this document establishes
only the ceiling itself.

## Increasing partitions is not simple autoscaling

The common advice "just add partitions for more throughput" undersells
what actually happens. Because a keyed record's partition is
`murmur2(keyBytes) % currentPartitionCount`, **changing the partition
count changes the modulus**, which can change the result for the exact
same key.

### The experiment, run for real

Against `partition-expansion-demo` (created with 3 partitions), 30
deterministic keys (`order-000001` .. `order-000030`) were produced and
their partitions captured. The topic was then expanded to 6 partitions via
`kafka-topics.sh --alter --partitions 6`, and the **same 30 keys** were
produced again:

```text
Key            Before     After      Remapped?
order-000001   2          5          YES
order-000002   2          5          YES
order-000003   2          5          YES
order-000004   1          1          no
order-000005   0          0          no
order-000007   0          3          YES
order-000023   1          4          YES
... (30 keys total)

14 of 30 keys remapped to a different partition after the topology change.
```

**14 of 30 — nearly half — mapped to a different partition for identical
keys**, produced identically, purely because the topic's partition count
changed underneath them.

### A precise, checkable consequence of doubling

Because `3 -> 6` is an exact doubling, the doubled modulus produces a
mathematically constrained result: for any key, its new partition must be
either its old partition, or its old partition plus the old partition
count (`oldP` or `oldP + 3` here) — never anything else. This isn't a
hand-wave; it follows from `h % 6` and `h % 3` sharing the same value
modulo 3. The real run above confirms it: `order-000001` moved `2 -> 5`
(`2 + 3`), `order-000007` moved `0 -> 3` (`0 + 3`), `order-000023` moved
`1 -> 4` (`1 + 3`) — every single observed remap in this run fit that
pattern (the lab's `PartitionCountChangeApp` checks and prints this
explicitly, rather than only asserting it in prose). This is a genuinely
useful diagnostic fact when partition count changes are exact multiples;
it does not hold in general for arbitrary partition-count changes (e.g.
`3 -> 5`), where the relationship between old and new partition is not
constrained this cleanly.

### What did and didn't happen

- **Existing, already-written records did not move.** Nothing in this
  client, or in Kafka's expansion mechanism, rewrites or relocates
  previously-appended data when partition count increases.
- **Only where *future* records for a given key land changed.**
- **Kafka did not "rebalance" old data across the new partitions.**
  Historical events for `order-000001` are still sitting in the old
  partition 2's log; new `order-000001` events now land in partition 5.
  One business key's history can now be split across two partitions.
- Per-partition ordering still holds — Kafka never breaks that guarantee.
  What breaks is any **application-level** assumption that "all of this
  business key's history is in one place." If a consumer maintains
  per-key state by reading one partition, or if downstream tooling
  assumes one key lives in one partition, this topology change is exactly
  the kind of event that quietly invalidates that assumption.

**Partition count is not the same kind of thing as a Kubernetes replica
count.** Application replicas are elastic, interchangeable, and safely
scaled up or down based on load; Kafka's partition topology is
**architectural state** with a real, one-directional consequence
(existing key→partition affinity for future records) attached to changing
it. Do not wire partition count to autoscale on CPU the way you would
application replicas — this is not what KRaft, or this client, or any
version of Kafka referenced in this repository, treats as safe to do
casually.

## Key salting

**Salting** splits one hot key into several, spreading its traffic across
multiple partitions:

```text
merchant-123
   ↓ (salted into N buckets)
merchant-123#0
merchant-123#1
merchant-123#2
```

Each salted variant hashes independently, so `merchant-123`'s traffic
that used to concentrate on one partition now spreads across (up to) N.
This directly mitigates the hot-partition problem `HotKeyDistributionApp`
demonstrated.

**It also destroys simple total ordering for that key.** A consumer that
needs to see all of `merchant-123`'s events in one sequence now has to
read from N partitions and merge — exactly the ordering guarantee
partitioning by `merchant-123` alone was providing. The trade-off is
explicit:

```text
more parallelism  <->  weaker/simpler ordering
```

This lab does not implement a salting framework — the trade-off is the
point, not the mechanism. Choose salting only when you've confirmed the
consumer side doesn't actually need per-entity total order (or is willing
to reconstruct it downstream), the same reasoning "Ordering vs.
throughput" below generalizes.

## Ordering vs. throughput

```text
1 partition        -> simple total ordering, hard parallelism ceiling
many partitions    -> real parallelism, only partition-local ordering
```

The Principal Engineer question this document keeps returning to:

> **What is the smallest business entity for which strict ordering is
> actually required?**

Don't pay the scalability cost of global (single-partition) ordering if
the business only ever needs per-order, per-account, or per-entity
ordering — partition by that entity and get real parallelism across
entities for free. Conversely, don't assume per-entity partitioning
solves every ordering need if the business actually requires cross-entity
ordering (rare, and expensive when real).

## Business partition-key design

### E-commerce example

For an order-events topic, compare candidate keys without declaring a
universal winner — the right answer depends on the invariant being
protected:

| Option | Required ordering | Cardinality | Skew risk | Parallelism | Notes |
|---|---|---|---|---|---|
| `orderId` | Per-order lifecycle events stay in order | Very high (one value per order) | Low, if orders are roughly uniform in event count | High | Natural fit if downstream logic is per-order |
| `customerId` | Per-customer event history stays in order | High, but see "high-cardinality ≠ good" below | Real — some customers order far more than others | Medium-high | Needed only if downstream state is per-customer, not per-order |
| `merchantId` | Per-merchant ordering | Medium — bounded by merchant count | High for large platforms (a few merchants dominate volume) | Low-medium, bounded by distinct merchants | Risks hot merchants exactly like the hot-key experiment |
| `country` | Rarely a real ordering requirement | Very low | Severe (`LowCardinalityDistributionApp`'s real result) | Very low, capped at the number of countries | Almost never the right key on its own |

### Financial example

For `AccountDebited` / `AccountCredited` / `BalanceAdjusted` events,
candidates include `transactionId`, `accountId`, and `customerId`.
`accountId` is the strongest candidate **if** the consumer maintains
account-level running state (a balance) that depends on seeing that
account's events in order — `transactionId` would scatter one account's
events across many partitions, breaking exactly the guarantee that state
needs. But `accountId` reintroduces hot-account risk the same way
`customerId` or `merchantId` does above. This is the concrete shape of:

> **Correctness and scalability can pull partition-key design in opposite
> directions.**

There is no universal resolution — only a documented, evidence-based
decision for the specific invariant being protected (see the decision
framework below, and consider salting if a specific account is known to
be disproportionately hot).

### High cardinality ≠ automatically good

A key with millions of possible values can still produce severe skew if
real-world frequency is uneven — a `customerId` key where one customer
generates 40% of all traffic has "good" cardinality and "bad" balance at
the same time. Evaluate:

```text
cardinality + frequency distribution + ordering requirement
```

together. None of the three alone is sufficient.

## The strategy comparison

| Strategy | Benefit | Risk |
|---|---|---|
| Key-based default partitioning | Simple, scales with key cardinality | Entirely dependent on key quality (cardinality + distribution + ordering fit) |
| Explicit partition | Deterministic physical destination | Topology coupling; doesn't adapt to partition-count changes; easy hot-partition source |
| Custom partitioner | Expresses routing logic hashing cannot | Long-lived cross-producer compatibility contract; own testing/versioning/migration burden |
| Null key | Simple to write, adaptive load-aware distribution | No business-key affinity at all — related events may not stay together |

There is no universally best strategy. Every row's "Risk" column is a
question the next section's framework exists to force you to answer
before choosing.

## Principal Engineer decision framework

For any proposed partition key, work through all ten questions — not just
the ones that flatter the key you already want to use. This framework is
intended to be reused in future system-design work in this repository.

1. **Correctness** — what must be ordered together, exactly?
2. **Cardinality** — how many distinct key values will realistically exist?
3. **Distribution** — how evenly is real traffic spread across those values?
4. **Hot-key risk** — can one entity plausibly dominate traffic?
5. **Parallelism** — how many consumers must realistically process concurrently?
6. **Growth** — what throughput is expected in 1-3 years, not just today?
7. **Topology evolution** — what happens to this key's guarantees if partition count changes later?
8. **Downstream semantics** — does a consumer maintain per-entity state that depends on ordering?
9. **Recovery** — how does replay interact with whatever ordering this key provides?
10. **Cost** — what operational cost does the resulting partition count create?

## Principal Engineer review scenario

A team proposes:

```text
Topic: payments
Partitions: 3
Key: country
Traffic: IN = 70%, US = 20%, UK = 5%, Others = 5%
Expected traffic growth: 10x
```

Work through this yourself before reading further — the value is in the
reasoning, not a memorized verdict.

- **What's wrong?** `country` is a low-cardinality key with severe,
  observed skew (70% on one value) — this is structurally the same
  failure `LowCardinalityDistributionApp` measured, just with real
  numbers attached. At most 4 of however many partitions exist will ever
  receive traffic, and one of those 4 will dominate.
- **Will adding 30 partitions solve it?** No. `LowCardinalityDistributionApp`'s
  real result already demonstrates this precisely: more partitions with
  the same low-cardinality key just means more idle partitions, not more
  effective parallelism — 27 of the hypothetical 30 would sit empty.
- **What ordering requirement are we actually protecting?** This is the
  question the proposal never answers. If nothing downstream actually
  requires per-country ordering, `country` was never solving a real
  ordering problem — it was chosen for a reason unrelated to the ten
  questions above.
- **Would `paymentId` be better?** For parallelism, likely yes — very high
  cardinality, no obvious concentration. But only if no consumer needs
  per-payer or per-account ordering; if one does, `paymentId` scatters
  that entity's events across partitions, breaking it.
- **Would `accountId` be better?** Possibly, if downstream processing
  maintains per-account state (balances, running totals) — but this
  reintroduces hot-account risk symmetric to the financial example above,
  and needs its own frequency-distribution measurement before being
  approved, not an assumption.
- **What new risks would those choices create?** `paymentId` risk: loses
  any ordering guarantee narrower than "none." `accountId` risk: a large
  merchant or a very active account becomes the new hot key, the same
  shape of problem as `country`, just with a bigger denominator.
- **What measurements are needed before deciding?** The actual frequency
  distribution of the candidate key (not an assumption — measure it, the
  way every experiment in this document was measured, not guessed), the
  real ordering requirement from whoever owns the downstream consumer,
  and the realistic growth curve, before committing to a partition count
  or a key.

There is no simplistic universal answer here — that refusal to simplify
is itself the Principal Engineer lesson this scenario exists to teach.

## Source-reading track

> **Question:** where does Kafka decide which partition receives a
> `ProducerRecord`?

```text
Question
   ↓
Repository / module
   ↓
apache/kafka, clients module, org.apache.kafka.clients.producer
   ↓
Relevant class / method (verified against kafka-clients:4.3.1)
   ↓
KafkaProducer.send() -> doSend() -> serialize key/value -> private
partition(ProducerRecord, keyBytes, valueBytes, Cluster) -> in order:
explicit record.partition(), then a configured custom Partitioner, then
BuiltInPartitioner.partitionForKey(keyBytes, numPartitions) for a keyed
record, else RecordMetadata.UNKNOWN_PARTITION (resolved later, adaptively,
inside RecordAccumulator via BuiltInPartitioner.nextPartition(...))
   ↓
Call-path summary
   ↓
send() -> doSend() -> partition() decides the destination -> (whichever
branch applies) -> RecordAccumulator.append(...) with that decision, or
with UNKNOWN_PARTITION for the accumulator's own adaptive logic to resolve
   ↓
Relationship to observed lab behavior
   ↓
This is exactly why BusinessOrderingApp's same-key events land together,
why NullKeyDistributionApp's distribution is adaptive rather than
round-robin, and why PartitionCountChangeApp's remapped keys match
murmur2(key) % newCount precisely.
   ↓
Write down what you found
   ↓
(Your own notes, ideally against the actual apache/kafka source at the
tag matching 4.3.1 — ../producer/PRODUCER_INTERNALS_INTRO.md already
covers the send()/doSend()/RecordAccumulator hand-off in more general
depth; this exercise is specifically about the partition() decision
inside that path.)
```

## Production observability preview

Not built in this work package — a preview of what a real deployment
needs to actually see the failures this document describes, before they
become an incident:

- Records/sec and bytes/sec **by partition**, not just by topic.
- Consumer lag **by partition**.
- Produce and fetch latency.
- Broker disk and network utilization.
- Leader distribution across brokers.
- Business-key distribution, where it can be measured safely (without
  turning observability infrastructure into a privacy problem).

**A topic-level average can hide a partition-level hotspot** — this
document's own hot-key numbers make that concrete: an average across six
partitions where one carries 91% of traffic tells you almost nothing
useful on its own.

## Local lab vs. production

| Lab | Production |
|---|---|
| A few thousand to ten thousand events per experiment | Millions to billions of events |
| A handful of partitions, created by hand | Capacity-planned partition topology |
| `String` keys | Governed domain keys, often with schema evolution concerns |
| A manual distribution report printed to a terminal | Metrics, dashboards, alerting |
| Single broker (WP-02's environment) | Replicated multi-broker cluster |
| Deliberately simple, single-dimension skew | Organic, multi-dimensional workload skew |
| Manual topic expansion via the CLI | Reviewed, planned topology change |
| No sensitive data | Real privacy/security constraints on what can even be measured |

Nothing in this lab claims to model production scale, production data
sensitivity, or production operational tooling — it models the
**mechanism**, precisely, so the mechanism is what you understand before
meeting it at scale.
