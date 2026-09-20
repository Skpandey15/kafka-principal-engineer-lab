# Transactions and Exactly-Once Semantics

## Scope note

This document covers WP-09: Kafka-native producer idempotence, Kafka
transactions, and exactly-once processing semantics WITHIN Kafka. It
deliberately stops at Kafka's own boundary. It does **not** cover:

- Schema Registry / schema evolution — WP-10
- Kafka Connect / CDC — WP-11
- The transactional outbox pattern — WP-12
- Retry/DLQ architecture — WP-13
- Kafka Streams — WP-14
- Spring Kafka — WP-15
- A full observability platform — WP-16

Sections 18-20 below introduce the dual-write problem and name the
patterns that solve it (idempotent consumer, transactional outbox, inbox,
CDC, business idempotency keys) precisely so the reader knows where the
boundary of THIS WP is — without implementing any of those patterns here.

## Environment

This lab reuses the WP-07 3-broker KRaft cluster
(`platform/kafka-cluster/`, real evidence in
[`docs/replication/REPLICATION_ISR_AND_BROKER_FAILURE.md`](../replication/REPLICATION_ISR_AND_BROKER_FAILURE.md))
rather than standing up a new one. That cluster already configures its
internal topics — including `__transaction_state`, the topic this WP's
transactions depend on — with replication factor 3 and
`min.insync.replicas=2`:

```
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
```

That is precisely what "a cluster configured appropriately for
transactional state" means in production terms: the transaction
coordinator's own bookkeeping is as durable as any other critical internal
topic, tolerant of one broker's loss without losing a transaction's
commit/abort decision. No new platform directory was created for this WP.

The lab's own **automated tests** run against a different, single-node
Testcontainers cluster (`TransactionsKafkaCluster`, replication factor 1)
for speed — every transactional-correctness property under test is a
property of the transaction protocol itself, not of replication factor.
See that class's Javadoc for the full reasoning. This mirrors the same
manual-vs-automated environment split WP-07 and WP-08 already established.

Kafka/client version throughout: **`apache/kafka:4.3.1`** (broker) and
**`kafka-clients:4.3.1`** (Java client) — the same pin every prior lab uses.
Every "modern client default" claim in this document was verified directly
against this jar's `ProducerConfig`/`ConsumerConfig` `ConfigDef`, not
copied from documentation written for an older version — see "Modern
client defaults, verified" below.

## 1. The duplicate problem — why retries are necessary and dangerous

```text
Producer
   │
   │ send record
   ▼
Broker
   │
   │ record persisted
   ▼
ACK lost / timeout
   │
   ▼
Producer retries
   │
   ▼
Broker receives record again
```

A producer that gives up after a single failed send is not reliable — a
transient network blip, a leader failover, or a slow GC pause can all make
an acknowledgment never arrive even though the broker successfully wrote
the record. Reliability REQUIRES retrying whenever the outcome is unknown.

But "unknown" is the operative word: `producer.send(record).get()` throwing
a `TimeoutException` tells the caller nothing about whether the broker
actually persisted the record before the connection dropped. Without
idempotence, retrying that unknown-outcome record is indistinguishable, to
the broker, from a legitimately new record — so a retry that happens to
follow a successful-but-unacknowledged write produces a genuine duplicate:

```text
Order-101
Order-101
```

**This lab does not fake a deterministic ACK-loss reproduction** — the
outcome (duplicate or not) genuinely depends on exactly when a real broker
dies relative to an in-flight request, and asserting a specific outcome in
an automated test would misrepresent that as guaranteed when it isn't. What
IS deterministic and reproducible is the MECHANISM:
[`DuplicateRiskProducerApp`](../../labs/lab-08-transactions-exactly-once/src/main/java/com/kafkalab/transactions/producer/DuplicateRiskProducerApp.java)
sends one record with `enable.idempotence=false`, `acks=1`, and Kafka's own
retry mechanism disabled (`retries=0`) so every retry visible in its output
is the APPLICATION's own decision, made explicitly because the outcome of
the previous attempt was unknown — exactly the reasoning real producer code
has to implement by hand when it turns idempotence off.

## 2. Idempotent producer — PID, epoch, sequence number

```text
Producer

PID = 42

Partition-0

seq=0
seq=1
seq=2
```

When idempotence is enabled, the broker assigns the producer a **Producer
ID (PID)** during `InitProducerId`, and the producer tags every record it
sends to a given partition with a strictly increasing **sequence number**,
starting at 0. The broker tracks the last-accepted `(PID, partition,
sequence)` per partition. If a retry arrives with a sequence number the
broker has already accepted:

```text
PID=42
Partition=0
Sequence=2

already accepted
      ↓
duplicate suppressed
```

...the broker returns the ORIGINAL record's metadata without writing
anything a second time. This closes exactly the gap Section 1 describes,
for a single partition, with zero application code — the client and broker
handle it below the `send()` API entirely.

**Real evidence, captured against the reused WP-07 cluster.** A 3-record
idempotent burst (`IdempotentProducerApp`, `enable.idempotence=true`, all 3
records sent to the same partition) followed by:

```
docker exec kafka-broker-1 /opt/kafka/bin/kafka-transactions.sh \
  --bootstrap-server kafka-broker-1:19092 \
  describe-producers --topic txn-lab-idempotent --partition 1
```

produced:

```
ProducerId  ProducerEpoch  LatestCoordinatorEpoch  LastSequence  LastTimestamp   CurrentTransactionStartOffset
6000        0              -1                      2             1789880852483   None
```

`LastSequence=2` for 3 records (sequence numbers 0, 1, 2) — real,
broker-reported confirmation that this producer's writes are tracked by
PID and sequence exactly as described above. `LatestCoordinatorEpoch=-1`
and `CurrentTransactionStartOffset=None` are exactly what a NON-transactional
idempotent producer looks like from this tool — idempotence alone never
talks to a transaction coordinator at all (see Section 3).

There is no public `KafkaProducer` API to read back a producer's own PID,
epoch, or sequence numbers from application code — they are protocol-level
details the client manages internally. `describe-producers` (a broker-side
admin tool) is the only honest source for this evidence, which is why
`IdempotentProducerApp` prints its configuration but not these values
itself.

## 3. Modern client defaults, verified

Verified directly against the pinned `kafka-clients:4.3.1` jar's
`ProducerConfig`/`ConsumerConfig` `ConfigDef` (not copied from
documentation written for an older client version):

| Config | Default in kafka-clients 4.3.1 | Note |
|---|---|---|
| `enable.idempotence` | **`true`** | Idempotence is ALREADY ON by default. A caller has to explicitly set it to `false` to get Section 1's duplicate-risk scenario. |
| `acks` | **`all`** | Also already the default, independent of idempotence. |
| `retries` | **`2147483647`** (`Integer.MAX_VALUE`) | Not a meaningful "retry count" in practice — bounded by `delivery.timeout.ms`, not by this number. |
| `max.in.flight.requests.per.connection` | **`5`** | Idempotence requires `<= 5` in this client version, NOT `<= 1` as documentation written for pre-2.5 Kafka often states — the broker can track and de-duplicate up to 5 concurrent in-flight sequence numbers per partition. |
| `transactional.id` | `null` (unset) | Idempotence does not require one; see Section 4. |
| `transaction.timeout.ms` | `60000` | The client's default transaction timeout — see the "unexpected behavior" note below. |
| `delivery.timeout.ms` | `120000` | |
| `request.timeout.ms` | `30000` | |
| `isolation.level` (consumer) | **`read_uncommitted`** | An ordinary `KafkaConsumer`, with NO configuration at all, WILL observe records from a transaction that later aborts. See Section 5. This surprises people who assume a transactional producer alone is enough. |

**Unexpected behavior found building this lab:** the test cluster's
broker-side `transaction.max.timeout.ms` must be **greater than or equal
to** the client's `transaction.timeout.ms` (60000 by default) or every
`initTransactions()` call fails outright with `KafkaException: Unexpected
error in InitProducerIdResponse; The transaction timeout is larger than
the maximum value allowed by the broker`. An earlier version of this lab's
test cluster set `transaction.max.timeout.ms=30000` to speed up test runs
and broke every single transactional test with exactly this error — fixed
by removing that override rather than fighting it. This is a real,
reproducible finding, not a hypothetical caveat.

## 4. Idempotence is not the same thing as a transaction

An idempotent producer prevents ONE partition from seeing a DUPLICATE of
ONE record. It says nothing about a group of writes succeeding or failing
together:

```text
Business operation:

write A
write B
write C
```

```text
A → success
B → success
C → failure
```

Idempotence can guarantee A and B each land exactly once. It does **not**
make `A + B + C` one atomic operation — nothing rolls A and B back because
C failed, and nothing prevents a reader from seeing A and B committed while
C never shows up at all. Getting that atomicity is what Kafka transactions
add on top of idempotence (every transactional producer IS idempotent —
`enable.idempotence` is forced `true` the moment `transactional.id` is set
— but not every idempotent producer is transactional).

## 5. Transactional producer — real API

```java
producer.initTransactions();
producer.beginTransaction();

producer.send(record1);
producer.send(record2);

producer.commitTransaction();
```

or:

```java
producer.abortTransaction();
```

`initTransactions()` registers the configured `transactional.id` with its
coordinator, **fences any earlier producer instance still using that same
id** (Section 9), and completes or aborts any transaction that earlier
instance left hanging — all three of those things happen inside this one
call, not as separate steps the caller manages.

## 6. Commit experiment — real, captured

`TransactionalProducerApp` (`-Paction=commit`) against the reused WP-07
cluster:

```
initTransactions() -- registers this transactional.id with its coordinator...
ProducerId set to 5000 with epoch 0
beginTransaction()
  sent (inside open transaction) | eventId=Order-100 | partition=2 | offset=0 -- NOT yet visible to read_committed
  sent (inside open transaction) | eventId=Order-101 | partition=2 | offset=1 -- NOT yet visible to read_committed
  sent (inside open transaction) | eventId=Order-102 | partition=1 | offset=3 -- NOT yet visible to read_committed
Pausing 8000ms before commit -- check a read_committed consumer now: these offsets exist on disk but are withheld.
commitTransaction()
```

A `read_committed` consumer started DURING the 8-second pause and run
afterward (`IsolationLevelConsumerApp`, real output):

```
isolation.level=read_committed | partition=2 | offset=0 | key=Order-100 | value=eventId=Order-100|customerId=CUSTOMER-TXN|amount=99.99
isolation.level=read_committed | partition=2 | offset=1 | key=Order-101 | value=eventId=Order-101|customerId=CUSTOMER-TXN|amount=99.99
isolation.level=read_committed | partition=1 | offset=3 | key=Order-102 | value=eventId=Order-102|customerId=CUSTOMER-TXN|amount=99.99
```

All three records became visible together, only after `commitTransaction()`
returned — not incrementally as each `send()` completed. This is the
visibility behavior Section 7 of the spec asks for, captured directly.

## 7. Abort experiment — real, captured

The same app, `-Paction=abort`, three different records (`Order-200`,
`Order-201`, `Order-202`):

```
beginTransaction()
  sent (inside open transaction) | eventId=Order-200 | partition=1 | offset=5
  sent (inside open transaction) | eventId=Order-201 | partition=0 | offset=0
  sent (inside open transaction) | eventId=Order-202 | partition=1 | offset=6
abortTransaction() -- the broker writes an ABORT marker...
```

The `read_committed` consumer run afterward (Section 8, full output below)
never surfaced any of these three records — not immediately, and not ever.
They exist on disk (real offsets were allocated: partition 1 offsets 5 and
6, partition 0 offset 0) but a `read_committed` reader skips straight past
them.

## 8. `read_uncommitted` vs `read_committed` — real, captured

Both isolation levels run against the SAME topic, after BOTH the Section 6
commit and the Section 7 abort had already happened:

**`read_committed`** (real output):
```
isolation.level=read_committed | partition=2 | offset=0 | key=Order-100 | ...
isolation.level=read_committed | partition=2 | offset=1 | key=Order-101 | ...
isolation.level=read_committed | partition=1 | offset=3 | key=Order-102 | ...
Done. isolation.level=read_committed consumed 6 record(s)  # (3 above + 3 unrelated pre-existing records on this topic)
```

**`read_uncommitted`** (real output, same topic, same point in time):
```
isolation.level=read_uncommitted | partition=0 | offset=0 | key=Order-201 | ...
isolation.level=read_uncommitted | partition=2 | offset=0 | key=Order-100 | ...
isolation.level=read_uncommitted | partition=2 | offset=1 | key=Order-101 | ...
isolation.level=read_uncommitted | partition=1 | offset=3 | key=Order-102 | ...
isolation.level=read_uncommitted | partition=1 | offset=5 | key=Order-200 | ...
isolation.level=read_uncommitted | partition=1 | offset=6 | key=Order-202 | ...
Done. isolation.level=read_uncommitted consumed 9 record(s)
```

`read_uncommitted` shows `Order-200`, `Order-201`, and `Order-202` —
the three records from the ABORTED transaction — with their real content,
which `read_committed` never surfaces at all. This is the exact contrast
Section 9 of the spec asks for:

```text
read_uncommitted
      ↓
can observe transactional records that later abort
```
```text
read_committed
      ↓
only exposes committed transactional records
```

...and it is why Section 3's finding (`isolation.level` defaults to
`read_uncommitted`) matters in practice: an application that adds
transactional producers without also setting `isolation.level=read_committed`
on its consumers gets NONE of the abort-safety a transaction is meant to
provide.

## 9. Atomic writes across partitions and topics — real, captured

```text
Transaction T1

orders-P0
payments-P1
audit-P2
```

`MultiPartitionTransactionApp` writes one logical event to THREE separate
topics (`txn-lab-orders`, `txn-lab-payments`, `txn-lab-audit`) inside one
transaction. Commit run (real output):

```
MultiPartitionTransactionApp | transactionalId=... | eventId=Order-500 | action=commit
Writing to 3 topics in ONE transaction: txn-lab-orders, txn-lab-payments, txn-lab-audit
  sent | topic=txn-lab-orders | partition=2 | offset=3
  sent | topic=txn-lab-payments | partition=2 | offset=0
  sent | topic=txn-lab-audit | partition=2 | offset=0
commitTransaction()
```

Abort run, different event (`Order-501`):

```
  sent | topic=txn-lab-orders | partition=0 | offset=2
  sent | topic=txn-lab-payments | partition=0 | offset=0
  sent | topic=txn-lab-audit | partition=0 | offset=0
abortTransaction()
```

Real `read_committed` console-consumer output on the payments and audit
topics afterward:

```
$ kafka-console-consumer.sh ... --topic txn-lab-payments --isolation-level read_committed
eventId=Order-500-PAYMENT|customerId=CUSTOMER-MULTI|amount=250.0

$ kafka-console-consumer.sh ... --topic txn-lab-audit --isolation-level read_committed
eventId=Order-500-AUDIT|customerId=CUSTOMER-MULTI|amount=250.0
```

Only the COMMITTED event's payment and audit records appear; the aborted
event (`Order-501-PAYMENT`, `Order-501-AUDIT`) never does, on either topic.

**What "atomic" means here, precisely.** It does NOT mean these three
writes land at the same offset, the same timestamp, or even the same
broker — they don't; each topic's partition-0 leader can be (and, above,
mostly was) a different broker. It means: no `read_committed` consumer of
ANY of these topics can ever observe this transaction's commit outcome for
one of them without being able to observe it for all of them, because the
transaction coordinator writes control (commit/abort) markers to every
partition the transaction touched, and a `read_committed` consumer
withholds each partition's records until ITS marker arrives.

## 10. The transaction coordinator and `__transaction_state`

```text
Transactional Producer
        │
        ▼
Transaction Coordinator
        │
        ▼
__transaction_state
        │
        ├── partitions involved
        ├── transaction state
        └── producer metadata
```

**Do not read this as "every transaction goes through ONE global
coordinator."** Every `transactional.id` hashes to a partition of
`__transaction_state`, and the broker LEADING that partition is that
transactional.id's coordinator — different transactional IDs are, in
general, coordinated by different brokers, in parallel. `__transaction_state`
itself is a normal, replicated, **compacted** internal topic, just like
`__consumer_offsets` — its job is to durably remember, per
`transactional.id`, the current producer ID, producer epoch, transaction
state, and which partitions this transaction has touched, so that if the
coordinator broker itself fails, a NEW leader for that
`__transaction_state` partition can pick up exactly where the failed one
left off (including safely completing or aborting any transaction that was
in flight).

Real, captured evidence from the reused WP-07 cluster:

```
$ kafka-topics.sh --describe --topic __transaction_state
Topic: __transaction_state  PartitionCount: 50  ReplicationFactor: 3
  Configs: compression.type=uncompressed,min.insync.replicas=2,cleanup.policy=compact,segment.bytes=104857600,...
```

```
$ kafka-transactions.sh describe --transactional-id txn-lab-producer-1
CoordinatorId  TransactionalId      ProducerId  ProducerEpoch  TransactionState  TransactionTimeoutMs  TransactionDurationMs
1              txn-lab-producer-1   5000        1              CompleteCommit    60000                 17260
```

**Unexpected behavior found building this lab:** the producer epoch shown
here is `1`, not `0` — even though only ONE producer instance ever used
this `transactional.id`, and the client-side log line during the SAME run
showed `ProducerId set to 5000 with epoch 0`. The epoch visibly incremented
across a single `beginTransaction()`/`commitTransaction()` cycle, within
the SAME producer instance — not only on a brand-new instance calling
`initTransactions()` (which is the case most tutorials describe). This
repository did not trace the exact broker-side protocol reason down to a
specific KIP, but the OBSERVED, repeatable fact is: do not assume a
producer epoch is stable for the lifetime of one producer instance in
current Kafka versions — treat "same instance" and "same epoch" as two
different claims.

## 11. Producer fencing — real, captured

```text
Old producer instance
       +
new producer instance
       +
same transactional.id

       ↓

only one must remain authoritative
```

Two `KafkaProducer` instances, same `transactional.id`. Instance 1 begins a
transaction and sends one record. Instance 2 then calls its OWN
`initTransactions()` — this deterministically fences instance 1's epoch,
regardless of timing. Instance 2 proceeds to commit its own transaction
successfully. Instance 1 then attempts a second send.

**The exception this repository actually observed — real, captured
against the live 3-broker cluster:**

```
producer1 send #2 threw: java.util.concurrent.ExecutionException
  caused by: org.apache.kafka.common.errors.InvalidProducerEpochException: Producer attempted to produce with an old epoch.

producer1 commitTransaction() threw: org.apache.kafka.common.errors.InvalidProducerEpochException:
  Producer with transactionalId 'fencing-probe-txn' and (producerId=6001, epoch=0) attempted to produce with an old epoch
```

This is **not** `ProducerFencedException`, which a lot of older Kafka
tutorials name as "the" fencing exception. Both classes exist in
`kafka-clients:4.3.1` — inspected directly with `javap`:

```
public class org.apache.kafka.common.errors.InvalidProducerEpochException extends org.apache.kafka.common.errors.ApplicationRecoverableException
public class org.apache.kafka.common.errors.ProducerFencedException     extends org.apache.kafka.common.errors.ApplicationRecoverableException
```

They are **siblings**, not a subtype relationship — catching one does not
catch the other. This repository's automated fencing test
(`secondProducerInstanceFencesTheFirstUnderTheSameTransactionalId`) and
`FencingDemoApp` both catch both, and report honestly which one actually
occurred. Whichever exception arrives, the practical consequence is
identical: this producer INSTANCE is permanently unusable for this
`transactional.id` and MUST be discarded — it cannot be "un-fenced" or
retried; a caller must construct an entirely new `KafkaProducer`.

## 12. Consume-transform-produce WITHOUT transactions — real, captured

```text
Input Topic
    ↓
Consumer
    ↓
Business transformation
    ↓
Output Topic
```

```text
consume input
     ↓
produce output
     ↓
CRASH
     ↓
offset not committed
     ↓
input replayed
     ↓
output duplicated
```

`NonTransactionalConsumeTransformProduceApp`, crash injected AFTER the
output record is produced but BEFORE the input offset is committed. Real
output, first (crashing) run:

```
consumed inputOffset=0 eventId=Order-N1 -> produced outputPartition=0 outputOffset=0
SIMULATED_CRASH | after producing output for eventId=Order-N1, before committing input offset 1
Exiting abruptly WITHOUT committing the offset for the record just produced -- and WITHOUT closing the consumer/producer.
```

The automated equivalent
(`nonTransactionalPipelineDuplicatesOutputWhenCrashingBeforeOffsetCommit`)
restarts with a fresh consumer in the SAME group after this crash and
confirms, deterministically: the same input record is reprocessed (its
offset was never committed), a SECOND output record is produced, and the
output topic ends up with **2** records for the same logical input event —
a real, verified duplicate. This is WP-06's `AFTER_PROCESS_BEFORE_COMMIT`
failure window (see
[`docs/delivery-semantics/DELIVERY_SEMANTICS_AND_OFFSET_MANAGEMENT.md`](../delivery-semantics/DELIVERY_SEMANTICS_AND_OFFSET_MANAGEMENT.md)),
applied to a pipeline whose "processing" is itself a Kafka produce.

## 13. Atomic consume-transform-produce

```text
beginTransaction()

consume input
      ↓
transform
      ↓
produce output
      ↓
sendOffsetsToTransaction()

commitTransaction()
```

The atomic unit is no longer "one record's offset" — it is:

```text
OUTPUT RECORDS
       +
INPUT OFFSETS
```

committed together, via `producer.sendOffsetsToTransaction(offsets,
consumer.groupMetadata())` called INSIDE the same transaction as the
output `send()` calls, followed by ONE `commitTransaction()` that commits
both atomically.
`TransactionalConsumeTransformProduceApp` implements exactly this loop
against the current Kafka Java API (`ConsumerGroupMetadata` from
`consumer.groupMetadata()`, not the deprecated `String consumerGroupId`
overload some older code samples use).

## 14. Crash before commit — real, captured

Crash injected AFTER `sendOffsetsToTransaction()` but BEFORE
`commitTransaction()` returns
(`crashBeforeCommitLeavesNoVisibleOutputAndInputIsReprocessedOnRestart`,
passing):

1. Attempt 1 sends the output record, calls `sendOffsetsToTransaction()`,
   then throws the simulated crash — `commitTransaction()` is never called.
   Real assertion: a `read_committed` consumer of the output topic sees
   **zero** records for a bounded 5-second window immediately afterward.
2. Attempt 2 (simulated restart: a fresh consumer AND a fresh producer
   instance, same `transactional.id` and `group.id`) calls
   `initTransactions()` — which, per Section 5, aborts the hanging
   transaction attempt 1 left open — then reprocesses the SAME input record
   (its offset was never committed) and commits successfully.
3. Real, verified final state: **exactly one** committed output record
   exists (`PROCESSED-Order-B1`) — attempt 1's output never became visible,
   and attempt 2's retry did not duplicate it.

This is the guarantee Section 15 of the spec asks for, demonstrated with
real client behavior rather than asserted from documentation: the
"hanging" transaction from a crashed process does not linger as a threat
to correctness — the next producer instance sharing that `transactional.id`
resolves it automatically as part of ordinary startup.

## 15. Crash after commit — real, captured

Crash injected immediately AFTER `commitTransaction()` returns
(`crashAfterCommitDoesNotReprocessInputOnRestart`, passing):

1. Attempt 1 commits successfully (output visible, offset committed, both
   atomically), then the simulated crash fires.
2. Real, verified: the output topic already shows the committed record
   (`PROCESSED-Order-C1`) immediately, since the commit genuinely
   succeeded before the crash.
3. A restart (fresh consumer, same `group.id`) is given a bounded 8-second
   window with no new input available. It processes **zero** additional
   records — the previously-committed offset already covers this input, so
   there is nothing left to reprocess.

This directly extends WP-06's offset-commit-prevents-reprocessing finding
(the `AFTER_COMMIT_BEFORE_PROCESS`-adjacent case) to a pipeline where the
processing step is itself a Kafka produce: because the offset commit and
the output commit are the SAME atomic operation here, there is no window
in which "output is durable but offset commit hasn't happened yet" (or vice
versa) for a crash to land in.

## 16. Exactly-once Kafka processing — the boundary

```text
Kafka input
     ↓
Kafka processing
     ↓
Kafka output
```

Sections 12-15, taken together, are real evidence for a precise claim:
**when a consume-transform-produce pipeline stays entirely within Kafka —
reading from a Kafka topic, writing to a Kafka topic, committing its
consumer offsets via `sendOffsetsToTransaction()` in the same transaction
as its output — Kafka transactions provide exactly-once PROCESSING
semantics for that pipeline**, provided the whole application state that
matters is representable as Kafka records and offsets.

This document deliberately avoids the phrase "Kafka guarantees exactly
once everywhere." It does not. Sections 17-18 show precisely where that
guarantee stops.

## 17. The external database problem

```text
Kafka Consumer
      ↓
Business Logic
      ├── PostgreSQL UPDATE
      │
      └── Kafka Produce
```

**Can one ordinary Kafka transaction atomically commit a PostgreSQL
transaction AND a Kafka transaction together? No — not simply through
Kafka transactions.**

Kafka's transaction coordinator only knows about Kafka partitions. It has
no visibility into, and no protocol for, a PostgreSQL transaction's commit
decision. If an application does:

```text
BEGIN Postgres transaction
UPDATE ...
COMMIT Postgres transaction
producer.send(...)          <- crash here?
```

...a crash between the Postgres commit and the Kafka send leaves the
database updated but the Kafka message never sent. Reversing the order
(`producer.send()` then the Postgres update) just moves the same problem:
a crash between them leaves a Kafka message sent for a database update
that never happened. This is the **dual-write problem**: two independent
systems, each with its own commit boundary, and no atomic operation that
spans both. Wrapping the Kafka side in a transaction does not change this
— it makes the KAFKA side atomic and durable, and does nothing at all for
the PostgreSQL side's relationship to it.

## 18. External API side effects

```text
Kafka message
     ↓
chargeCreditCard()
     ↓
produce payment-completed
```

This is the dual-write problem's sharper form, because a REST call to a
payment provider has no "transaction" Kafka could even theoretically join.
If the process crashes between `chargeCreditCard()` succeeding and the
`payment-completed` record being produced (committed or not), **Kafka's
exactly-once semantics cannot undo the external charge, and cannot know
that the charge happened in order to avoid re-charging on retry.** The
money already moved in a system Kafka has no authority over and no
knowledge of. Exactly-once BUSINESS effects for irreversible external
side effects require patterns that live at the business-logic layer, not
inside the messaging layer — see Section 19.

## 19. Bridge to future WPs

Named here, not implemented — these are how real systems close the gaps
Sections 17-18 describe:

- **Idempotency key** — the external API call itself carries a
  caller-generated unique key (e.g., stored alongside the order), so a
  retried `chargeCreditCard()` call is recognized and deduplicated BY THE
  PAYMENT PROVIDER, not by Kafka. This is often the only real fix for
  Section 18's problem, because Kafka genuinely has no reach into the
  external system.
- **Idempotent consumer** — the WP-06 pattern (`ProcessedEventStore`) of
  checking "have I already applied this event's effect?" before applying
  it, backed by durable state OUTSIDE Kafka's own offset storage.
- **Transactional outbox** (**WP-12**) — write the "intent to produce a
  Kafka message" into the SAME database transaction as the business data
  change, then a separate relay process reads the outbox table and
  produces to Kafka, making the database write and the "will eventually
  produce to Kafka" decision atomic (though the actual Kafka produce still
  happens later, separately).
- **Inbox pattern** — the outbox pattern's mirror image on the consuming
  side: record an incoming message's ID in the same database transaction
  as applying its effect, so a reprocessed message is recognized and
  skipped using the SAME atomicity the outbox pattern gives the producer
  side.
- **CDC** (**WP-11**) — instead of an application writing to its database
  AND explicitly producing to Kafka, a change-data-capture tool
  (Debezium) reads the database's own commit log and produces to Kafka
  FOR the application, so there is only one write (to the database) for
  the application to get right at all.

## 20. Exactly-once terminology, precisely distinguished

| Term | What it actually guarantees | What it does NOT guarantee |
|---|---|---|
| **At-most-once** | A record is delivered zero or one times. Nothing is ever duplicated. | Records can be silently lost. |
| **At-least-once** | A record is delivered one or more times. Nothing is ever silently lost. | The same record's effect can be applied more than once. |
| **Idempotent producer** | ONE partition never durably stores a duplicate of ONE record, even across retries. | Nothing about a GROUP of writes succeeding or failing together (Section 4). Nothing about consumers or offsets at all. |
| **Kafka transaction** | A group of Kafka writes (and, via `sendOffsetsToTransaction()`, consumer offset commits) commit or abort together, atomically, and `read_committed` consumers only ever see the committed outcome. | Nothing outside Kafka. Does not make a transaction "fast" or "free" — see Observability below. |
| **Kafka exactly-once processing** | For a pipeline that consumes from Kafka, transforms, and produces back to Kafka (with offsets committed in the same transaction), each input record's effect on Kafka's own state is applied exactly once, even across crashes and restarts (Sections 12-16). | Anything about non-Kafka side effects performed during that same processing (Sections 17-18). |
| **Business exactly-once effect** | The REAL-WORLD outcome (a database row, a charged credit card, an email sent) happens exactly once, end to end. | This is NEVER a Kafka guarantee by itself — it requires the patterns in Section 19 (idempotency keys, idempotent consumers, outbox/inbox, CDC), applied deliberately, on top of whatever Kafka guarantees. |

This table builds directly on WP-06's own at-most-once/at-least-once
distinction (`docs/delivery-semantics/`) by inserting the four additional,
more precise rungs Kafka transactions actually add between "at-least-once"
and "business exactly-once" — rather than treating "exactly-once" as one
undifferentiated claim.

## 21. Failure matrix

| Scenario | Result | Status |
|---|---|---|
| Producer retry after unknown-outcome send, idempotence OFF | Duplicate risk is real; whether a specific run produces a duplicate depends on exact timing | Mechanism verified experimentally; specific duplicate occurrence is architectural reasoning, not asserted in CI (Section 1) |
| Idempotence ON, same PID/partition/sequence resent | Broker suppresses the duplicate, returns original record's metadata | Verified via `describe-producers` (Section 2); the suppression itself is Kafka's documented protocol behavior, not independently forced in this lab |
| Transaction commit | All records become visible to `read_committed` together, only after commit returns | **Experimentally verified** (Section 6, automated test) |
| Transaction abort | None of the transaction's records ever become visible to `read_committed` | **Experimentally verified** (Section 7, automated test) |
| Crash before commit (consume-transform-produce) | No output visible; input reprocessed on restart; exactly one committed output after retry | **Experimentally verified** (Section 14, automated test) |
| Crash after commit (consume-transform-produce) | Output already visible; input NOT reprocessed on restart | **Experimentally verified** (Section 15, automated test) |
| Consumer `read_uncommitted` | Observes records from transactions that later abort | **Experimentally verified** (Section 8) |
| Consumer `read_committed` | Never observes aborted records, at any point | **Experimentally verified** (Section 8) |
| Same `transactional.id`, two producer instances | Older instance is fenced (`InvalidProducerEpochException` in this client version); newer instance proceeds normally | **Experimentally verified** (Section 11, automated test) |
| Consume-transform-produce, no transactions, crash after produce before offset commit | Input reprocessed; output duplicated | **Experimentally verified** (Section 12, automated test) |
| Kafka transaction + PostgreSQL transaction, atomically | Not possible via Kafka transactions alone (dual-write problem) | Architectural reasoning (Section 17) — deliberately not attempted experimentally, since there is no Kafka-native mechanism to attempt |
| Kafka transaction + external API call (e.g., payment), atomically | Not possible; Kafka cannot undo or deduplicate the external effect | Architectural reasoning (Section 18) |

## 22. Observability

**Application-visible (from `KafkaProducer`/`KafkaConsumer` metrics and
exceptions directly):**

- Transaction commit latency — timed around `commitTransaction()` in
  application code, or read from the producer's `txn-commit-time-ns-total`
  / `txn-commit-time-ns-avg-max` style metrics exposed via
  `producer.metrics()` (introduced alongside the transactional producer;
  exact metric names should be confirmed against `producer.metrics()`'s
  actual keys for the deployed version rather than assumed).
- Fencing exceptions — `InvalidProducerEpochException` and
  `ProducerFencedException` (Section 11) — both fatal for the producer
  instance that receives them.
- `TimeoutException` during `initTransactions()` / `commitTransaction()` /
  `sendOffsetsToTransaction()` — visible directly as thrown exceptions, the
  same way WP-08's quorum-loss experiments observed client-side timeouts.
- Producer retry counts and error rates — `record-retry-rate`,
  `record-error-rate` producer metrics (unchanged from non-transactional
  producers).
- Consumer lag — unchanged from WP-04/WP-05; a transactional consumer's
  lag is measured the same way, though `read_committed` consumers lag
  BEHIND `read_uncommitted` consumers on the same topic by however long
  transactions typically stay open, since committed records only become
  visible once their transaction resolves.

**Broker/platform-visible (JMX, not independently re-verified against
4.3.1's exact metric names in this lab — treat the categories, not the
exact metric strings, as the takeaway):**

- Transaction coordinator error rates and hanging-transaction counts —
  historically exposed under
  `kafka.server:type=transaction-coordinator-metrics`.
- `__transaction_state` partition health — the same ISR/leader metrics
  every other internal topic exposes (WP-07), since it is an ordinary
  replicated topic.

A full monitoring-stack implementation (dashboards, alerting thresholds,
long-term metric retention) remains **WP-16**.

## 23. Production architecture discussion

There is no universal "right" topology decision this WP argues for —
only real trade-offs, verified where this lab could verify them:

- **Should every producer be transactional?** No — transactions add real
  coordinator round-trips (`initTransactions()`, `sendOffsetsToTransaction()`,
  the commit itself) that a purely idempotent, non-transactional producer
  never pays. Reach for a transaction specifically when a GROUP of writes
  (possibly including offset commits) must succeed or fail together — not
  as a default "safer" setting for every producer.
- **`transactional.id` stability matters operationally.** Because a NEW
  producer instance with the SAME `transactional.id` fences the old one
  (Section 11), a deployment strategy that briefly runs two instances of
  the same logical producer (a rolling restart, for example) will fence
  the outgoing instance the moment the incoming one starts — which is
  usually exactly the desired behavior (only one instance should be
  authoritative), but is worth designing for deliberately rather than
  discovering during an incident.
- **Kafka transactions solve the Kafka-to-Kafka case well; they do not
  extend to your database or your payment provider (Sections 17-18).**
  Reaching for a transactional outbox, an idempotent consumer with a
  business idempotency key, or CDC is not "extra" engineering bolted onto
  Kafka transactions — it is a DIFFERENT problem that Kafka transactions
  were never positioned to solve.

## 24. Principal Engineer questions

**1. What problem does Kafka producer idempotence solve?**
It closes the specific gap where a producer retries a send because it
received an unknown/ambiguous outcome (a timeout, a dropped connection)
and that retry lands as a SECOND, distinct record on the SAME partition,
even though the original write actually succeeded. It does this per
partition, with no application code required once `enable.idempotence=true`
is set (which, in kafka-clients 4.3.1, is already the default).

**2. How do PID and sequence numbers prevent duplicates?**
The broker assigns each idempotent producer a Producer ID during
`InitProducerId`, and the producer tags every record sent to a given
partition with a strictly increasing sequence number. The broker tracks
the highest `(PID, partition, sequence)` it has accepted; if a retry
arrives with a sequence number at or below what was already accepted for
that PID and partition, the broker recognizes it as a resend and returns
the original record's metadata instead of writing a duplicate. Real,
broker-reported evidence: `describe-producers` output showing
`ProducerId=6000`, `LastSequence=2` for a 3-record burst (Section 2).

**3. Idempotence vs. transaction — what's actually different?**
Idempotence guarantees ONE record is never duplicated on ONE partition.
A transaction guarantees a GROUP of writes (potentially across many
partitions and topics, and potentially including a consumer offset
commit) become visible together or not at all. Every transactional
producer is idempotent (the client forces it on), but idempotence alone
gives no atomicity across multiple records or partitions (Section 4).

**4. What does `transactional.id` do?**
It is a caller-supplied, stable identity that lets the SAME logical
producer be recognized across restarts. It is what the transaction
coordinator uses to track producer epoch, in-progress transaction state,
and which partitions a transaction has touched
(`__transaction_state`, Section 10) — and it is what makes fencing
possible: any two producer instances configured with the same
`transactional.id` are, to Kafka, the same logical producer, and only the
most recently initialized one is allowed to remain authoritative
(Section 11).

**5. What is producer fencing?**
The mechanism that guarantees only one producer instance can be
authoritative for a given `transactional.id` at a time. When a NEW
instance calls `initTransactions()` with a `transactional.id` an older,
still-running instance is using, the coordinator bumps the producer epoch,
which makes every subsequent operation from the OLD instance fail —
observed in this lab as `InvalidProducerEpochException` (not the
`ProducerFencedException` many tutorials name; see Section 11 for why
both exist and are siblings, not a subtype relationship, in
kafka-clients 4.3.1). This prevents split-brain: two producer instances
(e.g., during a botched deployment) both believing they are the
authoritative writer for the same logical stream.

**6. What is the transaction coordinator?**
The broker currently leading the `__transaction_state` partition a given
`transactional.id` hashes to. It manages that transactional.id's producer
epoch, records which partitions the current transaction has written to,
and durably writes the commit/abort decision as the authoritative record
of what happened — different transactional IDs are, in general, handled
by different coordinator brokers in parallel (Section 10); there is no
single global coordinator for a cluster.

**7. What is stored in `__transaction_state`?**
Per `transactional.id`: the current producer ID and epoch, the
transaction's state (e.g., `Ongoing`, `PrepareCommit`, `CompleteCommit`,
`CompleteAbort`), the transaction timeout, and the set of partitions the
current (or most recent) transaction has written to. It is a normal,
replicated, log-compacted internal topic — real evidence:
`PartitionCount: 50`, `ReplicationFactor: 3`,
`cleanup.policy=compact` (Section 10).

**8. `read_committed` vs. `read_uncommitted`?**
`read_uncommitted` (the client's default in kafka-clients 4.3.1) returns
every record physically written to a partition, including ones from
transactions that later abort. `read_committed` withholds each
partition's records until that transaction's outcome (commit or abort)
is known, and then either surfaces them (commit) or skips them entirely,
forever (abort). Real, side-by-side evidence in Section 8: the same
abort produced records `read_uncommitted` showed and `read_committed`
never did.

**9. How can one Kafka transaction span multiple partitions?**
The producer simply calls `send()` to as many different
topic-partitions as the business operation needs, all between one
`beginTransaction()` and one `commitTransaction()`/`abortTransaction()`
call. The coordinator tracks every partition touched
(`__transaction_state`) and, on commit, writes a control (marker) record
to EVERY one of those partitions — a `read_committed` consumer of any of
them waits for its own partition's marker before surfacing that
transaction's records, which is what makes the multi-partition,
multi-topic commit/abort in Section 9 atomic in observable effect even
though the underlying writes are physically on different brokers.

**10. Why use `sendOffsetsToTransaction()`?**
Because otherwise the input offset commit and the output record(s) are
TWO separate operations with their own independent failure windows — the
exact non-transactional problem Section 12 demonstrates experimentally
(crash between them causes reprocessing and duplicated output).
`sendOffsetsToTransaction(offsets, consumer.groupMetadata())`, called
inside the same transaction as the output `send()` calls, makes the
offset commit part of the SAME atomic unit as the output, so
`commitTransaction()` either commits both or neither.

**11. How does consume-transform-produce EOS actually work?**
Per poll batch: `beginTransaction()`, transform and `send()` each
record to the output topic (still inside the open transaction),
`sendOffsetsToTransaction()` with the input offsets, then
`commitTransaction()`. Nothing about the input records' offsets is ever
committed through the CONSUMER's own `commitSync`/`commitAsync` — offset
management is handed entirely to the transactional producer via
`sendOffsetsToTransaction()` instead (Section 13,
`TransactionalConsumeTransformProduceApp`).

**12. What happens if the application crashes before commit?**
The transaction is left "hanging" at the coordinator — its output is
never visible to `read_committed` consumers, and its input offsets were
never committed. On restart, the NEXT producer instance using the SAME
`transactional.id` calls `initTransactions()`, which aborts that hanging
transaction as part of ordinary startup (not extra code the application
has to write), and the consumer resumes from the last genuinely
committed offset, reprocessing the crashed attempt's input from scratch.
Real, verified: Section 14 (`crashBeforeCommitLeavesNoVisibleOutputAnd...`
test) shows exactly one committed output record after the retry, not two.

**13. What happens if it crashes after commit?**
Nothing is lost or duplicated: the output was already durably committed,
and — critically — the input offset was committed as PART OF THE SAME
transaction, so a restarted consumer resumes strictly after that offset
and never reprocesses this input. Real, verified: Section 15
(`crashAfterCommitDoesNotReprocessInputOnRestart` test) shows zero
additional records processed after the crash-adjacent restart.

**14. Does Kafka EOS make a PostgreSQL update exactly-once?**
No. Kafka's transaction coordinator has no protocol connection to
PostgreSQL at all — it cannot know about, participate in, or roll back a
PostgreSQL transaction. Wrapping the Kafka side of an operation in a
transaction makes the KAFKA writes atomic; it does nothing for the
relationship between the Kafka writes and a separate database write
(Section 17, the dual-write problem).

**15. Does Kafka EOS make a payment API call exactly-once?**
No, and this case is starker than the database case: a REST call to an
external payment provider has no transactional relationship to Kafka
whatsoever. If a process crashes after a real charge succeeds but before
the corresponding Kafka record is produced (or committed), Kafka cannot
undo the charge and cannot know a retry would be a duplicate (Section 18).
Preventing a double-charge requires the EXTERNAL system to deduplicate,
typically via a caller-supplied idempotency key on the API call itself.

**16. Kafka exactly-once vs. business exactly-once — what's the actual
difference?**
Kafka exactly-once (processing) is a real, verifiable guarantee about
Kafka's OWN state: given a consume-transform-produce pipeline entirely
within Kafka, each input's effect on Kafka's own topics is applied
exactly once, even across crashes (Sections 12-16, all experimentally
verified). Business exactly-once is a claim about the REAL WORLD — a row
updated once, a customer charged once, an email sent once — and Kafka's
guarantee only covers the portion of that outcome that is actually
represented as Kafka records. The moment a business operation touches
anything Kafka doesn't control (a database, an external API), achieving
business exactly-once requires the patterns in Section 19, deliberately
applied — it is never a side effect of using Kafka transactions.

**17. When would you still need an idempotency key even with Kafka
transactions in place?**
Any time the operation that must not be duplicated is NOT itself a Kafka
write — most commonly, a call to an external system (a payment provider,
a third-party API, an email service). Kafka transactions cannot make that
call idempotent; only the external system deduplicating by a
caller-supplied key can (Section 18). This is also true internally
whenever the "effect" is a non-Kafka side effect performed inside a
consumer's processing logic (a database write not covered by a
transactional outbox, for instance) — the idempotent-consumer pattern
(a durable "have I already applied this event's effect?" check, as
WP-06's `ProcessedEventStore` demonstrates) covers that case.

**18. When would you use a transactional outbox instead of relying on
Kafka transactions alone?**
When the business operation's SOURCE of truth is a database write, and
producing a Kafka message needs to be atomic WITH THAT DATABASE WRITE —
which a Kafka transaction alone cannot provide, since Kafka's coordinator
has no reach into the database (Section 17). The transactional outbox
pattern (WP-12) makes "update the database" and "record the intent to
produce a Kafka message" one atomic database transaction, then relies on
a separate relay process to actually produce to Kafka from the outbox
table afterward — trading "atomic across Kafka and the database" (not
achievable) for "atomic within the database, eventually produced to
Kafka" (achievable, and sufficient for most real dual-write problems).
