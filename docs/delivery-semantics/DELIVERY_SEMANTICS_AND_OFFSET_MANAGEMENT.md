# Delivery Semantics and Offset Management

This document is the conceptual and Principal Engineer depth behind
[`labs/lab-05-offset-management-delivery-semantics`](../../labs/lab-05-offset-management-delivery-semantics/README.md).
Every observation below is a real result captured while building and
validating that lab against the WP-02 Kafka environment (KRaft, single
broker, `apache/kafka:4.3.1`) -- see the lab's README for exact commands
and full captured output.

## Scope note: what this document is not

This document is about what a **consumer's own poll/process/commit loop**
can and cannot guarantee. It deliberately does **not** cover:

- Kafka's idempotent producers or transactions (`enable.idempotence`,
  `transactional.id`, `read_committed`) -- reserved for WP-09.
- The transactional outbox pattern, for coordinating a Kafka publish with
  a database write atomically -- reserved for WP-12.
- A production-grade idempotent-consumer implementation (durable
  deduplication tables, retry/DLQ integration) -- reserved for WP-13.

This lab's idempotent-consumer experiment is real and its result is real,
but it is intentionally the simplest possible version of the pattern (a
flat file keyed by `eventId`), so the *shape* of the idea is visible
without pulling in database transactions, retry topics, or dead-letter
handling that belong to those later work packages. See "The dual-write
problem" near the end of this document for exactly where this lab's
scope ends and WP-12/WP-13's begins.

## Offset fundamentals: three numbers that are not the same number

```text
Record offset       -- a fixed property of a record, assigned once by the
                        partition leader when the record was written.
                        Never changes.

Consumer position    -- this consumer's own in-memory bookmark: the next
                        offset it will fetch on this partition. Advances
                        every time poll() returns records for that
                        partition, whether or not the application has
                        finished (or even started) doing anything with
                        them yet.

Committed offset     -- what this consumer GROUP has durably told Kafka,
                        via commitSync()/commitAsync(), it considers
                        safe to resume from. Lives in the internal
                        __consumer_offsets topic, keyed by
                        (group, topic, partition). Only changes when the
                        application explicitly commits (or auto-commit
                        does so on its behalf).
```

Real, captured proof that these are genuinely independent numbers
(`lab-05`'s `OffsetLifecycleApp`, phase 1 -- full command and output in
the lab README):

```text
fetched record: partition=0 recordOffset=0 ... 
  -> consumer position for partition=0 is now 4 (next offset to fetch)
fetched record: partition=0 recordOffset=1 ...
  -> consumer position for partition=0 is now 4 (next offset to fetch)
...
committed offsets after committing everything except the last fetched record:
  partition=0 committed=OffsetAndMetadata{offset=3, ...} position=4  <-- position != committed offset
  partition=1 committed=null position=1  <-- position != committed offset
```

Two things worth being precise about, both directly visible in that real
output:

1. **Position advances for an entire `poll()` result, not per record
   your loop has actually finished handling.** All four records above
   came back in a single `poll()` call, so `position` jumped straight to
   4 the instant `poll()` returned -- before the application had
   processed any of them individually. A slow or crashing application
   loop does not hold position back; only a commit changes what the
   *group* will resume from.
2. **`partition=1 committed=null position=1`** — this consumer was
   assigned partition 1 too (a 2-partition topic), `poll()` had already
   fetched into it, but the deliberately-limited experiment never
   iterated a record from it. Position moved anyway. This is the same
   mechanism as point 1, just visible on a partition the application
   loop never touched at all in this run.

### What "offset N committed" actually means -- the off-by-one that trips people up

A committed offset of `N` means: **"resume by fetching from offset `N`
next"** -- i.e., **the last record this group has finished with is offset
`N - 1`**, not offset `N`. `OffsetAndMetadata(record.offset() + 1)` in
this lab's own commit calls is not an arbitrary `+1`; it is that exact
statement, spelled out in code every time it appears. Get this backwards
and you will either skip a record on restart (commit `record.offset()`
instead of `record.offset() + 1`) or replay one you already finished
(forget the `+1` and then reason as if you hadn't).

### Restart and replay -- real proof

Phase 2 of the same experiment: a **second, independent `KafkaConsumer`
instance**, same `group.id`, simulating a restart:

```text
replayed record: partition=0 recordOffset=3 key=CUSTOMER-101 value=eventId=ORDER-1005|...
  this restart resumed at the last committed offset (3) -- not at phase 1's in-memory position
```

Phase 1 deliberately committed only through offset 2 (withholding the
last fetched record, offset 3, on purpose) to manufacture exactly this
gap. The restarted instance resumed at the **committed** offset (3), not
at phase 1's in-memory **position** (4) -- proving the two are tracked
completely independently, and that a restart only ever trusts the
committed value.

### `auto.offset.reset` -- real proof, both directions

For a group that has **never committed anything**, `auto.offset.reset`
decides where a fresh assignment starts:

```text
partition=0 starting position=4 (autoOffsetReset=earliest)
partition=1 starting position=1 (autoOffsetReset=earliest)
```

`earliest` starts at each partition's low watermark and replays the
entire retained history immediately -- position jumped straight to the
end of what was available the instant `poll()` returned. `latest` starts
at each partition's high watermark at assignment time instead: the
consumer sees nothing until a *new* record is produced after it joined.
Neither value is inherently correct; see Principal Engineer question 3.

## The two failure windows that matter

These are the two windows this lab's crash-injection framework
(`FailurePoint`: `BEFORE_PROCESS`, `AFTER_PROCESS_BEFORE_COMMIT`,
`AFTER_COMMIT_BEFORE_PROCESS`) exists to make reproducible on demand,
instead of depending on randomly killing a process.

### At-least-once: process, then commit

```text
poll() -> record fetched
   |
   v
processing runs (business side effect happens)
   |
   v
  [CRASH HERE]  <-- AFTER_PROCESS_BEFORE_COMMIT
   |
   v
commit (never reached)
   |
   v
RESTART -> resumes from last COMMITTED offset (still before this record)
   |
   v
record is FETCHED AND PROCESSED AGAIN -> duplicate
```

Real, captured (`lab-05`, `commitTiming=AFTER_PROCESS`,
`failurePoint=AFTER_PROCESS_BEFORE_COMMIT`, `crashAtEventId=ORDER-1003`):

```text
consumer-1 | partition=0 | offset=2 | eventId=ORDER-1003 | attempt=1 | status=SUCCESS
consumer-1 | SIMULATED_CRASH | Simulated crash at AFTER_PROCESS_BEFORE_COMMIT for eventId=ORDER-1003
consumer-1 | exiting abruptly WITHOUT consumer.close() -- no LeaveGroup is sent, ...

--- restart, fresh consumer instance, same group ---
consumer-2 | partition=0 | offset=2 | eventId=ORDER-1003 | attempt=1 | status=SUCCESS   <-- reprocessed
consumer-2 | partition=0 | eventId=ORDER-1003 | commitSync -> COMMITTED committedOffset=3
```

`ORDER-1003` was processed twice: once by `consumer-1` before it
crashed, once again by `consumer-2` on restart. This is at-least-once
delivery working exactly as designed -- Kafka never promised your
application would run exactly once, only that it would not silently
drop a record it never told the group it had finished.

### At-most-once: commit, then process

```text
poll() -> record fetched
   |
   v
commit (offset advances -- group now considers this record "done")
   |
   v
  [CRASH HERE]  <-- AFTER_COMMIT_BEFORE_PROCESS
   |
   v
processing (never reached)
   |
   v
RESTART -> resumes from last COMMITTED offset (already PAST this record)
   |
   v
record is NEVER FETCHED AGAIN -> permanently lost, from the app's perspective
```

Real, captured (`commitTiming=BEFORE_PROCESS`,
`failurePoint=AFTER_COMMIT_BEFORE_PROCESS`, `crashAtEventId=ORDER-1003`):

```text
consumer-1 | partition=0 | eventId=ORDER-1003 | commitSync -> COMMITTED committedOffset=3
consumer-1 | SIMULATED_CRASH | Simulated crash at AFTER_COMMIT_BEFORE_PROCESS for eventId=ORDER-1003

--- restart, fresh consumer instance, same group ---
consumer-2 | partition=0 | offset=3 | eventId=ORDER-1004 | attempt=1 | status=SUCCESS   <-- jumps straight past 1003
```

`ORDER-1003` is gone. Not delayed, not retried -- gone. This is the
sharpest possible illustration of why `commitTiming=BEFORE_PROCESS` is a
dangerous default: it optimizes for "never reprocess," which sounds safe
in isolation, at the direct cost of "never lose."

## `commitSync()` vs. `commitAsync()`

| | `commitSync()` | `commitAsync()` |
|---|---|---|
| Blocks the poll loop | Yes, until the broker acknowledges (or the call fails) | No -- returns immediately, result delivered to a callback later |
| Retries on retriable failure | Yes, automatically, until success or a non-retriable error | No -- retrying is the caller's responsibility, and only safely for the *latest* offset (see below) |
| Throughput impact | Every commit is a real pause in the loop | None -- commits pipeline alongside fetching |
| Failure visibility | An exception, synchronously, at the call site | A callback, asynchronously, easy to silently ignore if not wired up deliberately |

Neither is universally "the right one." This lab's `commitMode=SYNC`
path is used throughout its README because a lab about offset semantics
benefits from every commit being a real, ordered, observable event in
the log -- not because `commitAsync()` is inferior. The commonly
recommended production pattern is a mix: `commitAsync()` for routine,
frequent commits during normal processing (accepting that a rare
send-and-lost commit will just be caught by the *next* successful async
commit or by the eventual sync commit below), and one final
`commitSync()` at shutdown, in a `finally` block, so the last word on
this consumer's progress is a blocking, retried, exception-surfacing
call rather than a fire-and-forget one. `commitAsync()`'s own retry
danger is real and specific: retrying an older, now-stale async commit
out of order can overwrite a *later* commit that already succeeded --
which is why the Kafka client's own recommended pattern for
`commitAsync()` retries tracks a monotonically increasing commit
sequence number and discards any retry that is no longer the latest.

## Auto-commit vs. manual commit -- real proof

`enable.auto.commit=true` (this lab's default interval, unmodified:
5000ms) commits **periodically**, from inside a `poll()` call, entirely
independent of whether your application has actually finished with what
it already fetched. Real, captured result (`autoCommit=true`,
`failurePoint=AFTER_PROCESS_BEFORE_COMMIT`, `crashAtEventId=ORDER-1018`,
18 records processed in well under 5 seconds):

```text
consumer-1 | partition=0 | offset=17 | eventId=ORDER-1018 | attempt=1 | status=SUCCESS
consumer-1 | SIMULATED_CRASH | Simulated crash at AFTER_PROCESS_BEFORE_COMMIT for eventId=ORDER-1018

$ kafka-consumer-groups.sh --describe --group auto-commit-demo
GROUP             ... CURRENT-OFFSET  LOG-END-OFFSET  LAG
auto-commit-demo  ... -                21              -
```

**Zero** offsets were committed. All 18 processed records replay from
scratch on restart -- a materially larger duplicate blast radius than
this document's at-least-once example above (one duplicate), because
auto-commit's interval had not yet elapsed when the crash happened. This
is not "auto-commit is bad" -- it is auto-commit doing exactly what it
was configured to do (commit roughly every 5 seconds, no more often).
The trade-off it makes explicit: simplicity and no per-record commit
overhead, in exchange for an unpredictable, workload-dependent
replay-on-crash window instead of a small, understood one. Manual commit
trades the opposite way: you decide exactly how large that window is
allowed to get, at the cost of writing (and reasoning about) the commit
calls yourself.

## Batch-commit boundaries -- whole-batch vs. safe partial progress

Real side-by-side result, same failure (processing fails at one specific
record inside a batch), two different commit strategies:

**`commitTiming=AFTER_BATCH`** (commit once, only after the *entire*
poll batch succeeds):

```text
consumer-2 | partition=0 | offset=10 | eventId=ORDER-1011 | status=SUCCESS
consumer-2 | partition=0 | offset=11 | eventId=ORDER-1012 | status=SUCCESS
consumer-2 | partition=0 | offset=12 | eventId=ORDER-1013 | status=SUCCESS
consumer-2 | partition=0 | offset=13 | eventId=ORDER-1014 | status=FAILED
consumer-2 | BATCH_NOT_COMMITTED | whole-batch-commit semantics: because this batch
  failed partway through, none of its offsets were committed -- even the records
  that succeeded before the failure will be reprocessed on restart.

$ kafka-consumer-groups.sh --describe --group batch-boundary-demo-2
... CURRENT-OFFSET=10  LOG-END-OFFSET=15  LAG=5
```

**`commitTiming=AFTER_PROCESS`** (commit after *each* record, the exact
same failure at the exact same record):

```text
consumer-2 | partition=0 | offset=10 | eventId=ORDER-1011 | status=SUCCESS
consumer-2 | partition=0 | eventId=ORDER-1011 | commitSync -> COMMITTED committedOffset=11
consumer-2 | partition=0 | offset=11 | eventId=ORDER-1012 | status=SUCCESS
consumer-2 | partition=0 | eventId=ORDER-1012 | commitSync -> COMMITTED committedOffset=12
consumer-2 | partition=0 | offset=12 | eventId=ORDER-1013 | status=SUCCESS
consumer-2 | partition=0 | eventId=ORDER-1013 | commitSync -> COMMITTED committedOffset=13
consumer-2 | partition=0 | offset=13 | eventId=ORDER-1014 | status=FAILED

$ kafka-consumer-groups.sh --describe --group batch-boundary-safe-demo
... CURRENT-OFFSET=13  LOG-END-OFFSET=15  LAG=2
```

Identical failure, identical data, **5 records must be replayed under
whole-batch-commit versus 2 under per-record commit** -- entirely a
function of commit granularity, not of the failure itself.
Whole-batch-commit is not simply worse, though: it means exactly one
network round trip per poll batch instead of one per record, a real
throughput advantage when failures are rare and records are cheap to
reprocess (particularly alongside idempotent processing, below). The
trade a batch-boundary strategy always makes explicit: **larger commit
granularity always means a larger replay set on partial failure** --
choose the granularity based on how expensive a duplicate actually is
for that specific business operation, not as a blanket default.

**Partition-ordering implication:** because Kafka only ever guarantees
order *within* a partition, and a restart always resumes from the
group's single committed offset for that partition, replay after a
batch failure is never "reprocess record 13 only" -- it is always "reprocess
everything from the committed offset forward, in the original order,"
including records that already succeeded once. There is no mechanism to
selectively skip 11 and 12 while reprocessing 13; the committed offset is
a single number, not a per-record ledger.

## Idempotent processing -- real proof

Same crash point as the at-least-once example above
(`AFTER_PROCESS_BEFORE_COMMIT`), this time with `idempotent=true` (a
`ProcessedEventStore` keyed by `eventId`, checked before applying any
side effect):

```text
--- restart, fresh consumer instance, same group ---
consumer-2 | partition=0 | offset=7 | eventId=ORDER-1008 | attempt=1 | status=DUPLICATE_SKIPPED
consumer-2 | partition=0 | eventId=ORDER-1008 | commitSync -> COMMITTED committedOffset=8
```

Kafka redelivered `ORDER-1008` -- that part is identical to the
non-idempotent case above. The difference is entirely in the
application: the redelivered record's offset is still committed (so the
consumer does not spin on it forever), but its business side effect is
**not** reapplied, because the durable idempotency store already has a
record of it. This is the idempotent-consumer pattern in its simplest
possible form: **Kafka's delivery guarantee (at-least-once) plus an
application-level "have I done this before?" check equals an observed
exactly-once *effect*, without Kafka itself ever providing exactly-once
delivery.** That distinction is not pedantry -- see "The dual-write
problem," below, for exactly where this simple version stops being
sufficient.

## Delivery semantics comparison

| Semantic | Typical sequence | Main risk |
|---|---|---|
| At-most-once | commit → process | A crash between commit and process permanently loses the record; the group will never fetch it again. |
| At-least-once | process → commit | A crash between process and commit causes the record to be redelivered and reprocessed; a duplicate side effect unless the application is idempotent. |
| Exactly-once (Kafka's own, `read_committed` + transactions/idempotent producer) | Producer-side deduplication + atomic multi-partition/topic writes, `read_committed` isolation for consumers | Precise and real **within Kafka**: no duplicate records from producer retries, no partial transactional reads. Says nothing about what your application does with a record once delivered. |

**This repository deliberately does not claim ordinary offset commits
alone provide end-to-end exactly-once *business* semantics**, and does
not claim Kafka's own EOS (reserved for WP-09) closes that gap either.
Kafka's exactly-once semantics guarantee things entirely internal to
Kafka -- what a producer wrote, and what a `read_committed` consumer
sees. The moment "exactly-once" is asked to mean "the database was
updated exactly once" or "the email was sent exactly once," that is a
claim about your application's side effects, which Kafka has no
visibility into and no mechanism to guarantee, with or without
transactions.

## The dual-write problem

```text
Kafka record fetched
        |
        v
application updates its own database
        |
        v
  [CRASH HERE]
        |
        v
offset was never committed
        |
        v
RESTART -> record redelivered
        |
        v
application updates the database AGAIN
```

This is the same at-least-once replay window from earlier in this
document, but pointed at an external database instead of at this lab's
in-memory logging. The offset commit and the database write are two
separate operations against two separate systems, with no shared
transaction between them -- Kafka's own commit protocol has no way to
know about, participate in, or roll back an arbitrary external database
transaction, and never will, regardless of which delivery semantic or
commit strategy is chosen on the Kafka side alone. This is precisely
why "commit the offset" and "the business effect happened exactly once"
are different claims, and why this document keeps them visibly separate
throughout.

Three named patterns exist to close this gap, each with a genuinely
different responsibility -- named here so each has a place, not
implemented here because a correct treatment of any of them needs more
than this document has room for:

- **Idempotent consumer** (this lab's simple version, above): make the
  *business effect* safe to apply more than once, by checking a durable
  record of "have I done this before" keyed by something stable in the
  event itself (an `eventId`, a natural business key). Solves duplicate
  *effects*; does nothing about the write to that durable record itself
  potentially being the very dual-write problem one level down (which is
  why a real implementation needs the effect and the idempotency marker
  written in the *same* local database transaction -- see WP-13).
- **Transactional outbox** (WP-12): flip the direction entirely -- write
  the *intent* to publish an event into the same local database
  transaction as the business change, and let a separate process (built
  on CDC, WP-11) reliably turn outbox rows into Kafka records
  afterward. Solves "the database write and the fact that an event
  should be published are atomic with each other"; the Kafka publish
  itself is still at-least-once, so a consumer of it still needs its own
  idempotency handling.
- **Kafka transactions** (WP-09): make a producer's writes across
  multiple partitions/topics atomic, and let `read_committed` consumers
  see only committed transactional writes. Solves atomicity *within
  Kafka*, for producers that need to publish to several places as one
  unit (including the "consume, transform, produce" pattern) -- it does
  not, on its own, make an arbitrary external database write atomic with
  a Kafka publish, which is exactly why the outbox pattern above exists
  as a separate, composable piece rather than being subsumed by
  transactions.

## Rebalance and commit strategy: they cannot be designed separately

Real, unplanned, and instructive: `commitTiming=AFTER_BATCH` combined
with a slow per-record processing delay (`processingDelayMs=1500`,
24 records, `max.poll.interval.ms` at its own default of 20000)
demonstrated the following live during this lab's own validation:

```text
consumer-1 | partition=1 | offset=4 | eventId=ORDER-1200 | status=SUCCESS
consumer-1 | partition=1 | offset=5 | eventId=ORDER-1204 | status=SUCCESS

ConsumerCoordinator - [Consumer clientId=consumer-1, ...] Failing OffsetCommit
  request since the consumer is not part of an active group

Exception in thread "main" org.apache.kafka.clients.consumer.CommitFailedException:
  Offset commit cannot be completed since the consumer is not part of an active
  group for auto partition assignment; it is likely that the consumer was kicked
  out of the group.
```

What happened, exactly: `consumer-1` spent longer than
`max.poll.interval.ms` inside a single poll-batch's processing loop
(24 records × 1.5s each, with no intervening `poll()` call, well past the
20-second budget). Its own background heartbeat thread detected the
violation and proactively left the group on its behalf -- but the
foreground loop had no way to know that yet, and kept processing every
record successfully. Only at the very end, when `AFTER_BATCH` finally
tried to commit the entire batch in one call, did Kafka reject it:
`CommitFailedException`. **Every one of those 24 records' business
effects had already happened. None of their offsets were ever
committed.** A second consumer that joined afterward started from
offset 0 and began replaying the entire batch.

This is the concrete version of an abstract warning: **a commit
strategy that defers all commits to the end of a large batch is making
an implicit bet that the batch will finish inside `max.poll.interval.ms`
(and, separately, inside `session.timeout.ms` if heartbeating were also
affected). A rebalance -- whether from this consumer being evicted, a
new member joining, or an existing member leaving -- can invalidate an
entire batch's pending commit in one blow, no matter how much of it
already succeeded.** Choosing `AFTER_BATCH` without also choosing
`max.poll.records`/batch size, per-record processing cost, and
`max.poll.interval.ms` together is choosing this failure mode, not
avoiding it.

The mechanical half of this -- what actually changes hands during a
rebalance, and when `PARTITIONS_REVOKED`/`PARTITIONS_ASSIGNED` fire -- is
WP-05's territory and is not re-derived here; see
[`docs/consumer-groups/CONSUMER_GROUPS_AND_REBALANCING.md`](../consumer-groups/CONSUMER_GROUPS_AND_REBALANCING.md).
What this lab adds is specifically the commit-strategy interaction shown
above: from a new partition owner's perspective, a reassignment and a
plain restart are mechanically identical -- both mean "begin fetching
this partition from its last committed offset" -- so everything this
document already showed about what does and doesn't survive a restart
applies verbatim to what does and doesn't survive a rebalance.

### A commit-on-revoke design that looked right and was not

Building this lab's rebalance experiment, an earlier version of
`DeliverySemanticsApp` committed `consumer.position(tp)` synchronously
inside `onPartitionsRevoked`, on the theory that flushing in-flight
progress before a partition changes hands shrinks the replay window.
Real testing caught this as wrong: `consumer.position(tp)` reflects how
far the client has *fetched*, not how far the application has decided is
*safe to commit* -- and a batch-boundary run that deliberately withheld
a commit after a mid-batch failure had its "withheld" offsets committed
anyway the moment `consumer.close()` triggered that revoke handler,
because `poll()` had already fetched past the failure point in one call.
The next consumer to join that group found nothing left to consume and
blocked in `poll()` indefinitely -- confirmed with a thread dump showing
the main thread parked inside `ClassicKafkaConsumer.poll`. The listener
now only logs; see
[`DeliverySemanticsApp.CommitOnRevokeListener`](../../labs/lab-05-offset-management-delivery-semantics/src/main/java/com/kafkalab/deliverysemantics/consumer/DeliverySemanticsApp.java)'s
Javadoc for the full account, including why this lab's specific
synchronous, single-threaded loop design has no genuine "processed but
uncommitted" window for a revoke handler to rescue in the first place --
that window is real for architectures where processing happens on a
separate thread from the poll loop, which is a different, more complex
design this lab does not use.

## Production considerations

| Lab | Production |
|---|---|
| `System.exit(1)` on a configured `FailurePoint` | A real crash: OOM kill, node failure, `kill -9`, a scheduler evicting the pod |
| A flat file `ProcessedEventStore`, one process, no concurrency | A real deduplication store (database table with a unique constraint, or a keyed cache with appropriate TTL), sized for actual event volume and retention needs |
| `commitTiming`/`failurePoint`/`crashAtEventId` chosen deliberately per experiment | One commit strategy, chosen deliberately once per consumer, matched to that consumer's actual duplicate-cost and throughput requirements |
| Manually triggered rebalances (join/evict) | Rebalances driven by real deploys, autoscaling, and genuine node failure, at a cadence this lab cannot simulate |
| Single-partition and small multi-partition topics | Partition counts chosen for real throughput and consumer-parallelism needs (WP-04's territory) |
| No monitoring beyond this lab's own stdout logging | Consumer lag, commit-failure rate, and rebalance frequency all alerted on in production (WP-16) |

## Principal Engineer questions

**1. What is the difference between "processed" and "committed"?**

"Processed" is an application-level claim -- your business logic ran
against this record. "Committed" is a durable, group-scoped fact stored
in `__consumer_offsets` about where this group will resume from. This
lab's crash-injection framework exists specifically to force these two
apart on demand: `AFTER_PROCESS_BEFORE_COMMIT` processes without
committing (the at-least-once window); `AFTER_COMMIT_BEFORE_PROCESS`
commits without processing (the at-most-once window). Real production
code has exactly the same gap between the two at all times; a crash
just makes it visible.

**2. Why does at-least-once require idempotent consumers?**

Because "at least once" is a lower bound, not an upper bound -- Kafka
promises a record will not be lost as long as it isn't committed before
being finished, but makes no promise about how many times it might be
redelivered after a crash in that window (this lab's real result: one
duplicate from one crash, but nothing in the mechanism caps it at one).
Without an idempotency check, every one of those redeliveries reapplies
whatever side effect the business logic performs. This lab's idempotent
experiment shows the fix is not "prevent redelivery" (impossible without
also risking loss) but "make redelivery harmless" (a durable check
before applying the effect).

**3. When would you choose `earliest` vs. `latest` for
`auto.offset.reset`?**

`earliest` for anything where completeness matters more than
recency -- a new consumer group backfilling historical state, an
analytics pipeline, any case where "process everything retained" is the
correct behavior for a group that has never committed. `latest` for
anything where only fresh events matter and reprocessing history would
be actively wrong -- a live dashboard, a notification service, anything
where replaying months of history on every new deployment of a
never-before-seen group id would be a bug, not a feature. The two real
results in this document's "Offset fundamentals" section show exactly
what each choice does on first join: `earliest` immediately consumed
everything retained; `latest` (documented, not separately re-run) would
have started at the high watermark and seen nothing until new production
began.

**4. What's the danger of `commitAsync()` retries specifically?**

An async commit can fail and its automatic (or manually coded) retry can
arrive at the broker *after* a later, already-succeeded commit for the
same partition -- silently regressing the committed offset backward,
which reopens a replay window that had already been closed. The
documented-safe pattern tracks a local, monotonically increasing commit
counter and discards any retry whose counter is no longer the latest one
issued, specifically to prevent this.

**5. Why is `commitTiming=BEFORE_PROCESS` (commit-then-process) rarely
the right default?**

Because it inverts the risk this document's two failure-window diagrams
make concrete: it guarantees a record is never reprocessed, at the cost
of guaranteeing a crash in the (however small) window between commit and
processing loses that record permanently, with no mechanism to ever
recover it. This lab's real at-most-once result shows the loss is not
theoretical or rare-in-practice-only -- it reproduces on demand, every
time, at exactly the configured failure point. The only scenarios where
this trade is defensible are ones where losing a record occasionally is
strictly preferable to ever reprocessing one -- genuinely rare in
business systems, though real for some telemetry/best-effort-metrics
use cases.

**6. How do you decide the right commit granularity (per-record vs.
per-batch)?**

Weigh the real, measured trade-off this document's batch-boundary
section shows directly: per-record commit (`AFTER_PROCESS`) bounds the
replay set to the exact records not yet committed at the moment of
failure (this lab's real result: 2 records), at the cost of one network
round trip per record. Per-batch commit (`AFTER_BATCH`) trades that
throughput cost away, at the cost of a strictly larger replay set on any
mid-batch failure (this lab's real result: 5 records, the entire batch)
-- and, as the rebalance section shows, an even larger and less
predictable cost if the batch takes long enough to risk a rebalance
mid-flight. Choose smaller batches (down to per-record) as the cost of
reprocessing a single record rises, and larger batches as that cost
falls toward "cheap and idempotent anyway."

**7. What does Kafka's exactly-once semantics actually guarantee, and
what does it not?**

It guarantees things entirely internal to Kafka: a producer's retried
send does not create a duplicate record (idempotent producer), and a set
of writes across multiple partitions/topics either all become visible to
a `read_committed` consumer or none do (transactions). It says nothing
about what an application does with a record once delivered -- a
database write, an email, a charge are all outside Kafka's transaction
boundary regardless of whether the *publish* to Kafka happened inside
one. This document's delivery-semantics comparison table states this
distinction explicitly because conflating "Kafka EOS" with "my business
logic ran exactly once" is one of the most common Kafka
misunderstandings at every experience level.

**8. What is the dual-write problem, concretely?**

Any sequence where a Kafka offset commit and an external system's write
(most often a database) are two independent operations with no shared
transaction: "update the database, then commit the offset" can crash
between the two steps, leaving the database updated but the offset
uncommitted -- the record replays and the database is updated again.
Reordering to "commit the offset, then update the database" just moves
the same risk to the opposite failure (offset committed, database update
lost, and since the offset already advanced, it is lost for good). No
ordering of these two independent operations removes the gap between
them; only making them not-independent (an outbox, a transaction, an
idempotency check) does.

**9. Why can't ordinary offset commits alone solve the dual-write
problem?**

Because an offset commit is a write to exactly one system --
`__consumer_offsets` inside Kafka -- and the dual-write problem is
specifically about coordinating that write with a write to a
*different*, external system. No amount of choosing when within the
Kafka-side loop to commit (before processing, after processing, per
record, per batch) changes the fact that the external write and the
Kafka commit remain two separate operations that can fail
independently, in either order, with nothing to make them atomic with
each other. That coordination has to come from something that spans both
systems -- an idempotency check the external write itself makes safe to
retry, or a pattern (outbox, transactions) that changes what "atomic"
even means for the boundary in question.

**10. What's the actual difference between the idempotent-consumer
pattern and the transactional outbox pattern?**

They solve the dual-write problem from opposite ends. Idempotent
consumer (this lab): the write into Kafka already happened normally
(at-least-once, possibly duplicated); make the *consumer's* side effect
safe to apply more than once. Transactional outbox (WP-12): the write
that needs to happen atomically with a database change is the *publish
to Kafka itself*; make that publish part of the same local database
transaction as the business change (by writing an outbox row instead,
atomically, and relaying it to Kafka afterward), so the "was this
published" question never has a dual-write gap in the first place. A
system can reasonably need both: an outbox to make publishing reliable,
and an idempotent consumer on the other end because the outbox relay
process is itself at-least-once.

**11. If a consumer's own local idempotency check (the flat file /
database row this lab writes to) is itself written by a separate,
non-transactional operation from the business side effect, doesn't that
just move the dual-write problem one level down?**

Yes, precisely -- and this document says so explicitly in "The dual-write
problem" section rather than glossing over it. This lab's
`ProcessedEventStore.markProcessed()` call and its business "side
effect" (here, just a log line) are not wrapped in any shared
transaction, so in principle a crash between them could leave the
effect applied without the marker, or the marker written without the
effect. A correct production idempotent consumer closes this by writing
the marker and the effect in the *same local transaction* against
whatever datastore the effect itself lives in (a `processed_events`
table with a `UNIQUE(event_id)` constraint, written in the same
transaction as the business row it protects) -- which is WP-13's
territory, not reproduced here because doing it correctly needs a real
transactional datastore this lab does not set up.

**12. How does static membership (`group.instance.id`, from WP-05)
interact with commit strategy?**

It doesn't change what gets committed or when -- it changes whether a
*temporary* disconnection (a rolling restart, a brief network blip)
triggers a rebalance at all. Without it, any disconnection longer than
the missed-heartbeat detection window causes a real rebalance, with all
of this document's commit-strategy-vs-rebalance interactions in play.
With it, the same brief disconnection can rejoin using the same member
identity within `session.timeout.ms` and keep its prior partition
assignment, with no rebalance and therefore none of this document's
rebalance-interaction risk -- at the cost of that partition sitting idle
for the disconnected window instead of failing over immediately, which
is WP-05's own trade-off, unchanged by anything in this document.

**13. Why does the batch-boundary experiment show a strictly worse
outcome under `AFTER_BATCH` than `AFTER_PROCESS`, and is `AFTER_BATCH`
ever still the right choice?**

It's strictly worse *for replay size* in this document's specific
side-by-side result (5 records vs. 2), because `AFTER_BATCH` by
definition withholds every record's commit until the whole batch
succeeds. It remains the right choice when per-record commit's network
overhead is the actual bottleneck and reprocessing a whole batch is
cheap -- specifically, when records are naturally idempotent (so a
larger replay set costs CPU, not correctness) or when batch failures are
rare enough that the throughput win dominates the occasional larger
replay. The real evidence in this document is for choosing with that
trade-off explicit, not for treating `AFTER_BATCH` as universally worse.

**14. What would you actually check first if a consumer group's lag
suddenly spiked after a deploy?**

Whether the deploy changed commit strategy, batch size, or per-record
processing cost in a way that pushes processing time closer to
`max.poll.interval.ms` -- this document's real rebalance-interaction
result is exactly that failure mode (a batch that used to finish in time
no longer does, gets evicted mid-batch, and an entire uncommitted batch
replays under a new owner, compounding the original lag with a full
batch's worth of reprocessing). Check for `CommitFailedException` and
proactive `LeaveGroup due to consumer poll timeout has expired` in logs
before assuming the lag spike is purely a throughput problem -- it may
be a rebalance-churn problem caused by a commit-strategy change,
exactly as this document's own validation run happened to discover.
