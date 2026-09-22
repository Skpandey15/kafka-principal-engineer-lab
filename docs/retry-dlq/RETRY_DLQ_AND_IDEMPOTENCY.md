# Retry, DLQ & Idempotency (WP-13)

## Prerequisites

- [`docs/delivery-semantics/DELIVERY_SEMANTICS_AND_OFFSET_MANAGEMENT.md`](../delivery-semantics/DELIVERY_SEMANTICS_AND_OFFSET_MANAGEMENT.md)
  (WP-06) -- introduces the idempotent-consumer check this WP takes to
  its full production depth.
- [`docs/outbox/TRANSACTIONAL_OUTBOX.md`](../outbox/TRANSACTIONAL_OUTBOX.md)
  (WP-12) -- conceptually anchored directly before this WP in the
  curriculum map (row 13 follows row 12).

## 1. From WP-06's `ProcessedEventStore` to a real table

WP-06's `ProcessedEventStore` (`labs/lab-05-offset-management-delivery-semantics`)
is a single-process, file-backed `Set<String>`: `isProcessed(eventId)`
then, separately, `markProcessed(eventId)`. That's a real
**check-then-act race** -- nothing stops two concurrent callers from both
calling `isProcessed` before either calls `markProcessed`, both seeing
"not yet processed," and both applying the side effect.

This WP's `IdempotencyStore` replaces the check-then-act pair with ONE
atomic statement:

```sql
INSERT INTO processed_events (group_id, event_id, status, claimed_at)
VALUES (?, ?, 'IN_PROGRESS', now())
ON CONFLICT (group_id, event_id) DO NOTHING
```

The database's own `PRIMARY KEY (group_id, event_id)` constraint is the
arbiter -- at most one concurrent `INSERT` can ever succeed for the same
key, full stop. `concurrentDuplicateClaimsAreSafelySerializedByTheUniqueConstraint`
proves this directly: two threads race to claim the SAME `eventId`
through two independent JDBC connections, and the real, verified
outcome is that business logic runs exactly once, no matter which
thread's `INSERT` the database happened to order first.

## 2. A claim lifecycle, not a boolean

`processed_events.status` moves through three real states:

```text
(no row)  --tryClaim-->  IN_PROGRESS  --complete-->  DONE
                              |
                              +--release--> (row deleted)
```

- **CLAIMED** -- a fresh `IN_PROGRESS` row; the caller now owns this
  event_id and must call `complete` or `release`.
- **ALREADY_DONE** -- a true duplicate (redelivery of an event whose
  side effect already succeeded); skip entirely, no retry, no DLQ.
- **IN_PROGRESS_ELSEWHERE** -- another attempt currently owns this
  event_id and hasn't finished; skip (see Section 4 for what happens
  when that "other attempt" crashed and never finishes).

## 3. Retry with backoff, then the DLQ

`RetryingRecordProcessor` retries a claimed event's business logic up
to `maxAttempts` times, sleeping `baseBackoff * attempt` between
attempts (linearly growing backoff, deliberately simple over a jittered
exponential curve -- see Section 8 for why a real implementation should
do better). `aTransientlyFailingMessageIsRetriedWithBackoffThenSucceeds`
is real, timed evidence: a message configured to fail exactly twice
then succeed takes measurably longer than the sum of its backoff
windows (150ms + 300ms), because the delays actually elapsed, not
because the test asserts a mocked clock.

When every attempt fails, the claim is **released** (its `IN_PROGRESS`
row deleted, not left stuck) and the record is published to a DLQ topic
with headers mirroring Spring Kafka's real
`DeadLetterPublishingRecoverer` convention:

| Header | Content |
|---|---|
| `kafka_dlt-exception-fqcn` | the failure's exception class name |
| `kafka_dlt-exception-message` | the failure's message |
| `kafka_dlt-original-topic` | the source topic |
| `kafka_dlt-original-partition` | the source partition |
| `kafka_dlt-original-offset` | the source offset |

`anAlwaysFailingMessageExhaustsRetriesIsRoutedToTheDlqAndItsClaimIsReleased`
confirms both halves: exactly `maxAttempts` real attempts are made
(no more, no fewer), AND the claim row is gone afterward -- a later,
manually-fixed redelivery of the same event_id would be free to try
again, not permanently blocked.

## 4. Poison messages: a different failure class entirely

A **poison message** here means the payload itself can never be parsed
into an `OrderEvent` at all -- `OrderEvent.parse` throws
`IllegalArgumentException` for malformed wire-format text, deterministically,
every single time. Retrying a poison message teaches nothing new on
attempt 2 that attempt 1 didn't already prove: it will fail identically
forever. `RetryingRecordProcessor` recognizes this and skips the retry
loop and the idempotency claim ENTIRELY -- there's no reliable eventId
to claim in the first place -- and routes straight to the DLQ on the
very first attempt. `aPoisonMessageIsRoutedToTheDlqImmediatelyWithOriginalHeadersAndPayload`
confirms the DLQ record's value is byte-for-byte the original malformed
payload, with the same real `kafka_dlt-*` headers as an exhausted-retry
record.

## 5. Why this matters: partition-blocking

If a poison (or permanently-failing) message had NO DLQ path, and the
consumer only committed offsets past records it successfully processed,
that ONE message would block its entire partition forever -- every
subsequent record behind it in the same partition would never even be
attempted. `consumptionContinuesPastAPoisonMessageToTheNextRecordOnTheSamePartition`
proves the DLQ path avoids this concretely: a poison record and a valid
record are produced back-to-back on the same single-partition topic,
and the real consumer loop (`consumeAndProcessAll`, using genuine
`consumer.poll`/`commitSync`, not a hand-fed list) processes BOTH --
the poison one lands on the DLQ, and the valid one that follows it is
still applied.

## 6. A real crash scenario the claim lifecycle has to handle

What if the process holding an `IN_PROGRESS` claim crashes mid-attempt --
after `tryClaim` succeeds, but before it ever calls `complete` or
`release`? An unconditional "IN_PROGRESS blocks forever" design would
mean that event_id can NEVER be processed again by this consumer group,
a genuine, silent correctness bug far worse than an occasional
duplicate. `tryClaim` handles this with a lease: an `IN_PROGRESS` claim
older than `claimLeaseTimeout` is eligible for atomic reclaim (another
conditional `UPDATE ... WHERE claimed_at < now() - lease`, same
single-statement-is-the-arbiter approach as the original claim).
`anOrphanedInProgressClaimOlderThanTheLeaseWindowCanBeReclaimed` proves
both halves deterministically: a claim well within its lease window is
correctly treated as still-owned (`IN_PROGRESS_ELSEWHERE`), and the
SAME claim, once past a short lease window, is successfully reclaimed.

## 7. The dual-write problem, again, in miniature

Committing `processed_events.status = 'DONE'` and committing the Kafka
consumer offset are still two independent systems -- the SAME class of
problem WP-12's outbox pattern exists to solve for a business database
and a Kafka publish. This lab deliberately commits the DB row FIRST,
THEN the Kafka offset (see `RetryDlqConsumerApp`): a crash between the
two leaves a `DONE` row with an uncommitted offset, so the consumer
group redelivers the same record on restart -- and the idempotency
claim correctly recognizes it as `ALREADY_DONE` and skips it. Ordering
it the other way (commit the offset first, write `DONE` second) would
be strictly worse: a crash in that window would advance past the
record's offset with NO durable proof the side effect ever happened --
an unrecoverable business-logic loss, not just a redundant retry.
`duplicateEventIsProcessedOnlyOnceEvenAfterRedeliveryAfterACrashBeforeOffsetCommit`
models exactly this ordering and confirms the redelivered record is
correctly skipped, with business logic invoked exactly once.

## 8. What this lab does NOT build

- **Jittered exponential backoff** -- `baseBackoff * attempt` is linear,
  not exponential-with-jitter; a real production retry policy should
  add both (exponential to avoid retry storms overwhelming a struggling
  downstream, jitter to avoid many consumers retrying in lockstep).
  Left simple here to keep the retry timing this lab asserts on
  deterministic and fast.
- **A lease reaper / background sweep** -- Section 6's reclaim only
  happens lazily, the next time SOMETHING calls `tryClaim` for that
  exact event_id. An event_id whose owning process crashed and is never
  retried by anything (no redelivery, ever) stays `IN_PROGRESS` forever
  with nothing noticing. A real system would pair this with monitoring
  (alert on `IN_PROGRESS` rows older than N × the lease window) or a
  scheduled sweep.
- **DLQ reprocessing tooling** -- this lab produces TO the DLQ and
  proves what lands there; it does not build the operational side (a
  tool to inspect, fix, and replay DLQ'd records back onto the original
  topic). Real DLQ operations always need this half too.
- **Cross-partition or cross-topic idempotency** -- `group_id` scopes
  the table to one consumer group's business logic (mirroring WP-06's
  own scoping choice), not to a specific partition or topic; this is a
  deliberate, correct scope (the eventId identifies a business
  occurrence, independent of which partition happened to carry it,
  exactly as `OrderEvent`'s own Javadoc, inherited from WP-06, already
  says).

## 9. Principal Engineer questions

**1. Why is `ON CONFLICT DO NOTHING` + a follow-up `SELECT` safer than a
plain application-level `if (!store.contains(id))` check, even against
the SAME single-process consumer WP-06 used?** It isn't just about
concurrency within one process -- it's about SURVIVING A CRASH between
the check and the act. A plain in-memory check is also wiped out by a
crash (WP-06's own file-backed store exists for that reason); the
atomicity this WP adds is specifically for when MULTIPLE consumer
instances (or a rebalance handing the same partition to a new instance
mid-flight) can genuinely run concurrently, which WP-06's single-process
lab never needed to model.

**2. What happens if `complete()` is called for an event_id that was
never claimed?** It's a silent no-op -- the `UPDATE` affects zero rows.
Not tested explicitly here since it shouldn't happen given
`RetryingRecordProcessor`'s own call ordering, but worth naming: this
class trusts its caller's ordering rather than defending against
mis-use, an intentional simplicity trade-off for a lab-scale
implementation.

**3. Why release the claim on DLQ, rather than marking it a THIRD
status like `FAILED`?** Because `FAILED` would still need SOME later
mechanism to decide the record is eligible for reprocessing once fixed
-- deleting the row makes "eligible for reprocessing" the DEFAULT state
again (no row = never claimed), which is simpler and correct for this
lab's scope. A system with automated DLQ replay tooling (Section 8)
might reasonably want `FAILED` instead, to distinguish "never
attempted" from "attempted and gave up," for observability.

**4. Why does a poison message skip the idempotency claim entirely,
while an always-failing-but-parseable message goes through claim →
retry → release?** The idempotency table is keyed by `eventId`, which
only exists once the payload is successfully parsed. A message that
can't even be parsed has no reliable key to claim in the first place --
claiming on, say, the raw Kafka key (which may be null, or shared
across unrelated malformed records) would give false duplicate-detection
guarantees.

**5. This lab's retry loop blocks the polling thread with `Thread.sleep`
between attempts -- what's the real cost of that?** It stalls the ENTIRE
consumer (no other partition's records are polled or processed) for the
duration of every retry backoff, on top of not calling `poll()` again
within `max.poll.interval.ms` risking a rebalance. A production
implementation typically retries out-of-band (a separate retry topic
with increasing delay, consumed by a dedicated retry consumer) precisely
to avoid blocking the main partition's throughput on one record's
retry schedule -- named here as a deliberate simplification, not a
gap this lab's tests hide.

**6. How would you extend the claim lease (Section 6) to be safe against
clock skew across consumer instances?** Use the DATABASE's own clock for
every comparison (`now()` inside the SQL, exactly as this lab does),
never a client-supplied timestamp -- every claiming process agrees on
one authoritative clock (Postgres's), so no instance's local clock drift
can cause an incorrect early or late reclaim.

**7. Why is `group_id` part of the primary key instead of a separate,
smaller lookup table per group?** Two different consumer groups
legitimately processing the SAME topic (each with its own, independent
business logic and its own idempotency boundary) must never see each
other's claims -- WP-06's own `ProcessedEventStore.forGroup` already
established this scoping via one file per group; this WP's composite
key achieves the same isolation in one shared table instead of one file
per group.
