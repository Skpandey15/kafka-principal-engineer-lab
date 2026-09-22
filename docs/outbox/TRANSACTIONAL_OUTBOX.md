# Transactional Outbox (WP-12)

## Prerequisites

- [`docs/transactions/TRANSACTIONS_AND_EXACTLY_ONCE_SEMANTICS.md`](../transactions/TRANSACTIONS_AND_EXACTLY_ONCE_SEMANTICS.md)
  -- names the dual-write problem and previews the outbox pattern as one
  of its solutions (Section 19).
- [`docs/kafka-connect/KAFKA_CONNECT_AND_CDC.md`](../kafka-connect/KAFKA_CONNECT_AND_CDC.md)
  (WP-11) -- this WP is built DIRECTLY on that WP's CDC pipeline: same
  `platform/kafka-connect/` environment, same Debezium PostgreSQL
  connector, same Kafka Connect worker.

## 1. The dual-write problem, precisely

An application that needs to (a) change its own database AND (b)
publish an event about that change to Kafka is making TWO independent
writes to two independent systems, with no shared transaction spanning
both. Four orderings are possible once you draw the timeline out:

1. DB commits, Kafka publish succeeds -- the happy path.
2. DB commits, Kafka publish fails or the process crashes first -- a
   real, committed business change with **no** corresponding event.
3. DB commit fails, Kafka publish never happens -- consistent, if
   uninteresting.
4. DB commit fails AFTER a Kafka publish already succeeded (e.g. publish
   first, then write) -- a real event for a change that never actually
   happened.

Case 2 is what this lab's `NaiveDualWriteApp` demonstrates directly, and
what `naiveDualWriteLosesTheKafkaEventWhenTheProcessCrashesAfterTheDbCommit`
proves deterministically: a business row committed to Postgres with zero
corresponding Kafka events, because the process never even reached the
`KafkaProducer` call.

Kafka's own producer guarantees (idempotence, transactions, WP-08) do not
help here -- they make Kafka-internal delivery reliable, but they have no
reach into a SEPARATE database's transaction at all (see the transactions
doc, Section 17, for the same point made from Kafka's own side).

## 2. The outbox pattern's fix

Write the business change AND a row describing "an event needs to be
published" **in the same local database transaction**. Either both
commit together, or neither does -- ordinary ACID guarantees the
database was already giving you, now covering the publish INTENT as
well as the business data.

```sql
BEGIN;
INSERT INTO outbox_demo_orders (order_id, customer_id, amount) VALUES (...);
INSERT INTO outbox_event (aggregatetype, aggregateid, type, payload) VALUES (...);
COMMIT;
```

`OutboxWriterApp` and this lab's
`outboxRowAndBusinessRowCommitOrRollbackTogetherInOneTransaction` test
both demonstrate this directly: force a failure between the two inserts
and the commit, and NEITHER row survives.

This only solves HALF the problem, though -- it makes the "intent to
publish" durable and atomic with the business change, but something
still has to turn that outbox row into an actual Kafka message. That's
where WP-11's CDC pipeline comes back in.

## 3. Why CDC is the reliable way to drain the outbox

A naive outbox relay -- a separate poller process that periodically
`SELECT`s unpublished rows, produces them to Kafka, then marks them
published -- reintroduces its own version of the SAME dual-write problem
(what if the process crashes between the Kafka `send()` and marking the
row published?), just with a longer window and an extra "published"
column to manage.

Debezium's PostgreSQL connector (WP-11) instead reads the row directly
off the write-ahead log -- the row's INSERT is already durable the moment
it's in the WAL, and CDC is driven by that durable log, not by any
relay process's own liveness. This lab's
`outboxEventIsCapturedEvenWhenTheWritingProcessNeverTalksToKafka` test
proves this concretely: it writes the outbox row, and only registers the
Debezium connector **afterward** -- the connector still picks up the
already-committed row (via `snapshot.mode=initial`), because publication
never depended on the writing process staying alive or ever creating a
`KafkaProducer` at all. `OutboxWriterApp` itself never imports
`org.apache.kafka.clients.producer.KafkaProducer`.

## 4. The Outbox Event Router transform

Debezium ships a purpose-built single message transform for exactly this
pattern: `io.debezium.transforms.outbox.EventRouter` -- confirmed
present in the SAME plugin bundle WP-11 already downloaded (real
evidence: `unzip -l debezium-connect-plugins-3.6.3.Final.jar | grep
outbox` lists `io/debezium/transforms/outbox/EventRouter.class` and
its supporting classes). No separate plugin fetch was needed for this
WP.

Real, verified config keys (extracted directly from
`EventRouterConfigDefinition.class`'s constant pool, not from
documentation that could be stale against 3.6.3.Final):

| Config key | Default | What it does |
|---|---|---|
| `route.by.field` | `aggregatetype` | which outbox column decides the destination topic |
| `route.topic.replacement` | `${routedByValue}` | the destination topic name template |
| `table.field.event.id` | `id` | the outbox row's own primary key column |
| `table.field.event.key` | `aggregateid` | becomes the routed Kafka record's KEY |
| `table.field.event.payload` | `payload` | becomes the routed Kafka record's VALUE |
| `table.expand.json.payload` | `false` | whether to parse the payload column into a structured schema, or leave it as a raw JSON string |

This lab's connector (`OutboxConnectorRegistrationApp`,
`registerOutboxConnector` in the test suite) sets
`transforms.outbox.route.topic.replacement=outbox.event.${routedByValue}`,
so a row with `aggregatetype='Order'` routes to `outbox.event.Order` and
one with `aggregatetype='Customer'` routes to `outbox.event.Customer` --
confirmed by `debeziumRoutesOutboxEventsToATopicNamedByAggregateTypeWithTheOriginalPayload`,
which writes both in the same run and asserts each lands on its own
topic with its own payload intact.

**Crucially, `table.include.list` names only `public.outbox_event`** --
never the business table (`outbox_demo_orders` in this lab). Debezium
never even reads the business table's WAL changes; only the outbox
table is a replication source. `outboxDemoOrdersTableChangesAreNeverCapturedOnlyOutboxEventIs`
proves this: a direct write to the business table alone produces zero
events on any `outbox.event.*` topic.

## 5. What the routed record actually looks like on the wire

Unlike WP-11's raw CDC topics (whose value is Debezium's full
`op`/`before`/`after`/`source` envelope, decoded via `Struct`), the
EventRouter-routed record's value is **only the outbox row's own
`payload` column content** -- with `table.expand.json.payload` left at
its default (`false`), that's a plain JSON **string**, not a structured
`Struct`. `OutboxEventConsumerApp` and the test suite's `payloadValue`
helper both decode it via the same `JsonConverter.toConnectData` call
WP-11 used, but the returned Java object this time is a `String`, which
then needs its OWN JSON parse (via Jackson) to read individual fields --
two layers of JSON, not one.

## 6. A real finding: PostgreSQL's `jsonb` re-serializes with a space

Building `debeziumRoutesOutboxEventsToATopicNamedByAggregateTypeWithTheOriginalPayload`,
a first version asserted the routed payload contained the raw substring
`"orderId":"O-ROUTE-..."` and failed -- `expected: <true> but was:
<false>` -- even though a looser substring check for the ID alone
(inside `containsValue`, used to find the right record in the first
place) had already succeeded. Root cause: the `outbox_event.payload`
column is `JSONB`, and PostgreSQL's `jsonb` output function
re-serializes on every read with a space after each `:` and `,` --
`'{"a":"b"}'::jsonb` round-trips as `{"a": "b"}`, not byte-identical to
what was inserted. The fix: parse the payload as real JSON (Jackson's
`ObjectMapper`) and assert on individual fields, not raw substrings --
both more correct and immune to this exact formatting detail. A real,
general lesson for anyone asserting against `jsonb` column content: never
assume byte-for-byte round-tripping.

## 7. Outbox table cleanup and `tombstones.on.delete`

The outbox table grows unboundedly unless something periodically deletes
already-captured rows -- a genuine operational responsibility this
pattern adds (usually a scheduled job, deleting rows older than some
retention window past their `created_at`). The one thing that deletion
must NOT do is produce a spurious event on the routed topic. This
connector sets `tombstones.on.delete=false` for exactly that reason --
without it, Debezium's normal CDC behavior (a tombstone record
immediately following any DELETE, see WP-11 Section 8) would follow
every cleanup deletion, polluting the routed topic with meaningless
tombstones for events consumers already processed.
`deletingAnAlreadyCapturedOutboxRowProducesNoFurtherEventOnTheRoutedTopic`
proves this: after the real captured event, the row is deleted, and a
bounded re-poll confirms the total matching-record count stays at
exactly 1 -- the delete produced nothing new.

## 8. Reliability guarantees actually delivered

| Guarantee | Delivered? | Why |
|---|---|---|
| Business change and "intent to publish" are atomic | Yes | one local DB transaction, ordinary ACID |
| Publication does not depend on the writer staying alive | Yes | CDC reads the durable WAL, not app memory |
| Every committed outbox row is EVENTUALLY published | Yes, at-least-once | same guarantee WP-11's CDC pipeline already gives; a resumed connector picks up from its last confirmed LSN (WP-11, Section 11) |
| Every committed outbox row is published EXACTLY once | **No** | at-least-once, same as any CDC pipeline -- a consumer restart, rebalance, or connector task retry can replay an already-seen event; see Section 9 |
| Events preserve the order they were written in, per aggregate | Yes, per key | WAL replay is ordered; the routed record's key is `aggregateid`, so per-key ordering is preserved by normal partitioning as long as `tasks.max=1` for this connector (a single-task PostgreSQL connector reads one WAL stream) |

## 9. What this pattern does NOT give you

- **Not exactly-once delivery** -- see Section 8. A consumer on the
  other end that cares about processing each event exactly once still
  needs its own idempotency mechanism (the idempotent-consumer topic,
  full treatment in WP-13) -- the outbox pattern's mirror image on the
  consuming side is the **inbox pattern**, named but not built in this
  WP (see the transactions doc, Section 19).
- **Not synchronous confirmation** -- the writing process gets no
  acknowledgment that its event was actually published; it only knows
  the outbox row was committed. If the caller genuinely needs
  "publish-or-tell-me-now" semantics, this pattern is the wrong tool.
- **Not free of operational cost** -- an extra table, an extra
  connector to run and monitor, and a cleanup job to build (Section 7).
  A codebase with only occasional cross-system consistency needs might
  reasonably choose a simpler pattern (e.g. Kafka transactions alone,
  WP-08, when the "other system" IS Kafka) instead.

## 10. Lab limitations

- No dedicated failure-matrix row names this WP specifically (checked
  against `PRINCIPAL_ENGINEER_FAILURE_MATRIX.md`) -- WP-11's own two
  rows (Connect task failure, source DB unavailability) already cover
  this WP's underlying CDC transport, since this WP reuses that same
  pipeline unchanged.
- The outbox cleanup job itself (the scheduled deletion process,
  Section 7) is not built here -- only its SAFETY property
  (`tombstones.on.delete=false`) is demonstrated. A real cleanup job is
  ordinary application code with no new Kafka concepts.
- `table.expand.json.payload` is left at its default (`false`) --
  routed events carry their payload as a raw JSON string, not a
  Connect-schema-aware structure. A production system publishing to
  schema-governed consumers (WP-09/WP-10) would likely set this `true`
  and pair it with Avro/Protobuf serialization on the way out; left out
  here to keep this WP focused on the outbox mechanism itself.

## 11. Principal Engineer questions

**1. Why does the outbox pattern insert into a SEPARATE table instead of
just publishing straight from the business table's own changes?**
Because the business table's schema is shaped for the application's own
query needs, not for what downstream consumers should see -- coupling
CDC directly to it means every schema change to `outbox_demo_orders`
(a column rename, a new internal-only field) becomes a breaking change
for every Kafka consumer. The outbox table is a deliberate, stable
publication contract, decoupled from internal storage shape.

**2. Why not just use Kafka transactions (WP-08) instead of an outbox
table?** Kafka transactions make a producer's writes atomic ACROSS
KAFKA topics and consumer offset commits -- they have no reach into a
separate database's transaction at all. The outbox pattern exists
specifically for the case Kafka transactions cannot reach: coordinating
a Kafka publish with a change to a system Kafka doesn't participate in.

**3. What happens if the SAME outbox row gets processed twice by
Debezium (a connector task restart mid-stream, for instance)?** The
routed topic sees the same event twice -- at-least-once, not
exactly-once (Section 8). Whether that's a problem depends entirely on
the consumer: an idempotent consumer (keyed on the outbox row's own
`id`, WP-13) shrugs it off; a naive one double-processes.

**4. Why is `table.include.list` scoped to ONLY `outbox_event`, never
the business table -- what would go wrong if it included both?**
Debezium would then ALSO emit raw CDC events for the business table's
own inserts/updates, going out on a `<topic.prefix>.public.<table>`
topic Debezium creates automatically -- polluting the topic space with
an unintended, unversioned second publication contract nobody designed,
directly exposing internal storage shape to consumers (see question 1).

**5. Does the outbox pattern need `REPLICA IDENTITY FULL` the way
WP-11's `orders` table did?** No -- this lab's `outbox_event` table
never gets UPDATEd or DELETEd as part of the pattern itself (only
INSERTed, then eventually DELETEd by a cleanup job); `FULL` is set here
for consistency with WP-11 but isn't load-bearing, since the pattern
never needs a `before` value.

**6. How would you extend this to guarantee per-aggregate ordering
across a Kafka rebalance?** The routed topic's key is `aggregateid`
(Section 4), so ordinary Kafka partitioning already keeps one
aggregate's events in one partition; ordering is only at risk if the
SOURCE connector itself runs more than one task (`tasks.max > 1`),
since PostgreSQL's WAL is a single ordered stream per replication slot
-- this lab deliberately never sets `tasks.max` above its default of 1
for that reason.

**7. What's the actual latency between the outbox row's commit and the
routed Kafka record becoming visible to consumers?** Bounded by
Debezium's WAL-polling behavior, not by any fixed interval this lab
configures -- in practice, low hundreds of milliseconds under this
lab's local, unloaded test environment (consistent with WP-11's own
CDC latency, since the underlying transport is identical).
