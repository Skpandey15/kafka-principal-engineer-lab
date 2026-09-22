# Kafka Streams (WP-14)

## Prerequisites

- [`docs/transactions/TRANSACTIONS_AND_EXACTLY_ONCE_SEMANTICS.md`](../transactions/TRANSACTIONS_AND_EXACTLY_ONCE_SEMANTICS.md)
  (WP-09) -- Kafka Streams' `exactly_once_v2` processing guarantee wires
  together the exact transactional producer + consumer-offset machinery
  that WP explains; this WP is the payoff of that groundwork, configured
  for you rather than hand-implemented.
- `docs/roadmap/PRINCIPAL_ENGINEER_FAILURE_MATRIX.md`'s "Integration and
  processing failures" section -- this WP owns two real, named rows
  ("Kafka Streams process dies", "Kafka Streams state restoration takes
  a long time").

## 1. What Kafka Streams actually is

A Java library, not a separate service -- `org.apache.kafka:kafka-streams`
is a dependency your own application embeds, built entirely on top of
the producer/consumer APIs every prior lab already uses. A `StreamsBuilder`
declares a topology (`stream()`, `map()`, `groupByKey()`, `aggregate()`,
`.to()`); `new KafkaStreams(topology, props).start()` runs it as one or
more real internal consumer+producer threads. This is why WP-14 reuses
the existing WP-07 cluster unchanged -- there is no new broker-side
infrastructure to add.

## 2. State stores: local, changelog-backed, and the point of this WP

`OrderAggregationTopology` maintains a running total per `customerId` in
a `Materialized` key-value store. Two things make this different from a
plain in-memory `Map`:

1. It's backed by embedded RocksDB on local disk (`state.dir`), so it
   survives a JVM restart of the SAME instance, on the SAME machine.
2. Every update is ALSO written to an internal **changelog topic** --
   an ordinary, compacted Kafka topic Kafka Streams creates and manages
   automatically. This is what makes the state survive losing the
   machine entirely: another Streams instance can rebuild it from the
   changelog.

`runningTotalAggregatesAcrossMultipleOrdersForTheSameCustomer`
(`TopologyTestDriver`) proves the aggregation logic itself; the
Testcontainers suite (Sections 5-6) proves the changelog-durability
half.

## 3. Windows: buckets, not a continuous accumulator

`WindowedOrderTopology` uses `TimeWindows.ofSizeWithNoGrace(windowSize)`
-- a **tumbling** window: fixed-size, non-overlapping buckets keyed by
event time. `tumblingWindowSumsOrdersWithinAWindowAndStartsFreshInTheNextWindow`
is real, deterministic evidence (via `TestInputTopic.pipeInput(key,
value, Instant)`, which controls EVENT time directly): two orders 2
seconds apart land in the same 10-second window and their sums combine;
a third order 12 seconds after the first lands in a NEW window and
starts at its own amount alone, `7.0`, not `22.0`. "No grace" means a
record whose event time falls after its window has already closed is
dropped outright, not retroactively merged -- a real, deliberate
trade-off for keeping this lab's window tests fast and deterministic
(see Section 9 for the production implication).

## 4. Joins: co-partitioning, and a real serde finding

`EnrichedOrderTopology` performs a `KStream`-`KTable` **inner** join --
`streamTableJoinEnrichesMatchedOrdersAndDropsUnmatchedOnes` proves both
halves: a matched order emits an enriched record combining both sides,
while an order for a customerId absent from the KTable produces **zero**
output records (not a null-enriched one).

A real finding building this topology: after `.map()` re-keys the order
stream by `customerId` (necessary since the join needs to match the
KTable's key), the DSL loses its earlier, implicit key serde. A first
version failed at topology initialization --
`ConfigException: Please specify a key serde or set one through
StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG` -- even though the
downstream `.to()` call already had its own explicit `Produced.with(...)`.
The fix: `Joined.with(keySerde, valueSerde, otherValueSerde)` supplies
the join itself with the serde information a key-changing `.map()`
upstream doesn't propagate automatically.

## 5. Failure-matrix row 1: a Streams process dies

`killingOneStreamsInstanceMigratesItsTasksAndRestoresFullStateOnTheSurvivor`
runs TWO real `KafkaStreams` instances sharing one `application.id`
against a 4-partition input topic -- Kafka Streams' own internal
consumer group splits the resulting tasks between them automatically,
the same rebalancing mechanism WP-05 covers, just driven by Streams
rather than a hand-written consumer loop. After both instances confirm
`RUNNING` and every expected customer total is queryable somewhere
across the two, instance A is closed (a real, graceful stop -- modeling
"the process dies"). The test then confirms:

- Instance B returns to `RUNNING` after absorbing A's former tasks.
- EVERY customer total A used to own is now correctly queryable on B --
  proving Kafka Streams rebuilt that state from the changelog topic,
  not that B happened to already have it.
- A real `StateRestoreListener` registered on B recorded a non-zero
  count of restored records during that exact window -- direct evidence
  restoration actually happened, not just that the end state looks
  right by coincidence.

## 6. Failure-matrix row 2: standby replicas shrink restoration time

`aStandbyReplicaMeansFailoverRequiresLittleOrNoChangelogRestoration`
repeats the same experiment with `num.standby.replicas=1` on both
instances. A standby replica is a task's state store kept warm on ANOTHER
instance in the background, continuously replicating the same changelog
a live active task writes to -- so when that active task's owner dies,
the standby is often already caught up, or close to it, and needs far
less (sometimes zero) changelog replay to become the new active copy.
The test gives the standby 8 real seconds to replicate before killing
instance A (a deliberate, documented fixed wait -- there's no cheap
public "standby is caught up" signal to poll instead), then asserts the
restored-record count stays small. Contrasted directly with Section 5's
row (a cold task with NO standby, which must fully replay), this is
real, comparative evidence for exactly what the failure matrix's
"Recovery" column says: standby replicas reduce restoration time by
keeping warm copies elsewhere.

## 7. Exactly-once processing, in practice

`exactlyOnceProcessingCommitsTransactionallyWithNoDuplicationAcrossMultipleCommits`
runs `OrderAggregationTopology` with `processing.guarantee=exactly_once_v2`
across THREE separate input batches, spaced far enough apart that
multiple real commit intervals elapse between them (not one big batch
inside a single transaction). The output topic is read with
`isolation.level=read_committed` (WP-09's own mechanism), and the final
committed total for every customer matches exactly -- no
under-counting from an aborted-but-partially-visible transaction, no
over-counting from a duplicate replay across a commit boundary.

## 8. A real interactive-query limitation this lab's own tests had to work around

`KafkaStreams.store(...)` only returns data for tasks THAT SPECIFIC
INSTANCE currently owns -- there is no automatic cross-instance query
routing built into the library itself (a real production system wants
this and builds it via `KafkaStreams.metadataForAllStreamsClients()`
plus an RPC layer between instances; not built here, out of scope). This
lab's own `allKeysMatch` test helper has to check EVERY instance for
each expected key and treat "not found on this one" as "try the next
instance," not as evidence the key doesn't exist. This exact limitation
is discoverable no other way than by writing a distributed interactive
query and watching it silently return `null` from the wrong instance.

## 9. Lab limitations

- No dedicated failure-matrix row beyond the two Sections 5-6 already
  implement -- both of WP-14's named rows are now `Demonstrated`.
- Windows use `ofSizeWithNoGrace` (Section 3) -- a production topology
  would very likely configure an explicit grace period to tolerate
  realistic out-of-order arrival, trading a small amount of extra
  latency (waiting for the grace period to elapse before a window is
  considered final) for correctness against late data. Left at zero
  grace here specifically so this lab's window-boundary test stays
  fast and fully deterministic.
- No `GlobalKTable` example -- this WP covers `KTable` (partitioned,
  co-partitioning required) but not `GlobalKTable` (fully replicated to
  every instance, no co-partitioning requirement, at the cost of full
  local replication). A reasonable scope boundary; the trade-off is
  named here rather than built.
- No cross-instance interactive-query RPC layer (Section 8) -- the
  limitation is demonstrated and worked around inside the test suite,
  not solved with production tooling.
- `num.stream.threads` is left at its default (1 thread per instance) --
  this lab's multi-instance experiments already demonstrate
  cross-INSTANCE task distribution, which is the more interesting case
  for the failure-matrix rows this WP owns; intra-instance
  multi-threading is a separate, narrower scaling knob not explored
  here.

## 10. Principal Engineer questions

**1. Why does this WP reuse the existing WP-07 Kafka cluster with zero
new platform infrastructure, when WP-10 through WP-13 all added a new
container?** Because Kafka Streams is a client library, not a service --
the "infrastructure" it needs is exactly the broker cluster every
producer/consumer application already needs, plus internal topics it
creates for itself (changelogs, repartition topics) using the SAME
admin operations any client can perform. There's nothing new to run.

**2. What's the real difference between the state-store durability this
WP demonstrates and WP-12's transactional outbox durability?** Both are
"the local, fast thing is backed by a durable Kafka log so it survives
a crash" -- but the outbox pattern durably records an EXPLICIT business
intent a human designed (an event about to be published); a Streams
state store durably records DERIVED, implicit computation state (a
running total) that only exists because of the topology's own logic.
Losing an outbox row is losing a specific business fact; losing (and
then rebuilding) a state store's local copy is an availability blip,
not a correctness problem, precisely because the changelog is the
source of truth and the local RocksDB copy is a disposable cache of it.

**3. Section 8 says interactive queries are local-only -- doesn't that
make `Materialized` state stores useless for actually serving queries
from an application?** Not useless, but it means a real
query-serving system built on Streams' interactive queries needs its
own routing layer on top (query `metadataForKey`/`allMetadataForStore`
to find WHICH instance owns a key, then forward the request there via
some RPC mechanism you build) -- Kafka Streams gives you the primitive
(a durable, partitioned, locally-queryable store) but not a finished
distributed query service.

**4. Why does the "process dies" experiment (Section 5) use a graceful
`close()` rather than forcibly killing the JVM?** A graceful close is a
real, legitimate way for "the process dies" to happen (a deploy, a
scale-down, an orderly shutdown) and it reliably triggers the exact
same task-reassignment and changelog-restoration path a hard kill
would, without this lab needing to spawn and forcibly terminate a
SEPARATE JVM process from inside a JUnit test -- a meaningfully more
complex test-harness problem for the same underlying evidence.

**5. Section 6's test waits a FIXED 8 seconds for standby replication
to catch up rather than polling for a real signal -- is that a gap in
rigor compared to the rest of this repository's condition-polling
convention?** Yes, and it's named as such rather than hidden (Section 6
and the README both call it out explicitly) -- there IS a real signal
(`KafkaStreams.metadataForLocalThreads()`/lag-based introspection could
be built), but reaching it reliably within this WP's scope would cost
meaningfully more test-harness complexity for a lab whose point is the
CONCEPT (standby replicas exist and reduce restoration cost), not a
production-grade lag-polling utility.

**6. Why is `commit.interval.ms` lowered from its default (30 seconds
for `at_least_once`, 100ms for `exactly_once_v2`) to 500ms across every
test in this suite?** The default is tuned for production throughput,
not test latency -- at 30 seconds, this lab's bounded test timeouts
(30-60 seconds) would frequently expire before even ONE commit (and
therefore one changelog flush) had happened, making restoration
evidence unobservable within a reasonable test run. 500ms is a
deliberate test-only override, not a production recommendation.

**7. How does this WP's exactly-once guarantee (Section 7) relate to
the idempotent-consumer topic WP-13 built?** They solve different
layers of the same overall problem. Streams' `exactly_once_v2` makes
the read-process-write cycle WITHIN Kafka atomic (consume input,
update state, produce output, commit offsets -- all or nothing, exactly
as WP-09 describes for a hand-written consume-transform-produce loop).
WP-13's idempotent-consumer table solves the SAME problem for side
effects OUTSIDE Kafka a topology (or any consumer) might trigger --
Streams' EOS guarantee has no reach into an external database write a
`Processor` might make, the same boundary WP-09's own dual-write
discussion already establishes.
