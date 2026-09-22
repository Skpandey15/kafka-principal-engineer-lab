# Performance & Capacity Engineering (WP-17)

## Prerequisites

- WP-04 (partitioning) — partition count as a parallelism ceiling is a
  direct extension of that WP's own mechanics.
- WP-05 (consumer groups) — the same ceiling, from the consuming side.
- WP-16 (observability) — the JMX/Prometheus pipeline that would, in a
  real deployment, be how you'd actually WATCH the numbers this WP
  measures directly, in code, instead.

## 1. Why tuning and sizing are taught together here

The roadmap's own framing for this WP is deliberate: a benchmarking
harness and a capacity workbook, not two separate tracks. The reason is
concrete, not just organizational — `CapacityCalculator`'s
throughput-based partition-count formula (Section 6) takes a MEASURED
per-partition throughput ceiling as its input, not an assumed
industry-average number pulled from a blog post. Tuning (what can one
partition, with these settings, actually sustain?) is the thing that
makes sizing (how many of those do I need?) answerable with a real
number instead of a guess.

## 2. A real, hand-built benchmark, not a wrapped shell script

`ProducerBenchmark` is a plain Java class around a real
`KafkaProducer` — not `kafka-producer-perf-test.sh` hidden behind a
subprocess call. Every config knob's effect is directly attributable:
the SAME method runs with different `Map<String, Object>` overrides,
and the only thing that changes between runs is that map. Latency is
measured per-record (send-callback round trip, in real nanoseconds,
not simulated), and throughput is computed from real wall-clock elapsed
time and real payload byte counts.

## 3. Batching: a real, measured throughput ceiling

`batchingRealMeasurablyImprovesThroughputOverUnbatchedProduction`
compares two real runs of the identical 3,000-record workload: one with
`batch.size=1`/`linger.ms=0` (as close to "no batching at all" as the
producer API allows — a real ceiling of roughly one network round-trip
per record), and one with `linger.ms=20`/`batch.size=65536` (real
batching). The batched run's real, measured records/sec is asserted
strictly greater than the unbatched run's — not against a fixed
absolute number (which would be tied to this specific machine's
performance and flaky across environments), but against the OTHER
real run from the same test, on the same hardware, moments apart.

## 4. Compression: a real, measured byte-size reduction

`compressionRealMeasurablyReducesBytesSentForCompressibleData` uses
`PayloadGenerator.compressible(...)` — a deliberately repetitive byte
pattern, not random bytes, because a compression algorithm has nothing
to exploit in high-entropy data; a real payload with genuine repeated
structure (the vastly more common real-world case: JSON field names,
repeated string values, etc.) is what makes compression's real effect
visible at all. The evidence is the producer's own real
`compression-rate-avg` metric (confirmed present in the
`producer-metrics` group), not an assumption: `compression.type=none`
reports a ratio near 1.0 (no reduction), while `lz4` against this
compressible payload reports a ratio well under 1.0 — real bytes
actually sent over the wire, smaller than the logical payload size.

## 5. Partition count as a real, hard consumer-parallelism ceiling

`partitionCountIsARealCeilingOnConsumerParallelism` starts 5 REAL,
separate `KafkaConsumer` instances in one group against a 3-partition
topic, waits for a real rebalance to settle, and confirms: all 3
partitions get assigned (someone owns each one), and at least 2 of the
5 consumer instances end up with an EMPTY assignment — real,
observable idleness, not a hypothetical. This is WP-04/WP-05's own
partition-ownership mechanic, viewed from the capacity-planning side:
"how many partitions do I need" is partly a THROUGHPUT question
(Section 6) and partly, independently, a PARALLELISM question — you
can have all the per-partition throughput headroom in the world and
still leave real consumer capacity sitting idle if partition count is
the actual limiting factor.

## 6. `fetch.min.bytes` and `fetch.max.wait.ms`: a real, measured trade-off

`lowFetchMinBytesReturnsQuicklyWhileHighFetchMinBytesWaitsForTheRealMaxWait`
produces exactly one small record, then measures two consumers' real
`poll()` latency:

- `fetch.min.bytes=1` (effectively "return with whatever you have")
  returns in real, measured well under 2 seconds.
- `fetch.min.bytes=1,000,000` (never satisfied by one small record) with
  `fetch.max.wait.ms=3000` genuinely BLOCKS for real, measured time
  close to that 3-second ceiling before the broker gives up waiting for
  more data and returns what it has anyway.

This is a real latency-vs-efficiency trade-off, not just a config
reference table entry: a larger `fetch.min.bytes` reduces the NUMBER of
fetch requests a consumer needs (each one carries more data, real
network-efficiency win at scale) at the direct cost of added latency
on the specific requests that don't get enough data fast enough to
avoid the wait.

## 7. The capacity workbook, worked with real inputs

`CapacityCalculator` is real, deterministic arithmetic across three
independent questions a capacity plan actually has to answer:

1. **Storage** (`estimateStorage`): messages/sec × average message size
   × retention window, multiplied by the REPLICATION FACTOR — because
   each replica is a full, independent on-disk copy (WP-07's own
   mechanism), not a a logical count. A 1,000 msg/s, 1KB-average, 1-day
   retention, RF=3 workload needs roughly 3× a single replica's ~82GB,
   not 82GB total — a mistake this formula makes structurally
   impossible to make by accident.
2. **Throughput-driven partition count** (`requiredPartitionsForThroughput`):
   target aggregate throughput ÷ a REAL measured per-partition ceiling
   (Section 3's own benchmark output, not an assumed number), rounded
   UP — a partial partition's worth of remaining headroom still needs
   one more whole partition.
3. **Parallelism-driven partition count**
   (`requiredPartitionsForConsumerParallelism`): the independent
   Section 5 question — how many partitions does your DESIRED consumer
   parallelism alone require, regardless of throughput headroom?

A real capacity plan takes the MAXIMUM of #2 and #3 — whichever
requirement is larger governs the actual partition count, since both
are hard constraints, not averaged together.

## 8. Lab limitations

- No consumer-side throughput benchmark harness (a `ConsumerBenchmark`
  mirroring `ProducerBenchmark`) — this WP's consumer-side experiments
  (Sections 5-6) measure specific, targeted behaviors (parallelism
  ceiling, fetch-wait trade-off) directly, rather than building a
  second general-purpose benchmarking class; a reasonable scope
  boundary given the producer side already demonstrates the general
  benchmarking pattern.
- No `acks`/`min.insync.replicas` durability-vs-throughput trade-off
  experiment in this WP specifically — WP-07 already covers `acks` and
  `min.insync.replicas` in full depth from the durability angle; this
  WP focuses on the batching/compression/fetch knobs WP-07 doesn't
  cover, to avoid re-treading the same ground.
- The capacity workbook is real, tested arithmetic, not a spreadsheet
  or GUI tool — appropriate for what this repository is (a Java-code
  curriculum), but a real capacity-planning exercise at a company would
  likely also want the numbers in a shareable, non-code format.
- Single-node Testcontainers cluster for all benchmark runs — absolute
  throughput numbers measured here reflect this specific test
  environment's hardware and the single-broker topology, not any
  particular production cluster's real ceiling; every assertion in this
  WP's suite is accordingly a RELATIVE comparison (batched vs.
  unbatched, compressed vs. not), never an absolute pass/fail threshold
  tied to one machine's performance.

## 9. Principal Engineer questions

**1. Why does `requiredPartitionsForThroughput` take a MEASURED
per-partition ceiling as a parameter instead of computing one from
broker specs (CPU, disk, network) directly?** Because the real ceiling
depends on far more than broker hardware alone — payload size,
compression codec, `acks` setting, replication factor, and the actual
message shape all materially change what one partition can sustain
(Sections 3-4 demonstrate two of these levers directly). A number
computed from hardware specs alone would be a guess dressed up as
math; a number measured against your ACTUAL workload shape, on your
ACTUAL pinned Kafka version, is real.

**2. Section 5's test shows partitions can leave consumers idle -- why
not just always provision MORE partitions than you'll ever need, to
avoid ever hitting this ceiling?** Partitions aren't free -- each one is
real per-broker overhead (open file handles, replication traffic, more
metadata for the controller to track, WP-08's own territory) and more
partitions can mean WORSE per-partition throughput under some
workloads (batching efficiency spreads thinner across more, smaller
partitions). Over-provisioning trades one real cost (idle consumer
capacity) for a different real cost (broker overhead) -- the right
partition count is the LARGER of Section 7's two real requirements, not
an arbitrarily large safety margin.

**3. Section 6 shows `fetch.max.wait.ms` bounds how long a consumer
waits for `fetch.min.bytes` to be satisfied -- what happens to that
same trade-off under REAL, sustained high-throughput production
(unlike this test's single small record)?** Under sustained high
throughput, `fetch.min.bytes` is satisfied almost immediately on nearly
every poll -- the wait this test measures only shows up when the
PRODUCTION rate is genuinely below what `fetch.min.bytes` needs within
the `fetch.max.wait.ms` window. This is precisely why the trade-off
matters most for LOW-throughput or bursty topics, not busy ones -- a
real, situational trade-off, not a universal tax.

**4. Why does the compression test (Section 4) use a repetitive payload
instead of a REALISTIC one (e.g., real JSON)?** A controlled, known
payload isolates the variable actually under test -- HOW compressible
the data is, versus how well a given codec exploits that
compressibility -- from confounds a "realistic" payload would
introduce (varying field lengths, varying repetition across records,
etc.). Section 4 explicitly names this: the repetitive pattern is
representative of a REAL property most production payloads share
(structural repetition -- field names, common string values), not an
artificially favorable edge case.

**5. Section 7's storage formula assumes constant message rate and
size -- how would you adapt it for a workload with a known daily peak,
not just an average?** Compute #2 (partition count, throughput-driven)
against the PEAK rate, not the average -- undersizing partitions for
peak load is the failure mode that actually hurts (WP-06's delivery
guarantees don't change, but WP-16's own "consumer lag explosion"
failure-matrix row is exactly what happens when sustained throughput
exceeds what your partition count can carry). Storage (#1) can
reasonably use the AVERAGE rate, since retention-driven storage is
already amortized over the whole retention window, smoothing out
short peaks.

**6. Why does Section 3's throughput comparison use `batch.size=1`
instead of comparing against Kafka's own DEFAULT `batch.size`
(16,384 bytes)?** `batch.size=1` isolates the batching MECHANISM itself
as the variable under test — it's as close as the API allows to "no
batching happens at all," making the comparison a clean before/after of
the mechanism's existence, not a comparison between two already-batched
configurations that would show a smaller, harder-to-attribute
difference.

**7. This WP's tests never assert an ABSOLUTE throughput number -- how
would you actually validate, for a REAL cluster, whether ITS measured
numbers are "good"?** Compare against the SAME cluster's own prior
measurements over time (a real regression, not a one-off number) and
against the theoretical ceiling implied by its actual network/disk
hardware -- there is no universal "good" throughput number, only
"good relative to what this specific hardware and workload shape
should be able to do," which is exactly why Section 8 names absolute
numbers as environment-dependent rather than asserting on them
directly.
