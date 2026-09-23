# Failure Engineering & Production Simulation (WP-19)

## Prerequisites

- `PRINCIPAL_ENGINEER_FAILURE_MATRIX.md` — this WP's real scope is
  defined by that document, not by re-deriving a failure list from
  scratch (Section 1).
- WP-07 (replication, ISR & broker failure), WP-05 (consumer groups &
  rebalancing) — the two mechanics this WP's production-simulation
  scenario combines were each already proven INDIVIDUALLY in these WPs.

## 1. What this WP actually scopes — and why it's narrower than its own name suggests

This WP's roadmap line names a long list of failure types: "broker
failures, consumer failures, network issues, rebalances, lag spikes,
partition skew, disk pressure, recovery experiments." Cross-referencing
`PRINCIPAL_ENGINEER_FAILURE_MATRIX.md` directly (rather than treating
the roadmap line as an independent to-do list) shows almost every one
of those is ALREADY `Demonstrated` in an earlier WP — broker failure
(WP-07), rebalances and consumer failures (WP-05/WP-06), lag spikes and
partition skew (WP-16/WP-17). The failure matrix itself assigns this
WP exactly ONE previously-`Unscheduled` row: **disk pressure**
(Section 2). The roadmap's OTHER instruction —
"a production-simulation lab combining prior labs into one running
system" — is this WP's real, second piece of scope: not a new failure
mode, but proving the guarantees several earlier WPs each proved
INDIVIDUALLY still hold when multiple real failures overlap (Section
3), the way they actually do in production.

## 2. Disk pressure, simulated safely — and two real findings

`DiskPressureTest` mounts a real Kafka broker's log directory on a
size-capped **tmpfs** (48MB) — a genuinely real, exhaustible
filesystem, never the host's actual disk, matching the failure
matrix's own "simulate safely... rather than via literal disk
exhaustion" guidance exactly. Producing ~150MB of real records into
that 48MB-capped filesystem genuinely exhausts it, and a real send
failure surfaces — `Kafka does not preemptively throttle writes as the
disk fills`, exactly as the failure matrix's own "Expected Kafka
behavior" column says.

Two real, unglamorous findings building this ONE test:

1. **A real permission mismatch.** Docker's default tmpfs mount is
   root-owned; this image's broker process runs as a non-root user
   (`appuser`, UID 1000) — the FIRST attempt failed at broker STARTUP
   (before this test's own disk-pressure logic ever ran) with a real
   `java.nio.file.AccessDeniedException` writing the broker's own
   bootstrap metadata checkpoint. Fixed with an explicit `mode=1777`
   (world-writable) on the tmpfs mount.
2. **A real client-observable failure, not a broker crash.** With a
   generous `delivery.timeout.ms`, the producer's `send().get()` call
   itself surfaces the real failure — this test deliberately does NOT
   configure retries aggressively enough to mask it, since the whole
   point is OBSERVING the failure, not successfully working around it.

## 3. The production-simulation scenario: two real failures, overlapping

`ProductionSimulationTest` is this WP's answer to "combining prior
labs into one running system," taken literally: a real 3-broker KRaft
cluster (`ThreeBrokerSimulationCluster`, the same shape WP-07's own
`platform/kafka-cluster/` uses — needed specifically because a
believable "broker dies mid-traffic" scenario needs real replication
and leader failover, which a single-node cluster cannot demonstrate at
all), with:

- **Continuous, real production and consumption** — `acks=all`
  (WP-07's own durability mechanic), running for the WHOLE scenario,
  not just before or after the failures.
- **A real, hard broker KILL** (WP-07's mechanic) — not a graceful
  stop, mid-traffic.
- **A real consumer-group rebalance** (WP-05's mechanic) — a second
  consumer instance joins the SAME group, DELIBERATELY overlapping the
  broker-failure recovery window rather than sequenced cleanly after
  it.

The assertion this test actually makes: every record whose `send()`
was genuinely ACKNOWLEDGED (a real `acks=all` durability guarantee,
independent of anything that happens afterward) is eventually
consumed — real, end-to-end evidence that WP-07's durability guarantee
and WP-05's rebalance-recovery mechanic compose correctly under
simultaneous stress, not just in isolation. Duplicate consumption is
explicitly logged as real, EXPECTED evidence (WP-06's own
at-least-once mental model), not treated as a failure — a rebalance
mid-stream legitimately causes some re-delivery, and this test's
assertions are written to reflect that honestly rather than papering
over it with an unrealistically strict uniqueness check.

## 4. A real finding: starting a 3-voter KRaft quorum sequentially deadlocks

A first version of `ThreeBrokerSimulationCluster.start()` started its
three broker containers with a plain sequential `forEach` —
`GenericContainer#start()` BLOCKS until its own wait strategy succeeds,
so node 1 waited alone for `"Kafka Server started"`, which never
happened: a 3-voter KRaft quorum needs a MAJORITY of voters reachable
to elect a controller leader AT ALL, and nodes 2 and 3 hadn't even
been asked to start yet. Confirmed the hard way — every sequential
attempt timed out waiting for log output that could only ever appear
once all three nodes were starting together. Fixed by starting all
three concurrently (a real `ExecutorService`, not a Testcontainers
convenience method) — not an optimization here, but the only way a
3-voter KRaft bootstrap can succeed at all.

## 5. Lab limitations

- **No live, in-place recovery from disk pressure.** `DiskPressureTest`
  proves the FAILURE mode clearly (Section 2); actually recovering a
  broker whose log directory has genuinely filled — freeing space, or
  expanding the volume, live, without restarting — is real,
  substantial additional scope (the failure matrix's own "Recovery"
  column: "Free or expand storage") this lab names but does not
  automate.
- **The production simulation combines exactly TWO overlapping
  failures** (broker kill + rebalance), not every combination the
  failure matrix could theoretically produce — a deliberate, bounded
  scope: proving the PRINCIPLE (guarantees compose under overlapping
  stress) generalizes further than exhaustively enumerating every
  possible failure pairing would.
- **No network-partition experiment** — the roadmap line names
  "network issues" as in-scope, but a REAL network partition (as
  opposed to a process kill) needs traffic-control-level tooling
  (`tc`/`iptables` inside containers, or a chaos-engineering-style
  proxy) this WP doesn't build; a process kill is a real, different,
  but related failure mode already covered by WP-07 and this WP's own
  Section 3.

## 6. Principal Engineer questions

**1. Section 1 shows this WP scoping ITSELF down from its own
roadmap-line list to just two real experiments — is that a red flag
for how the other 19 WPs in this curriculum were scoped?** No — it's
the SAME scoping discipline every WP in this curriculum has followed:
cross-reference the failure matrix / curriculum map as the actual
source of truth, rather than treating a one-line roadmap description
as an independent, uncross-referenced to-do list. What's different
here is just how VISIBLE the discrepancy is, since this WP's own name
promises the broadest scope of any WP in the curriculum.

**2. Section 3's test treats duplicate consumption as expected, not a
failure — how would you tell the difference, in a REAL production
incident, between "expected duplicates from a rebalance" and "a real
bug causing unbounded reprocessing"?** Bound the duplicate count
against something concrete — HOW MANY records could plausibly have
been "in flight" (unacknowledged locally, or in the consumer's
pre-commit buffer) at the moment of the rebalance, given your own
`max.poll.records` and commit frequency. A HANDFUL of duplicates near
a rebalance boundary is expected; duplicates growing UNBOUNDED, or
appearing for records nowhere near the rebalance's timing, points to a
real bug (an idempotency check that isn't actually idempotent, most
commonly — WP-13's own territory).

**3. Section 4's fix (start all three brokers concurrently) feels
obvious in hindsight — why does Testcontainers' own API make it easy
to get wrong?** `GenericContainer#start()`'s blocking-until-ready
behavior is exactly right for the COMMON case (one container, or
several INDEPENDENT containers with no bootstrap
interdependency) — most Testcontainers usage in this entire repository
relies on exactly that blocking behavior to avoid needing extra
synchronization. The KRaft quorum case is the unusual one: containers
that are only READY as a GROUP, not individually — the API doesn't
distinguish the two cases, so the caller has to know which one applies.

**4. Section 2's disk-pressure test uses a 48MB tmpfs -- why not a much
smaller one, to make the failure surface faster?** A cap too close to
KRaft's own real bootstrap overhead (the `__cluster_metadata` log, JVM
startup housekeeping) risks the broker failing to even START
successfully, for reasons that have nothing to do with THIS test's own
disk-pressure scenario — indistinguishable, from the test's own
perspective, from the failure it's trying to demonstrate. 48MB leaves
real headroom for genuine startup, so a subsequent failure is
attributable to the test's OWN 150MB of writes, not bootstrap noise.

**5. This WP's production simulation kills exactly ONE of three
brokers -- what would change, mechanically, if it killed TWO?**
`min.insync.replicas=2` (set on every internal/data topic in this
lab's own cluster, WP-07's own durability configuration) means losing
a SECOND broker would drop the surviving ISR below what `acks=all`
requires — real, correctly REJECTED writes (`NotEnoughReplicasException`,
the same real mechanism WP-07's own experiments cover), not silent
data loss. This test deliberately stays within the cluster's own
configured durability tolerance (one broker lost, of three, with
RF=3/min.isr=2) specifically to prove the "should keep working"
case; a two-broker loss is a DIFFERENT, already-covered experiment
(WP-07's own ISR-shrink-below-minimum scenario), not a gap here.

**6. Why does `ThreeBrokerSimulationCluster` duplicate
`platform/kafka-cluster/`'s own 3-broker Testcontainers shape instead
of reusing WP-07's actual lab code?** The SAME reason every prior
lab's own Testcontainers helper is copied rather than shared (see any
lab's README, "Why a separate project") — plus a real, WP-19-specific
need WP-07's own helper doesn't have: a `killBroker(nodeId)` method
using the Docker client directly (a real, hard kill, not
Testcontainers' own graceful `stop()`), which this lab's own scenario
specifically needs and WP-07's helper was never built to expose.

**7. Section 3 doesn't wait for a specific "rebalance complete" signal
before asserting anything -- how do you know the 20-second recovery
window used here is actually enough, rather than just usually enough?**
It isn't proven to be ENOUGH in the abstract — it's a real, generous
window chosen empirically against this specific test's own real
traffic rate and cluster size, the same way every timing-sensitive
bound in this repository's test suites is chosen (a documented,
deliberate value, not a formally derived one). A production system
doesn't get to assume a fixed recovery window either — this is
precisely why WP-16's own observability tooling (watching real lag and
real under-replicated-partition metrics) is how you'd actually KNOW
recovery finished, rather than guessing a wait duration, in a system
you don't control the clock of.
