# Consumer Groups and Rebalancing

This document is the conceptual and Principal Engineer depth behind
[`labs/lab-04-consumer-groups-rebalancing`](../../labs/lab-04-consumer-groups-rebalancing/README.md).
Every technical claim below was verified against the real
`kafka-clients:4.3.1` source (the version this repository pins) before
being written down, and every observation is a real result captured while
building and validating that lab against the WP-02 Kafka environment — see
each section for exactly what was run.

## The mental model

```text
Topic (3 partitions)
├── P0 ──> Consumer-1
├── P1 ──> Consumer-2
└── P2 ──> Consumer-3
```

A **consumer group** is a named set of consumers that divide a topic's
partitions among themselves so that each partition is actively owned by
**at most one member of that group at a time**. This is the single
invariant everything else in this document follows from.

```text
Consumer-2 disappears
├── P0 ──> Consumer-1
├── P1 ──> ???            <- must be reassigned
└── P2 ──> Consumer-3
      ↓
A REBALANCE happens
      ↓
├── P0 ──> Consumer-1
├── P1 ──> Consumer-1 (or Consumer-3 — the assignor decides)
└── P2 ──> Consumer-3
```

Consumer 2's disappearance is only a problem for *this* group. See
"Independent consumer groups," below, for why the same partition being
consumed by a completely different group at the same time is not a
violation of anything.

## Partition ownership and consumer-group scaling — real results

Run for real against a 3-partition topic
(`orders.consumer-group.lab`), adding one
[`ConsumerGroupMemberApp`](../../labs/lab-04-consumer-groups-rebalancing/src/main/java/com/kafkalab/consumergroups/consumer/ConsumerGroupMemberApp.java)
instance at a time to the same group:

| Consumers | Real observed assignment |
|---|---|
| 1 | `consumer-1` → `[P0, P1, P2]` |
| 2 | `consumer-1` → `[P0, P1]`, `consumer-2` → `[P2]` |
| 3 | `consumer-1` → `[P0]`, `consumer-2` → `[P1]`, `consumer-3` → `[P2]` |
| 4 | as above, plus `consumer-4` → `[]` |
| 5 | as above, plus `consumer-5` → `[]` |

```text
effective consumer parallelism <= partition count
```

Consumers 4 and 5 are not broken, not misconfigured, and not failing
health checks — they are fully running, correctly joined the group, and
received a real `PARTITIONS_ASSIGNED` callback with an **empty** partition
list. **Increasing replicas beyond partition count does not increase
processing throughput for one consumer group** — there is nothing left to
give the extra replicas. This is the most common, and most avoidable,
consumer-group capacity-planning mistake.

## Independent consumer groups — real result

Two separate groups, `order-processing-service` and
`order-analytics-service`, each with one consumer, against the same
30-record topic. Real `kafka-consumer-groups.sh --describe` output after
both had caught up:

```text
GROUP                     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
order-analytics-service   0          8               8               0
order-analytics-service   1          15              15              0
order-analytics-service   2          7               7               0

order-processing-service  0          8               8               0
order-processing-service  1          15              15              0
order-processing-service  2          7               7               0
```

Both groups independently reached the same log-end offsets, with zero
lag, having each read every one of the 30 records on their own. This is
the precise statement worth memorizing exactly as written:

> A partition can be assigned to only one consumer **within a consumer
> group** at a time, but the same partition can simultaneously be consumed
> by consumers belonging to **different** consumer groups.

`__consumer_offsets` tracks committed position *per group*, not per
partition globally — that's the entire mechanism behind this.

## Assignment strategy actually used — verified, not assumed

Do not trust an older tutorial's description of Kafka's assignor here.
Verified directly against `kafka-clients:4.3.1`'s `ConsumerConfig` source:

```java
.define(PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        Type.LIST,
        List.of(RangeAssignor.class, CooperativeStickyAssignor.class),
        ...)
```

The config's own documentation comment states the consequence explicitly:

> "The default assignor is `[RangeAssignor, CooperativeStickyAssignor]`,
> which will use the `RangeAssignor` by default, but allows upgrading to
> the `CooperativeStickyAssignor` with just a single rolling bounce that
> removes the `RangeAssignor` from the list."

**What this means concretely:** with no override (which is exactly what
`ConsumerGroupMemberApp` and this lab's tests use), the *list* offered to
the broker contains two assignors, but the one actually **negotiated and
used** is `RangeAssignor` — because it's first in the list and every group
member supports it. This is not an assumption; every real consumer log
captured while building this lab shows it directly:

```text
Successfully joined group with generation Generation{generationId=2, memberId='consumer-1-...', protocol='range'}
```

**What Kafka supports vs. what this project configures vs. what was
observed:**

| | |
|---|---|
| Kafka 4.3.1 supports | `RangeAssignor`, `RoundRobinAssignor`, `StickyAssignor`, `CooperativeStickyAssignor`, and custom `ConsumerPartitionAssignor` implementations |
| This project configures | Nothing — `partition.assignment.strategy` is left at its client default everywhere in this lab |
| Observed experimentally | `protocol='range'` in every real consumer-group log this lab produced |

**`RangeAssignor`** assigns partitions on a per-topic basis, in partition
order, to consumers sorted by name — it is why 3 partitions over 2
consumers split 2-and-1 rather than something else, in every run captured
while building this lab. **`RoundRobinAssignor`** and **`StickyAssignor`**
are real, available alternatives this lab does not exercise. Do not claim
`CooperativeStickyAssignor` is active in this lab just because it's in the
candidate list — the observed `protocol='range'` in real logs settles it.

## Rebalancing — real results

### Experiment A: a consumer joins

Already shown in "Partition ownership," above — going from 1 to 2
consumers reassigned partition ownership immediately and correctly.

### Experiment B: graceful departure

Kafka's own client sends an explicit `LeaveGroup` request when a consumer
closes cleanly (`consumer.close()`, reached via this lab's
`wakeup()`-based shutdown hook — the same pattern WP-03 established).
`LeaveGroup` tells the coordinator immediately, so the remaining group
members are reassigned that member's partitions **without waiting for any
timeout at all**.

**A real, deliberate limitation of this validation, worth stating
honestly:** this repository's lab-validation environment could not send
an OS-level graceful termination signal to a background Java process on
Windows from the automation shell used to build this lab — `taskkill`
without `/F` was refused outright:

```text
ERROR: The process with PID ... could not be terminated.
Reason: This process can only be terminated forcefully (with /F option).
```

This is a genuine finding about that specific automation environment, not
a claim about graceful shutdown in general — pressing Ctrl+C in a real
interactive terminal, or a container orchestrator's normal `SIGTERM`
during a rolling deployment, delivers exactly the signal this consumer's
shutdown hook expects. The graceful-departure mechanism itself **is**
verified — directly, and deterministically, not by timing an OS signal —
by
[`ConsumerGroupsRebalancingIntegrationTest.consumerLeavingCausesItsPartitionsToBeReassigned`](../../labs/lab-04-consumer-groups-rebalancing/src/test/java/com/kafkalab/consumergroups/ConsumerGroupsRebalancingIntegrationTest.java),
which calls the consumer's own `wakeup()`-driven stop path in-process and
asserts the remaining member picks up every partition — passing,
repeatably, with no timing dependency on OS signal delivery.

### Experiment C: abrupt failure — real measured detection time

A consumer process was **force-killed** (`taskkill /F`, the Windows
equivalent of `kill -9` — no `LeaveGroup`, no chance to run any shutdown
code at all) while a partner consumer in the same group kept running.
Real, precisely timestamped results from two independent trials:

| Trial | Kill issued at | Reassignment observed at | Elapsed |
|---|---|---|---|
| 1 | `14:02:15.886Z` | `14:02:25.594Z` (REVOKED) / `14:02:25.600Z` (ASSIGNED) | **9.708s** |
| 2 | `14:03:45.442Z` | `14:03:56.310Z` (REVOKED) / `14:03:56.317Z` (ASSIGNED) | **10.868s** |

Both trials landed close to this lab's configured `session.timeout.ms` of
**10,000ms** (`ConsumerGroupMemberApp` shortens this from Kafka's own
default of 45,000ms specifically to make this experiment observable in a
lab session — see that class's Javadoc). That is not a coincidence: with
no `LeaveGroup` ever sent, the coordinator's only way to notice this
member is gone is the **absence of a heartbeat for the configured session
timeout**.

### Graceful departure vs. abrupt failure — the difference that matters

```text
graceful (LeaveGroup sent)   -> reassignment is near-instant
abrupt (no LeaveGroup)       -> reassignment waits up to session.timeout.ms
```

This is precisely why `session.timeout.ms` is a durability/availability
trade-off, not a value to set arbitrarily: too short, and transient
slowness gets misdiagnosed as a failure (see "Failure detection
configuration," below); too long, and a genuine crash leaves that
member's partitions unprocessed for the entire timeout window.

## Rebalance visibility

[`ConsumerGroupMemberApp`](../../labs/lab-04-consumer-groups-rebalancing/src/main/java/com/kafkalab/consumergroups/consumer/ConsumerGroupMemberApp.java)
implements `ConsumerRebalanceListener` explicitly, logging all three
callbacks with a timestamp and the exact partition list, verified against
the real interface (`org.apache.kafka.clients.consumer.ConsumerRebalanceListener`
in `kafka-clients:4.3.1`):

- **`onPartitionsRevoked`** — fires when this consumer is about to lose
  partitions it currently owns, *while it still has the chance to react*
  (commit offsets, flush state) before they're handed elsewhere.
- **`onPartitionsAssigned`** — fires with the partitions newly owned after
  a rebalance completes (can be an empty list — real evidence above).
- **`onPartitionsLost`** — has a default implementation in the interface
  itself (delegating to `onPartitionsRevoked`) but is distinct: it fires
  when partitions are taken away **without** this consumer having had a
  chance to react first, e.g. because the coordinator already considered
  it dead. This lab overrides all three explicitly rather than relying on
  the default, specifically so the distinction stays visible in the logs.

## Rebalance impact — real results

A continuous producer (one record every 200ms) plus two consumers in a
fresh group, with a third added mid-stream. Real per-consumer rebalance
timestamps from that run:

```text
consumer-1 | PARTITIONS_REVOKED | 14:03:23.290303100Z | [P0, P1]
consumer-2 | PARTITIONS_REVOKED | 14:03:23.290303100Z | [P2]
consumer-1 | PARTITIONS_ASSIGNED | 14:03:23.300206600Z | [P0]
consumer-2 | PARTITIONS_ASSIGNED | 14:03:23.300206600Z | [P1]
consumer-3 | PARTITIONS_ASSIGNED | 14:03:23.311178900Z | [P2]
```

**Read that revoked line carefully: both `consumer-1` and `consumer-2`
revoked *every* partition they owned — not just the one partition
changing hands.** This is the real, observed cost of the `RangeAssignor`'s
**eager** rebalance protocol: every existing member gives up its entire
assignment and rejoins from scratch on every rebalance, even members whose
actual assignment doesn't change. In this local, single-broker, no-network-
latency lab, the resulting pause was on the order of 10ms; over a real
network, with more members and more partitions, that same stop-the-world
window is measured in hundreds of milliseconds to seconds — during which
**none** of the group's partitions are being actively fetched by anyone.

This is exactly why the roadmap's failure matrix and this document both
flag frequent rebalances as operationally undesirable, independent of
whether any individual rebalance eventually resolves correctly:

```text
frequent rebalances
      ↓
repeated stop-the-world pauses across every member (eager protocol)
      ↓
consumption gaps, growing lag, even though the cluster itself is healthy
      ↓
processing that had already advanced past a not-yet-committed offset gets
re-delivered to whichever member ends up owning that partition next
      ↓
duplicate-processing risk, exactly per
docs/architecture/KAFKA_MENTAL_MODEL.md's fetch/position/commit distinction
```

**Offset-management implication:** a rebalance does not, by itself, lose
or duplicate data — but it does mean whatever was fetched-and-not-yet-
committed by the member losing a partition is fetched again by whoever
gets it next. This is the same at-least-once shape WP-03 already
established; a rebalance is simply one more concrete trigger for it,
alongside a plain crash.

## How cooperative rebalancing reduces this disruption

`CooperativeStickyAssignor` (available, in the candidate list, but **not**
the negotiated protocol in this lab's default configuration — see
"Assignment strategy," above) exists specifically to avoid the eager
protocol's stop-the-world cost. Instead of every member revoking its
entire assignment on every rebalance, cooperative rebalancing lets members
**keep consuming the partitions they're not losing**, revoking only the
specific partitions that are actually moving, over up to two rebalance
rounds instead of one. This directly narrows the "nobody is consuming
partition X" window this section's real timestamps demonstrate for the
eager protocol — it does not eliminate rebalancing, but it eliminates
disrupting members whose assignment didn't need to change at all. This
repository has not yet run a real, `group.protocol=consumer`-negotiated
comparison against this lab's eager-protocol numbers above — that
verification, if it happens, belongs to a future work package, not a
claim made here without evidence.

## Graceful shutdown — why it matters operationally

`ConsumerGroupMemberApp` uses the same `Runtime.addShutdownHook()` +
`consumer.wakeup()` pattern WP-03 established: a `WakeupException` breaks
the blocked `poll()` call, and `consumer.close()` runs in a `finally`
block, which is what actually sends `LeaveGroup`.

**In Docker**, a container receiving `SIGTERM` (the normal signal for
`docker stop`) needs a process that actually handles it — a JVM with this
shutdown hook does; a JVM killed by `SIGKILL` (what happens if it doesn't
exit before Docker's stop timeout) does not get the chance, and looks
identical to a crash from the coordinator's point of view (Experiment C's
real timing, above, applies).

**In Kubernetes**, a pod termination sends `SIGTERM`, waits up to
`terminationGracePeriodSeconds`, then `SIGKILL`s anything still running.
A consumer without a working graceful-shutdown path effectively becomes an
abrupt failure on every single pod termination — including entirely
routine ones.

**During rolling deployments**, every replaced pod is a deliberate,
routine consumer departure. If that departure is graceful, it costs a
near-instant, single reassignment per replaced pod (Experiment B). If it
is not, every replaced pod costs a full `session.timeout.ms` detection
delay (Experiment C) — multiplied by however many pods the rollout
replaces.

**Under autoscaling**, a scale-down event is, from Kafka's point of view,
indistinguishable from Experiment A followed immediately by Experiment B
or C for each removed instance. See "Kubernetes autoscaling and consumer
groups," below, for why scale-*up* is often the more surprising case.

## Production engineering notes

### Consumer parallelism and scaling

```text
effective consumer parallelism <= partition count
```

Real evidence above already proved the ceiling directly (consumers 4 and
5 sat idle against 3 partitions). The scaling consequence: **increasing
consumer-group replicas beyond the partition count does not increase
processing throughput for that group** — there is no more work to hand
out. If more parallelism is genuinely needed, the partition count itself
has to grow (WP-04's territory, with WP-04's own remapping consequences),
not just the consumer count.

### Failure detection configuration — verified defaults

Verified directly against `kafka-clients:4.3.1`'s `ConsumerConfig` source
(not assumed):

| Config | Kafka's own default | This lab's value | Role |
|---|---|---|---|
| `session.timeout.ms` | 45000 | 10000 | Upper bound on how long the coordinator waits without a heartbeat before considering a member dead |
| `heartbeat.interval.ms` | 3000 | 3000 (unchanged) | How often the background heartbeat thread pings the coordinator, classic protocol |
| `max.poll.interval.ms` | 300000 | 20000 | Upper bound on time between `poll()` calls before this member proactively leaves the group |

This lab shortens two of the three, explicitly, only to make Experiments
C and the slow-consumer experiment (below) observable inside a lab
session — see `ConsumerGroupMemberApp`'s Javadoc for the full reasoning.
**These are not production recommendations.** A real deployment's values
depend on acceptable failure-detection latency versus tolerance for
false-positive evictions during transient slowness — a trade-off, not a
default to copy.

### Slow consumers and poison records — real result

`max.poll.interval.ms` (shortened to 20000 for this lab) is enforced
**independently of `session.timeout.ms`**, by the same background
heartbeat thread that can keep a member's heartbeats flowing even while
the foreground thread is blocked in slow processing (the same fact
WP-02A's failure-matrix correction already established for this
repository). Running `ConsumerGroupMemberApp` with a 25-second
per-record processing delay against a 20-second `max.poll.interval.ms`
produced this real, verbatim log line, from the background heartbeat
thread, **before the foreground thread's 25-second sleep even returned**:

```text
[kafka-coordinator-heartbeat-thread | slow-consumer-demo] WARN ConsumerCoordinator -
consumer poll timeout has expired. This means the time between subsequent calls to
poll() was longer than the configured max.poll.interval.ms, which typically implies
that the poll loop is spending too much time processing messages. You can address
this either by increasing max.poll.interval.ms or by reducing the maximum size of
batches returned in poll() with max.poll.records.
```

Immediately followed by a proactive `LeaveGroup`, sent by that same
background thread — the foreground loop had not yet returned from
processing even its first record.

**What this means for a poison or slow record in production:** one record
that takes too long to process doesn't just delay the records behind it
on the same partition (an ordering/lag cost within this member) — past
`max.poll.interval.ms`, it can cost this member its *group membership
entirely*, handing every partition it owned to someone else while it is
still, unaware, working through its already-fetched batch. That creates a
real window where two consumers can be acting on overlapping data: the
evicted member finishing records from a batch it fetched before eviction,
and the new owner independently fetching and processing the same
partition from its last committed offset. This repository does not
implement a retry/DLQ architecture to address this here — that is a later
work package's job (`docs/roadmap/PRINCIPAL_ENGINEER_FAILURE_MATRIX.md`
already reserves it) — this document's job is only to make the mechanism
precise enough that a future retry/DLQ design has something real to
defend against.

### Kubernetes autoscaling and consumer groups

Scaling a consumer deployment up is not free parallelism the way scaling
a stateless web service is: every new pod triggers a real rebalance
(Experiment A's cost) among the *existing* members too, under the eager
protocol, and — per "Consumer parallelism," above — a scale-up past the
partition count adds pods that do nothing. Scaling down triggers
Experiment B or C's cost for every removed pod, depending on whether the
autoscaler gives it time to shut down gracefully. An autoscaler tuned
purely on CPU/memory, with no awareness of partition count or rebalance
cost, can create exactly the "frequent rebalances" problem this document
already showed is expensive — sized correctly, up to the partition count,
autoscaling consumer replicas is reasonable; sized without that ceiling in
mind, it is a source of unnecessary rebalance churn.

## Local lab vs. production

| Lab | Production |
|---|---|
| 3 partitions, up to 5 consumer processes on one machine | Partition counts and consumer-group sizes capacity-planned per WP-04's framework |
| `taskkill /F` to simulate failure | Real process crashes, OOM kills, node failures, network partitions |
| One broker (WP-02's environment) | A cluster where the group coordinator itself can fail over |
| Manually observed rebalance timestamps | Continuous rebalance-rate and rebalance-duration metrics, alerted on |
| `RangeAssignor` observed by default | An organization may deliberately standardize on `CooperativeStickyAssignor` (`group.protocol=consumer`) after real evaluation |
| Shortened `session.timeout.ms`/`max.poll.interval.ms` for lab observability | Values chosen from a real failure-detection-latency vs. false-positive-eviction trade-off |
| A single automation shell unable to send a graceful OS signal | Real orchestrators (Docker, Kubernetes) that reliably deliver `SIGTERM` before `SIGKILL` |

## Interview takeaways

- "Kafka guarantees ordering" is incomplete; "a consumer group balances
  load" is incomplete too, without the partition-count ceiling attached.
- Rebalancing is not inherently bad — *frequent, avoidable* rebalancing is
  the actual production concern, and its cost is protocol-dependent
  (eager vs. cooperative), not fixed.
- Graceful shutdown is not a nicety; it is the difference between an
  instant reassignment and a multi-second (`session.timeout.ms`-bounded)
  gap in partition coverage, repeated on every single deployment or
  scaling event that doesn't handle it.
- "Session timeout" and "max poll interval" answer different questions —
  one is about heartbeat liveness, the other about processing-loop
  liveness — and this lab's own real log line shows the background thread
  enforcing the second one independently of the first.
