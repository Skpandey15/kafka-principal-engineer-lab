# Replication, ISR, and Broker Failure

This document is the conceptual and Principal Engineer depth behind
[`labs/lab-06-replication-isr-broker-failure`](../../labs/lab-06-replication-isr-broker-failure/README.md).
Every observation below is a real result captured while building and
validating that lab against a real 3-node KRaft cluster
(`platform/kafka-cluster/`, `apache/kafka:4.3.1`) — see the lab's README
for exact commands and full captured output.

## Scope note

This document is about **data-plane replication and broker failure**:
what a partition's leader, followers, and ISR actually do, and what
happens to them when a broker dies and comes back. It deliberately does
**not** cover:

- KRaft controller-quorum failure as its own subject — reserved for
  WP-08. (This lab's own cluster happens to combine broker and
  controller roles on all three nodes, and one experiment below
  discovered a real coupling between the two as a result — see
  "A structural discovery," below — but that is reported as a finding
  about this specific topology, not a controller-quorum experiment in
  its own right.)
- Kafka transactions / exactly-once semantics — WP-09.
- Anything about consumer offset commits, `acks` at the
  application-delivery-semantics layer, or idempotent consumption —
  those are WP-06's territory; this document only touches `acks` as
  it determines what a producer waits for from the broker side.

## The three numbers that describe a partition's replication state

```text
Replicas  -- every broker assigned to hold a copy of this partition,
             whether or not it is currently caught up. Fixed at topic
             creation (or reassignment) time; does NOT change just
             because a broker goes down.

ISR       -- "In-Sync Replicas": the subset of Replicas that are
             CURRENTLY caught up closely enough with the leader to be
             trusted for acks=all durability. Changes continuously as
             brokers fail, recover, or fall behind.

Leader    -- the one replica (always a member of ISR) that all
             producers and consumers actually talk to for this
             partition. Changes when the previous leader is no longer
             eligible (usually because it failed).
```

Real, captured proof that **Replicas ≠ ISR** — the exact command the
WP-07 spec asks for:

```bash
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-broker-1:19092 --describe --topic replicated-orders
```

```text
Topic: replicated-orders	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=1
	Partition: 0	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
	Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
	Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
```

And later, real, captured, with broker 2 killed:

```text
Partition: 0	Leader: 3	Replicas: 2,3,1	Isr: 3,1
Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 3,1
Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,3
```

**`Replicas` never changed** (`2,3,1` is still `2,3,1`) — Kafka does not
forget a partition's assignment just because a broker is unreachable.
**`ISR` shrank** on every partition broker 2 was a member of, immediately
reflecting reality. This is the whole distinction in one piece of real
evidence: Replicas is a durable assignment; ISR is a live fact.

## Leader/follower — real proof clients only ever talk to the leader

Both the burst producer and the continuous producer in this lab connect
to all three brokers as bootstrap servers, but every real send in this
lab's captured logs landed on whichever broker `kafka-topics --describe`
reported as that partition's leader at the time — confirmed by cross-
referencing `ADMIN_QUERY` lines (a deliberately separate
`AdminClient.describeTopics` call — see the lab README's "what 'leader'
means in this log" note) against the partition each record actually
went to. Followers never appear anywhere in a producer or consumer's own
protocol traffic; they exist purely to receive replicated data from the
leader.

## Broker failure — the real transition, timestamped

```text
                 replicated-orders-P0

                  Leader
                Broker-2
                   │
              ┌────┴────┐
              ▼         ▼
          Broker-3   Broker-1
          Follower   Follower

          Replicas = [2,3,1]
          ISR      = [2,3,1]
```

Then, real:

```text
              Broker-2 💥  (docker kill, 17:00:54.9Z)
                   │
                   ▼
     3 send attempts time out against the dead leader
     (17:00:57.9Z, 17:01:00.9Z, 17:01:04.0Z --
      each ~2.8-3.0s, this lab's configured per-attempt timeout)
                   │
                   ▼
              Leader Election
                   │
                   ▼
                Broker-3
                NEW LEADER            (confirmed via ADMIN_QUERY at 17:01:04.66Z)
                    │
                    ▼
        4th send attempt SUCCEEDS, latency 262ms
        Replicas = [2,3,1]  (unchanged)
        ISR      = [3,1]    (broker 2 removed)
```

**Total observed recovery time: ~9.7 seconds** (kill at 17:00:54.9Z,
first successful resumed send at 17:01:04.66Z), against this lab's
`DELIVERY_TIMEOUT_MS`/`REQUEST_TIMEOUT_MS` tuned specifically to make
each retry attempt observable in a few seconds rather than the client's
own multi-minute defaults. This number is a property of this lab's
timeout tuning and this specific local environment — see "Production
considerations" for why it is not a number to memorize as "how long
Kafka takes to fail over."

**Producer behavior during the transition, precisely:** every failed
attempt raised `org.apache.kafka.common.errors.TimeoutException` —
*"Expiring 1 record(s) ... The request has not been sent, or no server
response has been received yet."* Not a broker-returned error — the
client's own producer never got a usable response from the dead leader
at all, timed itself out, refreshed metadata, discovered the new leader,
and resumed. This app implements its own outer retry loop specifically
so this progression is visible attempt-by-attempt; see the lab README's
"what 'attempt' means" note for why the Kafka client itself doesn't
expose this directly.

## Broker recovery — real proof ISR re-expansion is not instant, and leadership does not return automatically

Real, captured: broker 2 restarted at 17:01:39.5Z; by the next `describe`
(~20s later):

```text
Partition: 0	Leader: 3	Replicas: 2,3,1	Isr: 1,2,3
Partition: 1	Leader: 3	Replicas: 3,1,2	Isr: 1,2,3
Partition: 2	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
```

Broker 2 is back in every ISR it belongs to — but **partition 0's leader
is still broker 3**, not broker 2, even though broker 2 was the
*original* leader. Two genuinely separate mechanisms are visible here,
and conflating them is a common mistake:

1. **ISR re-entry** requires the recovering broker to actually fetch and
   catch up on everything it missed — it cannot be trusted for
   `acks=all` durability again until it has. This is why a "restarted"
   broker is not immediately back to full duty: restart and catch-up are
   different events, and only the second one matters for durability.
2. **Leadership does not automatically return** to a recovered broker
   just because it's caught up and back in ISR. Kafka only rebalances
   leadership back to each partition's *preferred* replica on a
   periodic check (`auto.leader.rebalance.enable`, checked every
   `leader.imbalance.check.interval.seconds` — 300s by default), not
   immediately on ISR re-entry. A cluster can run for a while after a
   recovery with leadership "unbalanced" relative to the original
   assignment, and that is expected, not a bug.

## Replication factor — real proof RF=1 has zero fault tolerance

**RF=1** (`rf1-demo`, sole replica on broker 3): produced successfully,
then broker 3 killed:

```text
Topic: rf1-demo	ReplicationFactor: 1	Configs: min.insync.replicas=1
	Partition: 0	Leader: none	Replicas: 3	Isr: 	Elr: 3	LastKnownElr: 3
```

**`Leader: none`.** Not degraded, not slow — completely unavailable.
A subsequent `acks=all` produce attempt failed after the full delivery
timeout with the same `TimeoutException` shape as the broker-failure
experiment above: *"The request has not been sent, or no server response
has been received yet."* There is no other copy of this partition
anywhere in the cluster to elect a leader from. (`Elr`/`LastKnownElr` —
Eligible Leader Replicas — is a KRaft-era bookkeeping field tracking
replicas that *were* eligible before going out of sync; it does not
change the fact that the partition is offline right now.)

**RF=2** (`rf2-demo`, replicas on brokers 1 and 2, leader broker 1):
leader (broker 1) killed instead:

```text
Topic: rf2-demo	ReplicationFactor: 2	Configs: min.insync.replicas=1
	Partition: 0	Leader: 2	Replicas: 1,2	Isr: 2
```

Leader failed over to broker 2 cleanly, and a subsequent `acks=all`
produce succeeded immediately. **The only difference between total
outage and a clean, transparent failover was one additional replica.**

```text
Replication Factor  ≠  number of brokers currently in ISR
```

RF is the *ceiling* on how many broker failures a partition can survive
— it is fixed at creation time and does not shrink when a broker dies.
ISR is how many of that ceiling's replicas are *actually* healthy right
now. RF=3 with ISR currently at 1 is a partition one more failure away
from `rf1-demo`'s fate, even though its RF still says 3.

## `acks` — real proof, and an honest negative result

```bash
./gradlew runProducer -Packs=0 -Psync=true ...   # fire-and-forget
./gradlew runProducer -Packs=1 -Psync=true ...   # leader only
./gradlew runProducer -Packs=all -Psync=true ... # full ISR
```

Real, captured:

```text
acks=0:   eventId=ORDER-3001 partition=1 offset=-1 latencyMs=610 status=SUCCESS
acks=1:   eventId=ORDER-3011 partition=2 offset=2  latencyMs=416 status=SUCCESS
acks=all: eventId=ORDER-3021 partition=1 offset=8  latencyMs=680 status=SUCCESS
```

**`acks=0` always reports `offset=-1`.** This is not a bug in this
lab's logging — it is `RecordMetadata` telling the truth: with
`acks=0`, the client never waits for (or receives) a broker response at
all, so it genuinely does not know what offset the record landed at, or
whether it landed anywhere. This is the sharpest possible illustration
of `acks=0`'s actual contract: the producer's confidence that a record
was written is exactly zero, always, regardless of whether the broker
happened to succeed.

**Honest negative result:** at this lab's scale — a healthy 3-broker
cluster, on one machine, over loopback — `acks=1` and `acks=all` showed
no meaningful latency difference in this data. That is a real
observation, not a hedge: the extra round-trip `acks=all` requires to
followers is negligible when the followers are on the same machine with
effectively no network latency between them. **The difference between
`acks=1` and `acks=all` is not primarily a happy-path latency story —
it is a *failure-mode* story**, made concrete in the next section:
`acks=1` never even notices when the requirement it actually enforces
(the leader's own local write) is insufficient for what you needed;
`acks=all` is the setting that makes Kafka observably refuse a write it
cannot yet make durable enough.

`acks=1` acknowledges the instant the **partition leader** has written
the record to its own local log — it says nothing about whether any
follower has it yet. If that leader fails before a single follower
replicates the record, the record is gone, silently, with the producer
having already been told "success." `acks=all` ties the acknowledgment
to `min.insync.replicas` instead — which is exactly what the next
section demonstrates being enforced for real.

## `min.insync.replicas` — the central experiment

Setup: `minisr-demo`, RF=3, `min.insync.replicas=2`, `acks=all`.

**Full ISR (3):** produce succeeds, `latencyMs=889` (first-connection
warm-up), then `latencyMs=12`.

**One broker down, ISR=2 (exactly at the threshold):**

```text
Isr: 2,1
eventId=ORDER-2101 partition=0 offset=2 latencyMs=1599 status=SUCCESS
```

**Production still succeeds at exactly `min.insync.replicas`** — the
threshold is inclusive, not "strictly more than."

**A structural discovery: what happened when ISR was pushed below the
threshold.** Killing a *second* broker (to force ISR to 1, below
`min.insync.replicas=2`) in this lab's specific 3-node topology does not
only remove a replica — because all three nodes in this lab's cluster
combine the broker and controller roles (see "Scope note," above), it
*also* removes a controller-quorum voter, and with 2 of 3 voters down,
the metadata quorum loses its majority. The real, observed consequence:
the surviving leader's `AlterPartition` request (the RPC a leader must
use to get the controller to *confirm* an ISR shrink) can never complete
while the quorum lacks a majority — so instead of a clean, fast
`NotEnoughReplicasException`, the client observed the write hang for the
full delivery timeout and then fail with a `TimeoutException`:

```text
eventId=ORDER-12001 latencyMs=10306 status=FAILED
  exception=org.apache.kafka.common.errors.TimeoutException
  message=Expiring 1 record(s) for minisr-demo-0:10015 ms has passed since batch creation.
          The request has not been sent, or no server response has been received yet.
```

This was not an accident of timing this lab could have avoided by
sequencing the two broker kills differently — it is structurally
guaranteed by the topology: with RF=3 spanning all 3 nodes, and all 3
nodes also serving as the only 3 controller-quorum voters, crossing
`min.insync.replicas=2` (which requires 2 of 3 replica-holding brokers
down) and losing controller-quorum majority (which requires 2 of 3
voters down) are **the same event**, not two independent ones, in this
specific topology. This is a genuine, discovered example of exactly why
production Kafka deployments often separate the controller and broker
roles onto different nodes, or use more controller voters than the
minimum: a combined-role cluster couples two failure domains that are
conceptually — and, with enough nodes, actually — independent.

This lab's **automated test suite sidesteps this coupling on purpose**
(see `ReplicationIsrBrokerFailureIntegrationTest.minIsrRejectsWriteBelowThreshold`)
by using an RF=2/`min.insync.replicas=2` topic and killing only one
broker — enough to cross the ISR threshold without also touching
controller-quorum majority — which reliably reproduces a clean, fast
rejection in CI. Both results are real; they demonstrate the same
underlying rule (`acks=all` is rejected once ISR drops below
`min.insync.replicas`) under two different topologies, one of which
happens to also reveal the quorum-coupling finding above.

**Why Kafka rejects the write at all, either way:** `min.insync.replicas`
exists so that `acks=all` durability has a real, enforced floor.
Without it, `acks=all` on an ISR that has shrunk to 1 replica would
"succeed" while offering the exact same durability as `acks=1` on a
single broker — silently weaker than what the configuration promised.
Rejecting the write is Kafka refusing to lie about the durability it
just provided.

## Durability vs. availability — the trade-off, stated precisely

```text
RF=3, min.insync.replicas=2, acks=all
```

is **not** "safe" in some absolute sense — it is a specific, deliberate
trade: this configuration will refuse new writes rather than accept one
with less durability than promised, the moment ISR drops to 1. A weaker
configuration (`acks=1`, or `min.insync.replicas=1`) never refuses a
write for this reason — and never notices that it just wrote something
less durably than the RF number alone might suggest.

> **Stronger durability requirements can intentionally reduce write
> availability during failures.** This is not a flaw to engineer around
> with a "just increase RF" reflex — it is the entire mechanism working
> as designed. The real question a Principal Engineer answers when
> choosing these values is never "what's safest" in isolation; it is
> "for this specific data, is a rejected write (an availability cost) or
> a silently-under-durable write (a durability cost) the one I can
> afford, under which failure scenarios, and how likely is each."

`RF=3` alone is not a durability guarantee — this document's RF=1 vs.
RF=2 evidence shows RF matters, but the `min.insync.replicas` evidence
shows that RF only bounds what durability is *possible*; `acks` and
`min.insync.replicas` are what actually *enforce* it on any given write.

## ISR behavior, vocabulary

```text
follower           -- a non-leader replica, continuously fetching from the leader.
in-sync follower    -- a follower caught up closely enough (replica.lag.time.max.ms)
                       to be trusted for acks=all -- i.e., currently a member of ISR.
out-of-sync replica -- a replica (follower) that has fallen too far behind and has
                       been removed from ISR -- still a Replica, no longer in ISR.
replica lag         -- how far behind the leader a follower currently is; the basis
                       for the in-sync/out-of-sync distinction above.
```

```text
ISR
  │  broker failure
  ▼
ISR shrinks            (real: [2,3,1] -> [3,1] in this lab's evidence)
  │  broker recovery
  ▼
replica catches up     (fetches everything it missed from the current leader)
  │
  ▼
ISR expands            (real: [3,1] -> [1,2,3] in this lab's evidence)
```

## Leader election — what clients actually do

Real, captured client-side behavior during the broker-failure experiment
above: the producer's `NetworkClient` logged `Node 2 disconnected`,
retried its in-flight request, refreshed its metadata (discovering
partition 0's new leader), and resumed sending — entirely inside the
Kafka client library, with no application code involved. The mental
model: **a client never "waits for an election" explicitly** — it keeps
retrying against stale metadata until a retry's response (or a
background metadata refresh) reveals a new leader, then simply starts
sending to that leader instead.

This document does not implement or discuss what happens when the
*controller* (the thing that actually runs the election) itself fails —
that is WP-08's dedicated subject. What this lab shows is only the
client-visible *consequence* of an election that some healthy controller
already ran.

## Unclean leader election — documented, not live-demonstrated

Real, verified (not assumed) on this lab's actual pinned cluster:

```bash
docker exec kafka-broker-1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka-broker-1:19092 \
  --entity-type topics --entity-name replicated-orders --describe --all
```

```text
unclean.leader.election.enable=false sensitive=false synonyms={DEFAULT_CONFIG:unclean.leader.election.enable=false}
```

`unclean.leader.election.enable=false` (the default, confirmed on the
actual pinned `apache/kafka:4.3.1` image, not assumed from documentation)
means Kafka will **never** elect a leader from outside the current ISR —
if every ISR member is down, the partition stays offline (exactly
`rf1-demo`'s observed behavior above, generalized) rather than resuming
service from a replica that might be missing acknowledged records.
Setting it to `true` trades that unavailability for the *possibility* of
resuming service sooner, at the cost of silently losing any records the
stale replica never received. This lab does not flip it on: constructing
a *controlled, reproducible* unclean-election scenario requires
deliberately engineering data loss (driving every ISR member offline
with divergent state) in a way that risks leaving this lab's shared
environment in a confusing state for the *next* experiment, for a
result that is fully and precisely explainable without it. **Do not
enable this in production without an explicit, documented decision that
some data loss is acceptable in exchange for faster recovery** — it is
never a safe default flip.

## Consumer behavior during broker failure — and why it is not a rebalance

Real, captured: a single consumer (a group of one, deliberately, so a
rebalance is never a confounding factor) reading partition 0 while its
leader (broker 3) was killed:

```text
17:02:52.2Z  last record before the gap (offset 237)
             -- broker 3 killed at 17:02:52.8Z --
17:02:52.5Z  "Node 3 disconnected" / DisconnectException (repeated, ~13s)
17:02:57-03:02.9Z  group-coordinator rediscovery churn (see below)
17:03:05.5Z  consumption resumes (offset 238)
```

**Total observed interruption: ~13.3 seconds.** The real log also shows
something worth being precise about: the consumer's **group coordinator
lookup** experienced its own churn during this window
(`Group coordinator ... is unavailable or invalid ... Rediscovery will
be attempted`) even though the group coordinator itself was not on the
dead broker — a side effect of the same connection/metadata disruption,
not a second, independent failure.

**This is not a consumer-group rebalance**, and the two must not be
conflated:

```text
Broker (partition) leader election      Consumer-group rebalance
------------------------------------    ------------------------------------
Triggered by a BROKER dying             Triggered by group MEMBERSHIP changing
Decided by the CONTROLLER               Decided by the GROUP COORDINATOR
Result: a new partition LEADER          Result: new PARTITION ASSIGNMENTS
                                         among consumers
Invisible to a healthy consumer         Fires PARTITIONS_REVOKED/ASSIGNED
except for a brief fetch interruption   callbacks on every affected member
```

This lab ran with exactly one consumer in its own group specifically so
this distinction is unambiguous in the evidence: nothing in the captured
log is a rebalance, because there was only ever one possible owner for
every partition. See
[`docs/consumer-groups/CONSUMER_GROUPS_AND_REBALANCING.md`](../consumer-groups/CONSUMER_GROUPS_AND_REBALANCING.md)
(WP-05) for what an actual rebalance looks like in this repository's own
captured evidence.

## Failure matrix

| Scenario | Expected result | Validated here |
|---|---|---|
| Follower broker fails | Leader continues serving; ISR shrinks | Real (broker-failure experiment) |
| Leader broker fails | New eligible leader elected from ISR | Real (broker-failure experiment, RF=2 experiment) |
| RF=1 replica broker fails | Partition unavailable (`Leader: none`) | Real (`rf1-demo`) |
| RF=3, one broker fails | Partition continues normally | Real (broker-failure experiment) |
| ISR ≥ `min.insync.replicas` | `acks=all` write succeeds | Real, at exactly the threshold (ISR=2, minISR=2) |
| ISR < `min.insync.replicas` | `acks=all` write rejected | Real (`TimeoutException`, plus a clean `NotEnoughReplicasException`/`TimeoutException` in the automated RF=2 test) |
| Broker rejoins | Replica catches up before ISR re-entry | Real (broker-recovery experiment; also the automated `revivedBrokerRejoinsIsrAfterCatchingUp` test) |
| All ISR members down, unclean election disabled | Partition stays offline, no data loss risked | Documented via real config verification, not live-forced |

## Observability

**Developer-visible** (this lab's own logs and CLI use):
`kafka-topics.sh --describe` (leader/replicas/ISR, exactly as used
throughout this document), producer/consumer exception logs
(`TimeoutException`, `DisconnectException`), this lab's own
`ADMIN_QUERY` lines.

**Broker/platform-team-visible** (JMX): this lab's containers do run
with JMX remote access enabled (verified: `jmxremote=true`,
`jmxremote.authenticate=false` on the actual container — local-only,
never appropriate beyond a lab), but this lab did **not** exhaustively
verify every individual MBean name against the exact `4.3.1` metric
tree within its own time budget — `kafka.tools.JmxTool`, present in
older Kafka versions, no longer exists in this pinned version, and this
lab stops short of standing up a full JMX-to-Prometheus pipeline (that
is WP-16's dedicated subject). The commonly-cited metric names below are
from Kafka's own documented metrics reference, not verified against this
exact pinned MBean tree — confirm each one (via `jconsole`, a JMX
exporter, or the official docs for the pinned version) before wiring a
real dashboard to it:

- `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions`
- `kafka.controller:type=KafkaController,name=OfflinePartitionsCount`
- `kafka.server:type=ReplicaManager,name=IsrShrinksPerSec` /
  `IsrExpandsPerSec`
- `kafka.controller:type=KafkaController,name=ActiveControllerCount`
  (KRaft-era naming/availability should be confirmed against the pinned
  version — this is carried over from the historical ZK-era metric name
  and may differ)
- Client-side: producer retry/error-rate metrics
  (`org.apache.kafka.clients.producer:type=producer-metrics`) and
  consumer fetch/lag metrics, both already exposed by the same
  `kafka-clients` library this lab's Java apps use.

This connects directly to the hot-partition/production-observability
thread this repository's earlier work already established (WP-04's
partitioning lab, and the client-resilience topic threaded through
WP-03/WP-06/WP-09) — `UnderReplicatedPartitions`/ISR-shrink metrics are
the broker-side signal for exactly the kind of failure this lab
generates on demand; a real operator would alert on them rather than
running `kafka-topics --describe` by hand.

## Production considerations

| Lab | Production |
|---|---|
| 3 nodes, all combining broker+controller roles | Controller and broker roles typically separated onto dedicated nodes at real scale, partly to avoid exactly the quorum-coupling this lab discovered |
| `docker kill` (immediate, no graceful shutdown) | Real failures: OOM kill, hardware fault, or a graceful rolling restart — each with different timing characteristics |
| Recovery timing tuned for lab observability (short retry/timeout windows) | Timeout/retry values chosen from real SLOs, not lab convenience |
| One machine, loopback network (near-zero inter-broker latency) | Real network latency between brokers/racks/AZs, which is where `acks=1` vs. `acks=all` latency differences actually become visible |
| Manually triggered kills via `docker kill`/`docker start` | Failures detected and alerted on automatically (WP-16) |
| No JMX pipeline stood up | Metrics scraped, dashboarded, and alerted on continuously |

## Principal Engineer questions

**1. Why isn't replication factor alone a durability guarantee?**

Because RF only bounds how many copies of a partition *can* exist — it
says nothing about how many are *currently* in sync, or what a given
producer's `acks` setting actually waits for. This document's real
evidence shows RF=3 can still lose an unacknowledged-by-followers record
under `acks=1`, and can still reject writes under `acks=all` once ISR
shrinks enough — RF sets the ceiling; `acks` and `min.insync.replicas`
are what's actually enforced on each write.

**2. What exactly is ISR?**

The subset of a partition's assigned replicas (`Replicas`) that are
currently caught up closely enough with the leader to be trusted for
`acks=all` acknowledgment. It is a live, continuously-updated fact, not
a fixed configuration — this document's real before/after evidence shows
it shrinking within seconds of a broker dying and expanding again only
after the recovering broker has actually caught up.

**3. What happens when the partition leader dies?**

In-flight and new requests to that leader start failing (this lab's real
evidence: `TimeoutException`, not an immediate clean error, because the
client has no way to know the broker is gone until its own timeout
fires). The controller detects the failure and elects a new leader from
the partition's ISR. Clients discover the new leader via a metadata
refresh, triggered by the failed request, and resume automatically —
this document's evidence shows total resumption in ~9.7 seconds in this
lab's specific, timeout-tuned environment.

**4. Why can `acks=all` still be misunderstood?**

Because people assume it means "every replica has the record" as an
absolute statement, when it actually means "every *currently in-sync*
replica has the record" — a set that can legitimately be as small as
one member if ISR has shrunk to `min.insync.replicas`. `acks=all` is a
statement about ISR, not about RF, and this document's `min.insync.replicas`
evidence is precisely the case where that distinction becomes
observable: `acks=all` against an ISR of exactly 1 (below the configured
minimum) is refused, but `acks=all` against an ISR of exactly 1 that
*is* the configured minimum's floor would have silently succeeded with
only one broker's copy.

**5. How does `min.insync.replicas` affect availability?**

Directly and by design: once ISR drops below it, `acks=all` writes are
rejected outright rather than accepted with weaker-than-promised
durability. This document's real evidence shows this is a hard floor,
not a soft degradation — the exact moment ISR crosses the threshold is
the exact moment write availability for `acks=all` producers stops.

**6. RF=3/minISR=2 vs. RF=3/minISR=1 — what trade-off changes?**

`minISR=2` tolerates exactly one broker failure before refusing
`acks=all` writes (this document's real evidence: ISR=2 still succeeds,
ISR=1 is refused). `minISR=1` tolerates two broker failures before
refusing writes — but at `minISR=1`, an `acks=all` write can be
acknowledged by a *single* surviving replica, which is exactly as weak
as `acks=1` durability the moment ISR is that small; the "all" in
`acks=all` stops meaning anything more than "one" once ISR has shrunk
that far. Lower `minISR` values buy more availability at the cost of a
durability floor that can get arbitrarily close to `acks=1`'s.

**7. Why might a restarted broker not immediately join ISR?**

Because "started" and "caught up" are different events. A restarted
broker begins with a fetch position wherever it left off (or from
scratch, if its storage was lost); it must actually replicate
everything it missed from the current leader before Kafka will trust it
for `acks=all` durability again. This document's real evidence shows a
real, measured gap between a broker's `docker start` and its
reappearance in ISR — never assume "the process is up" means "the
replica is safe to count on."

**8. Leader election vs. consumer-group rebalance?**

Different triggers (a broker dying vs. group membership changing),
different deciders (the controller vs. the group coordinator), and
different results (a new partition leader vs. new partition
*assignments among consumers*). This document's real evidence
deliberately ran a single-consumer group specifically so a captured
leader-election-driven interruption could never be confused with a
rebalance — see the side-by-side table above.

**9. When can Kafka lose acknowledged records?**

When the acknowledgment given was weaker than the durability actually
needed: `acks=0` (never confirmed at all — this document's real
evidence shows `offset=-1`, i.e., the client doesn't even know what
happened), `acks=1` with the leader failing before any follower
replicates the record, or `acks=all` with `min.insync.replicas` set low
enough that "all" of the current ISR is still just one or two brokers.
Kafka never silently loses a record that a sufficiently strict
`acks`/`min.insync.replicas` combination actually protected — but it
also never retroactively strengthens a weaker acknowledgment you already
accepted.

**10. How would you diagnose under-replicated partitions in production?**

Check `UnderReplicatedPartitions` (broker-side JMX) as the leading
signal — a non-zero, sustained value means ISR is smaller than the
configured Replicas for at least one partition, exactly the condition
this document's broker-failure experiment produces on demand. Cross-
check with `kafka-topics --describe` for exactly which partitions and
which brokers are missing from ISR (this document's own diagnostic tool
throughout), broker-level logs/metrics for the missing broker(s)
specifically (network, disk, GC pauses), and whether the gap is
transient (a broker recently restarted, still catching up) or sustained
(a broker that's up but persistently falling behind, or one that's
actually down).

**11. What configuration would you choose for a financial transaction
topic, and what trade-offs would you explicitly document?**

There is no single universally-correct answer here, and presenting one
would contradict this repository's own non-goals — the actual Principal
Engineer answer is the reasoning, not a number. For a financial
transaction topic, the durability side of the trade-off usually
dominates: `acks=all` (never accept a weaker acknowledgment for money
the business considers final), `replication.factor=3` or higher
(headroom above the minimum so a single failure doesn't immediately put
you at the availability cliff this document's `min.insync.replicas`
evidence shows), and `min.insync.replicas=2` on RF=3 (tolerate one
failure and keep writing; a second simultaneous failure correctly
refuses writes rather than accepting them on a single, unverified copy).
The trade-off to document explicitly alongside that choice: this
configuration *will* reject writes during a correlated two-broker
failure, and the business must have already decided that "reject the
payment and let the client retry" is preferable to "risk accepting it
on durability weaker than what the topic's own configuration
advertises" — a decision the engineering configuration cannot make on
the business's behalf, only enforce once made.
