# KRaft Controller Quorum and Failure

This document is the conceptual and Principal Engineer depth behind
[`labs/lab-07-kraft-controller-quorum-failure`](../../labs/lab-07-kraft-controller-quorum-failure/README.md).
Every observation below is a real result captured while building and
validating that lab against a real, dedicated-role KRaft cluster
(`platform/kraft-quorum/`, `apache/kafka:4.3.1`, 3 controller-only
voters + 3 broker-only nodes) — see the lab's README for exact commands
and full captured output.

## Scope note

This document is about the **control plane**: the KRaft metadata
quorum, its controller leader, and what happens when controllers fail.
It deliberately does **not** cover Kafka transactions/EOS (WP-09),
schema evolution (WP-10), Kafka Connect/CDC (WP-11), the transactional
outbox (WP-12), or a full production monitoring stack (WP-16). It
builds directly on WP-07's replication/ISR/broker-failure work — one
experiment below (Experiment 11) is deliberately designed to connect
the two.

## Why this lab's topology is not WP-07's topology

WP-07's cluster combines the broker and controller roles on all 3 of
its nodes. That lab discovered, for real, that this coupling means
crossing `min.insync.replicas` (which needs 2 of 3 brokers down) and
losing controller-quorum majority (which needs 2 of 3 voters down) are
*the same event* in that specific topology — you cannot experimentally
separate "the control plane failed" from "the data plane failed" when
every node is both.

This lab uses 3 controller-only nodes and 3 broker-only nodes
specifically so the two failure domains are independent, exactly as a
real production cluster's own reasoning for this separation would be.
Real, captured proof the separation is genuine — the exact command WP-07's
own cluster cannot produce a non-empty answer to:

```bash
docker exec kraft-quorum-broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kraft-quorum-broker-1:19092 describe --status
```

```text
CurrentVoters:      [{"id": 1, ...}, {"id": 2, ...}, {"id": 3, ...}]
CurrentObservers:   [{"id": 4, ...}, {"id": 5, ...}, {"id": 6, ...}]
```

The 3 brokers appear as **observers** — they replicate the metadata log
(so every broker has a current view of cluster metadata) but hold no
vote in it. In WP-07's combined cluster, this same command's
`CurrentObservers` list is always empty, because every node there is
also a voter.

## Controller quorum fundamentals

```text
3 controllers
      |
      v
majority = 2
```

A KRaft controller quorum commits a new piece of metadata (a topic
creation, a partition reassignment, a leader election) only once a
**majority** of voters have replicated it — not all of them. With 3
voters, that majority is 2. This is not a Kafka-specific choice; it is
the same Raft-consensus math every majority-quorum system uses:
`floor(n/2) + 1` voters must agree, which is why:

```text
quorum != all nodes
```

A 3-voter quorum tolerates exactly **1** voter failure (majority = 2
remains reachable with 1 down) and stops tolerating anything once a
**second** voter is also down (only 1 remains, below the majority of
2). Odd voter counts are preferred over even ones for a concrete
reason: a 4-voter quorum's majority is *also* 3 (majority of 4 is
`floor(4/2)+1 = 3`), meaning a 4-voter quorum tolerates the **same
single failure** a 3-voter quorum does, for one extra always-on node
and one extra vote that can never actually improve fault tolerance —
you pay for a 4th voter and get zero additional resilience for it. A
5-voter quorum genuinely improves tolerance to 2 failures, at the cost
of 2 more always-on voters than a 3-voter quorum. The odd-number
convention is this trade-off made explicit: every added *even* voter
is pure cost with no fault-tolerance benefit.

## Inspecting the metadata quorum — real, captured

```bash
docker exec kraft-quorum-broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kraft-quorum-broker-1:19092 describe --status
```

```text
ClusterId:              6Zy-q9GlR3u9dQkH6Aj_4A
LeaderId:               3
LeaderEpoch:            1
HighWatermark:          628
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   192
CurrentVoters:          [{"id": 1, "endpoints": ["CONTROLLER://kafka-controller-1:29093"]}, {"id": 2, ...}, {"id": 3, ...}]
CurrentObservers:       [{"id": 4, "directoryId": "..."}, {"id": 5, ...}, {"id": 6, ...}]
```

| Field | Meaning |
|---|---|
| `ClusterId` | The shared cluster identity every node (controller or broker) in this cluster was formatted with. |
| `LeaderId` | The `node.id` of the controller currently acting as the metadata log's Raft leader — the **active controller**. |
| `LeaderEpoch` | A monotonically increasing counter, incremented every time a new controller leader is elected. Real, captured: this lab's epoch went `1 -> 3 -> 4 -> ... -> 21` across its experiments, jumping by more than 1 at times (an epoch bump can happen during an election attempt even if that particular candidate doesn't end up winning, so the epoch number itself is not a count of "how many leaders there have been," only a strictly increasing fencing token). |
| `HighWatermark` | The offset up to which the metadata log is committed (replicated to a majority) — analogous to a partition's high watermark, but for the metadata log itself. |
| `CurrentVoters` | The nodes that can vote in leader elections and must acknowledge a record before it's committed. |
| `CurrentObservers` | Nodes that replicate the metadata log (so they always have current cluster metadata) but cannot vote or become leader — every broker in this lab's topology. |

The Java-API equivalent, verified directly against the real,
source-inspected `kafka-clients:4.3.1` `QuorumInfo` class (see
`DescribeQuorumApp` in this lab):

```java
QuorumInfo quorum = admin.describeMetadataQuorum().quorumInfo().get();
quorum.leaderId(); quorum.leaderEpoch(); quorum.highWatermark();
quorum.voters(); quorum.observers();
```

## Active controller — a distinct role, not to be conflated

```text
Controller-1    FOLLOWER
Controller-2    LEADER      <- active controller
Controller-3    FOLLOWER
```

The **active controller** is the single node in the metadata quorum
currently authorized to commit new metadata. This is a genuinely
different mechanism from three other "leader"/"coordinator" concepts
this repository has already covered, and conflating any of them is a
common source of confused reasoning:

| Role | What it's the leader/coordinator *of* | Decided by |
|---|---|---|
| **Controller leader** (this WP) | The metadata quorum — topic creation, partition leader elections, broker registration | The controller quorum's own Raft election |
| **Partition leader** (WP-07) | One partition's data — which replica producers/consumers actually talk to | The controller, based on that partition's ISR |
| **Consumer-group coordinator** (WP-05) | One consumer group's membership and offset commits | A hash of the group ID onto a broker partition of `__consumer_offsets` |
| **Transaction coordinator** (WP-09) | One producer's transactional state | A hash of the transactional ID onto a broker partition of `__transaction_state` |

All four are legitimately called "leader" or "coordinator" in Kafka's
own vocabulary, and all four can be different physical nodes at the
same moment, coordinating completely independent concerns.

## Controller leader failure — real, timestamped

```text
                 KRaft Controller Quorum

             Controller-1     FOLLOWER
             Controller-2     FOLLOWER
             Controller-3     LEADER
                    |
                    v
              Controller-3 (LEADER) killed
                    |
                    v
        remaining voters (1, 2) detect the failure
                    |
                    v
              controller election
                    |
                    v
             Controller-2   NEW LEADER
             Controller-1   FOLLOWER
```

Real, captured: Controller-3 (the leader) killed at `2026-09-20T03:49:13.7Z`.
By the first poll ~6 seconds later:

```text
LeaderId:               2
LeaderEpoch:            3
```

**Observed election time: within ~6 seconds** of the kill (this lab
polled every ~2s and already saw the new leader on the very first
poll after the kill settled — the true election time is somewhere
inside that window, not exactly 6s). This number is a property of this
lab's specific timeout/heartbeat configuration and a loopback network
with zero real latency — see "Production considerations" for why it is
not a general SLA claim. **Do not assume which surviving voter becomes
the new leader** — this lab's own automated test
(`controllerLeaderFailureCausesANewLeaderToEmerge`) only asserts that
*a different* voter took over, never which specific one, because Raft
leader election does not guarantee a specific outcome among eligible
candidates.

## Kafka client behavior during one-controller failure — the central distinction

```text
CONTROL PLANE                          DATA PLANE
--------------                         ----------
controller quorum                      producer
metadata mutations                     partition leader
cluster management                     consumer
                                        replication
```

Real, captured: with a continuous producer and consumer already running
against `quorum-orders`, the **controller leader was killed** (the same
event as the previous section). Producer and consumer logs across that
exact window:

```text
2026-09-20T03:52:05.257Z  <- controller-2 (leader) killed
2026-09-20T03:52:05.6+  ...uninterrupted SUCCESS/consume lines continue, no gap...
2026-09-20T03:52:32.150Z  eventId=ORDER-CP-103 ... result=SUCCESS latencyMs=9
```

**Ordinary produce/consume traffic did not notice the controller
failure at all** — no error, no latency spike, no gap in either log.
This is the direct, experimental answer to why: producing to and
consuming from an *already-existing, already-led* partition never
contacts the controller. The producer only needs to know the current
partition leader (a fact it already has cached, and which the broker
itself, not the controller, continues to serve fetch/produce requests
for); the controller is only consulted when the *metadata itself* needs
to change.

## Metadata operations with quorum intact (2/3) — real, captured

With controller-2 (the former leader) still down and only 2 of 3
controllers up — majority (2) still exists — real, captured results
against `kraft-quorum-broker-1:19092`:

```text
$ kafka-topics.sh --create --topic disposable-topic --partitions 1 --replication-factor 1
Created topic disposable-topic.

$ kafka-configs.sh --entity-type topics --entity-name disposable-topic --alter --add-config retention.ms=3600000
Completed updating config for topic disposable-topic.

$ kafka-topics.sh --describe --topic disposable-topic
Topic: disposable-topic  ...  Configs: min.insync.replicas=1,retention.ms=3600000
        Partition: 0  Leader: 5  Replicas: 5  Isr: 5

$ kafka-topics.sh --delete --topic disposable-topic
(succeeded)
```

Every metadata operation attempted — create, alter, inspect, delete —
succeeded normally, with no observable delay, while one of three
controllers was down.

```text
one controller failure  !=  control-plane outage
```

as long as quorum majority remains.

## Quorum-loss experiment — real, captured

Starting from 3/3 healthy, killing 2 of the 3 controllers (only 1
voter, `kafka-controller-2`, remains — below the majority of 2):

```bash
docker kill kafka-controller-1 kafka-controller-3
```

**The learner should not assume every operation fails identically —
this lab's own real results do not:**

| Operation attempted | Real result |
|---|---|
| `kafka-metadata-quorum.sh describe --status` | `TimeoutException: The request timed out.` — even this READ-ONLY quorum-status query needs the leader specifically to answer it, and no leader can be elected without a majority. |
| `kafka-topics.sh --list` | **Succeeded immediately**, returning the real topic list. A broker can answer this from its own locally-replicated metadata (it's an observer of the metadata log, and already has a current copy) without contacting the controller at all. |
| `kafka-topics.sh --describe --topic <existing topic>` | **Failed** — `TimeoutException` / `DisconnectException`, from an internal `listPartitionReassignments` call the `describe` command bundles in, which *does* require the controller. The basic leader/replicas/ISR information alone would not have needed it. |
| `kafka-topics.sh --create ...` | Failed: `TimeoutException`, root cause `DisconnectException ... due to node 4 being disconnected`. |
| `kafka-configs.sh --alter ...` | Failed: `TimeoutException`, root cause `DisconnectException ... due to node 5 being disconnected` (a *different* node than the create-topic failure above — which specific connection got cut varied between calls). |

The precise, honest lesson here: **"metadata operations fail without
quorum" is true, but not uniform.** Some read-only operations succeed
via a broker's local metadata cache; others (like `describe`, because
of an internal implementation detail) fail even though their core data
doesn't strictly need the controller; writes always fail, but the
specific exception and how quickly it surfaces varied by call. Do not
generalize a single failure mode from one experiment.

## Existing producer/consumer traffic during quorum loss — real, captured

With controller quorum fully lost (1 of 3 voters remaining) and the
same continuous producer/consumer from the earlier experiment still
running against the already-existing, already-led `quorum-orders`
partitions:

```text
2026-09-20T03:53:33.5Z   <- controllers 1 and 3 killed; only controller-2 remains (no majority)
2026-09-20T03:54:05.472Z eventId=ORDER-CP-329 ... result=SUCCESS latencyMs=8
```

**Traffic continued completely uninterrupted for over 30 seconds with
zero controller-quorum majority** — same conclusion as the
one-controller-down experiment, now taken to its logical extreme: the
data plane's continuity does not depend on quorum *at all*, as long as
nothing about the metadata itself needs to change.

## Broker failure while controller quorum is unavailable — real, captured (this connects WP-07 and WP-08)

This is the experiment that proves the boundary of the previous
section's conclusion. With quorum still fully unavailable (1 of 3
voters), the broker leading `quorum-orders` partition 0 — the exact
partition the continuous producer was writing to — was killed:

```bash
docker kill kraft-quorum-broker-2   # partition 0's leader
```

Real, captured producer log, before and after:

```text
2026-09-20T04:00:02.678692Z   eventId=ORDER-CP-1199 ... result=SUCCESS latencyMs=12
                                        <- broker killed at 04:00:03.0Z ->
2026-09-20T04:00:09.081Z  eventId=ORDER-CP-1200 result=FAILED latencyMs=6002 exception=TimeoutException
2026-09-20T04:00:11.085Z  eventId=ORDER-CP-1201 result=FAILED ... Expiring 2 record(s) for quorum-orders-0
2026-09-20T04:00:17.487Z  eventId=ORDER-CP-1202 result=FAILED latencyMs=6001 exception=TimeoutException
... (continues failing, unrecovered, for the entire duration quorum remained unavailable) ...
```

The consumer's log shows the same story from the other side: repeated
`DisconnectException`, never recovering, for as long as quorum stayed
down. **This failure did not self-heal.** It is fundamentally different
from every prior "data plane keeps working" result in this document,
for a precise reason: partition 0's leader is now gone, and **electing
a new leader for it is itself a metadata mutation** — exactly the kind
of operation the previous two sections already proved requires
controller quorum. With no quorum available to process that election,
partition 0 has no path back to availability, no matter how long you
wait.

```text
existing data path may continue
        does NOT imply
the cluster can safely react to every NEW failure
```

The distinction is precise: traffic to a partition survives quorum loss
only as long as that partition's *existing* leader stays alive. The
moment a *new* failure requires the controller to act — a leader
election, exactly as WP-07 studied under a healthy control plane — the
absence of quorum becomes an outage for that partition, not a
performance blip.

**Recovery, real and captured:** restoring one controller (bringing
quorum back to 2 of 3, a majority) allowed a new leader to be elected
for partition 0 within seconds, and both producer and consumer resumed
automatically with no manual intervention:

```text
2026-09-20T04:01:00.4Z   <- controller-1 restarted, majority (2/3) restored
2026-09-20T04:01:40.811Z eventId=ORDER-CP-1248 ... result=SUCCESS latencyMs=13   <- resumed
```

## Controller recovery — real, captured

```text
quorum lost (1/3)
    |
    v
one controller restored -> majority (2/3) returns
    |
    v
controller leader available again
    |
    v
metadata operations resume (real: create-topic succeeded again within 15s)
    |
    v
remaining controller restored -> full 3/3, CurrentObservers shows all 3 brokers again
```

Real, captured final state after restoring both remaining controllers:

```text
LeaderId:               1
LeaderEpoch:            21
MaxFollowerLag:         0
CurrentVoters:          [{"id": 1, ...}, {"id": 2, ...}, {"id": 3, ...}]
CurrentObservers:       [{"id": 4, ...}, {"id": 5, ...}, {"id": 6, ...}]
```

`MaxFollowerLag: 0` on every voter is the durable confirmation that
every controller — including the ones that were down — has fully
caught up on every metadata record committed while it was offline, not
merely that the process restarted.

## The metadata log — real, inspected on disk

KRaft's metadata is a Kafka-style log, `__cluster_metadata-0`, stored
and replicated exactly like an ordinary partition — with KRaft-specific
additions. Real, captured directory listing from an actual controller's
data directory:

```text
$ ls /var/lib/kafka/data/__cluster_metadata-0
00000000000000000000.index
00000000000000000000.log
00000000000000000000.timeindex
00000000000000001216.snapshot        <- a real KRaft snapshot file
leader-epoch-checkpoint
partition.metadata
quorum-state
```

`quorum-state` (real, captured content) is this node's own persisted
view of the quorum, read on restart so it doesn't have to relearn who
it voted for or who the leader was purely from the replicated log:

```json
{"clusterId":"","leaderId":1,"leaderEpoch":21,"votedId":-1,"appliedOffset":0,"currentVoters":[{"voterId":1},{"voterId":2},{"voterId":3}],"data_version":0}
```

`meta.properties` (real, captured) shows the per-node and per-log-directory
identity discussed in
[`docs/replication/REPLICATION_ISR_AND_BROKER_FAILURE.md`](../replication/REPLICATION_ISR_AND_BROKER_FAILURE.md)'s
broker-recovery correction — `directory.id` is a real, persisted UUID
distinct from `node.id`:

```text
cluster.id=6Zy-q9GlR3u9dQkH6Aj_4A
directory.id=iD-qLVcind8RQSmUgR8W_g
node.id=1
version=1
```

**Real, decoded metadata records** (`kafka-dump-log.sh --cluster-metadata-decoder`
against the actual log segment) — a genuine, committed record for each
kind of change this document already discussed happening:

```text
payload: {"type":"LeaderChange","leaderId":3,"voters":[...],"grantingVoters":[...]}
payload: {"type":"FEATURE_LEVEL_RECORD", ...}
payload: {"type":"CONFIG_RECORD","data":{"resourceType":4,"name":"min.insync.replicas","value":"1"}}
payload: {"type":"REGISTER_BROKER_RECORD","data":{"brokerId":4, "endPoints":[...], "fenced":true, ...}}
payload: {"type":"REGISTER_CONTROLLER_RECORD","data":{"controllerId":2, "endPoints":[...]}}
payload: {"type":"BROKER_REGISTRATION_CHANGE_RECORD","data":{"brokerId":4,"fenced":-1}}
payload: {"type":"NO_OP_RECORD","data":{}}
```

`NO_OP_RECORD`s appear at a steady ~500ms cadence in this real dump —
the active controller periodically appends one to keep the log (and
therefore the high watermark) advancing even when nothing else is
changing, which is also what lets followers' `MaxFollowerLagTimeMs`
stay meaningfully small during idle periods.

**Committed vs. high watermark, precisely:** the metadata log's high
watermark advances the same way a partition's does — once a majority of
voters have replicated a record, it's committed, and the high watermark
moves past it. A voter's own log can contain records *beyond* the
current high watermark (fetched but not yet known to be majority-replicated);
only the high-watermark-bounded prefix is guaranteed durable.
**Snapshots** (the real `.snapshot` file above) periodically compact the
full history of metadata records into a single point-in-time image, so
a recovering or newly-joining node doesn't have to replay every record
since cluster creation — it loads the latest snapshot, then replays
only the log records after it.

## ZooKeeper vs. KRaft — concise, not a ZooKeeper tutorial

```text
Older Kafka                          Modern Kafka (this repository)

Kafka brokers                        Kafka brokers
     |                                    |
     v                                    v
ZooKeeper (external system)          KRaft metadata quorum
                                      (Kafka's own controller nodes)
```

KRaft moved cluster metadata storage, controller election, and metadata
replication **into Kafka itself** — the same Raft-style consensus
protocol this document has been demonstrating all along, running on
Kafka's own broker/controller processes, instead of delegating that job
to a separate ZooKeeper ensemble with its own separate leader election,
its own separate storage, and its own separate operational surface to
run and monitor. This repository targets Kafka 4.x, where ZooKeeper
support has been removed entirely — every experiment in this document
and lab is real KRaft behavior on the pinned `apache/kafka:4.3.1`
image, not a hybrid or migration scenario.

## Failure matrix

| Scenario | Expected control-plane behavior | Verified here |
|---|---|---|
| 3/3 controllers alive | Normal | Real |
| 2/3 controllers alive | Quorum retained | Real (metadata operations succeeded) |
| Active controller fails | New controller elected | Real (leader failover in ~6s) |
| 1/3 controller alive | Quorum unavailable | Real (`describe --status` timed out) |
| Quorum unavailable | Metadata mutations unavailable | Real (create/alter failed; `--list` notably still worked) |
| Quorum restored | Metadata operations recover | Real (create-topic succeeded again within 15s of majority returning) |
| Broker fails while quorum healthy | Controller can coordinate the required metadata change | Real (WP-07's entire lab; leader election completes normally) |
| Broker fails while quorum unavailable | Partition becomes stuck; no leader election possible until quorum returns | Real (Experiment 11 — sustained failure, no self-recovery, resumed only after quorum restoration) |

Every row in this matrix reflects an experiment this lab actually ran —
none are purely theoretical extrapolations.

## Observability

**Metadata quorum status, active controller, voter membership, leader
epoch, high watermark, lag:** `kafka-metadata-quorum.sh describe --status`
(CLI) or `AdminClient.describeMetadataQuorum()` (Java, this lab's
`DescribeQuorumApp`) — both verified for real in this document, both
version-appropriate for the pinned KRaft-only `4.3.1` build (this
repository does not target the ZooKeeper-era `kafka.controller:type=KafkaController,name=ActiveControllerCount`
JMX metric as a KRaft observability mechanism without qualification —
its KRaft-era availability/equivalent was not independently verified in
this lab's time budget; `describeMetadataQuorum()`/`describe --status`
are the tools this document actually verified).

**Broker/controller connectivity:** this lab's own real evidence —
`NetworkClient` disconnect log lines, `DisconnectException`, and
`TimeoutException` messages — is exactly what an operator would see in
application/client logs during a real quorum problem.

**Controller logs:** each controller container's own stdout (`docker
logs kafka-controller-N`) shows KRaft's own election and replication
log lines directly (`QuorumController`, raft client, `KafkaRaftClient`
log lines) — this lab relies on client-visible symptoms and
`describe --status` rather than a full log-aggregation pipeline, which
is WP-16's dedicated subject.

## Production architecture discussion

```text
3 dedicated controllers
        +
N brokers
```

Dedicated controllers are preferable once a cluster's data-plane load
(broker CPU/memory/network from real partition traffic) is large enough
that it could compete with, delay, or destabilize the control plane's
own latency-sensitive Raft replication — exactly the coupling this
lab's topology was built to avoid experimentally. Separating the two
failure domains means a broker under heavy load, or even fully offline,
can never itself threaten the metadata quorum's ability to elect a
leader and keep committing metadata, and vice versa.

**Controller placement across failure domains:** the same majority-quorum
reasoning in "Controller quorum fundamentals" applies to *where* voters
are physically placed, not just how many there are — 3 controllers each
in a different availability zone tolerates one entire AZ's loss without
losing quorum majority; 3 controllers all in the same AZ does not
protect against that AZ's loss at all, regardless of the voter count.

This document does **not** claim one topology is correct for every
deployment size. A small, cost-sensitive cluster with modest traffic
may reasonably run combined-role nodes (WP-07's own topology, or a
production equivalent) and accept the coupling this document
identified, if the operational simplicity is worth more than the
failure-domain separation at that scale. The Principal Engineer
judgment is not "always dedicate controllers" — it's knowing precisely
which coupling you're accepting or avoiding, and why, for the specific
scale and risk tolerance in front of you.

## Principal Engineer questions

**1. What problem does the KRaft controller solve?**

It is the single authority that commits and orders every change to a
Kafka cluster's metadata — topic creation, partition assignments,
leader elections, broker registration — replicated via Raft consensus
across a small quorum of voter nodes, so the cluster has one consistent,
durable, agreed-upon view of its own metadata even as individual nodes
fail. Before KRaft, this job (and its own separate consensus/storage
problem) belonged to an external ZooKeeper ensemble; KRaft's contribution
is moving that responsibility into Kafka's own protocol and processes.

**2. What is stored in Kafka's metadata log?**

Every committed metadata change, as a typed record — this document's
real, decoded dump showed `REGISTER_BROKER_RECORD`,
`REGISTER_CONTROLLER_RECORD`, `CONFIG_RECORD`, `FEATURE_LEVEL_RECORD`,
`LeaderChange` (control records marking a new controller leader), and
periodic `NO_OP_RECORD`s the leader appends to keep the log advancing.
Periodic snapshots compact the full history so recovery doesn't require
replaying every record since cluster creation.

**3. Why are three controllers commonly used?**

Three is the smallest odd voter count that tolerates any failure at
all (majority = 2, tolerating 1 down) while minimizing the number of
always-on nodes paid for. This document's "Controller quorum
fundamentals" section shows precisely why an even count like 4 buys
nothing over 3 (same 1-failure tolerance, one more node to run) — the
odd-number convention is that arithmetic made into a rule of thumb.

**4. What happens when the active controller dies?**

The remaining voters detect the failure and hold a new leader election
via Raft; a new leader emerges from among the surviving voters (which
one is not predictable in advance) and starts a new, higher leader
epoch. This document's real, timestamped result: election completed
within roughly 6 seconds in this lab's specific configuration and
network — not a number to generalize to production without accounting
for real heartbeat/timeout tuning and real network latency.

**5. Can Kafka operate with two of three controllers?**

Yes — this document's real, captured evidence shows topic creation,
config alteration, and topic deletion all succeeding normally with one
controller down and two remaining, because two of three is still a
majority. "One controller failure" is not, by itself, a control-plane
outage.

**6. What happens with only one of three controllers?**

Quorum majority is lost. This document's real evidence shows even a
read-only quorum-status query timing out, and metadata mutations
(create topic, alter config) failing — though not always with the same
error shape (`DisconnectException`-wrapped timeouts in this document's
captured runs, sometimes pointing at different broker connections
between calls). Notably, `kafka-topics.sh --list` still succeeded,
because it can be answered from a broker's own locally-replicated
metadata without contacting the controller at all — a nuance this
document deliberately did not generalize away.

**7. Can producers continue when controller quorum is unavailable?**

Yes, for **existing, already-led partitions** — this document's real
evidence shows continuous production and consumption continuing
completely uninterrupted for over 30 seconds with zero controller
quorum majority. This does not extend to any operation that requires a
metadata change; see question 10.

**8. Why can existing traffic continue while topic creation fails?**

Because they depend on entirely different things. Producing to and
consuming from an existing partition only requires knowing that
partition's current leader — a fact the client already has cached and
the broker itself continues to serve directly, with no controller
involvement per request. Creating a topic requires committing a *new*
piece of cluster metadata, which by definition must go through the
controller quorum's consensus process. This document's control-plane
vs. data-plane table names this distinction explicitly.

**9. Controller leader vs. partition leader?**

Two different roles over two different things, decided by two different
mechanisms — this document's comparison table lays out all four
"leader/coordinator" roles this repository has now covered
(controller leader, partition leader, consumer-group coordinator,
transaction coordinator) specifically so they are never casually
conflated. A controller leader can be a completely different physical
node than any given partition's leader at the same moment, coordinating
entirely independent concerns.

**10. What happens if a partition leader fails while controller quorum
is unavailable?**

This document's real, central experiment: the partition becomes
permanently unavailable for as long as quorum stays down, with no
self-recovery, because electing a *new* leader for that partition is
itself a metadata mutation requiring the controller. This is the
concrete boundary of question 7's answer — "existing traffic
continues" is true only until something *new* needs the controller to
act, and a leader failure is exactly that.

**11. Why separate controllers and brokers in larger production
clusters?**

To make the coupling this document's "why this lab's topology is not
WP-07's" section identified impossible by construction: a broker's data-
plane load can never threaten the controller quorum's ability to elect
a leader and commit metadata, and controller-quorum activity can never
compete with brokers for data-plane resources. This is a real
architectural trade (more nodes to run and reason about) made
deliberately, not a default to apply at every scale — see "Production
architecture discussion" above.

**12. How would you diagnose a KRaft quorum problem in production?**

Start with `kafka-metadata-quorum.sh describe --status` (or
`AdminClient.describeMetadataQuorum()`) — this document's own primary
diagnostic tool throughout — to check `CurrentVoters` count and
`LeaderId` directly. If that call itself times out, quorum majority is
likely already lost (this document's own real result). Cross-check
whether ordinary produce/consume traffic is still flowing (if so, the
data plane is intact and the problem is isolated to the control plane,
per this document's own distinction) versus whether specific partitions
are stuck with no leader (this document's Experiment 11 signature —
check whether that coincides with a *recent* broker failure during
degraded quorum). Check controller container/process logs directly for
election and replication activity, and confirm network reachability
between controller nodes specifically, since a quorum problem is most
often a controller-to-controller connectivity issue, not a broker-side
one.
