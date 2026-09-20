# Dedicated Controller/Broker KRaft Cluster (WP-08)

A real KRaft cluster for
[`labs/lab-07-kraft-controller-quorum-failure`](../../labs/lab-07-kraft-controller-quorum-failure/README.md)
with the **control plane and data plane on separate nodes** — 3
controller-only voters plus 3 broker-only nodes.

## Why not just reuse WP-07's combined-role topology?

WP-07's cluster (`platform/kafka-cluster/`) combines the broker and
controller roles on all 3 of its nodes — a deliberate simplicity choice
that lab made explicit at the time (see its README). That choice has a
real, discovered consequence that this WP exists specifically to
isolate away: in a combined-role cluster, killing enough nodes to lose
controller-quorum majority *also* kills enough brokers to lose data
availability, so the two failure domains are impossible to separate
experimentally — you can never observe "the control plane is down but
the data plane is fine" in that topology, because both fail together.

Dedicated controller-only nodes fix this by construction: a controller
process holds no partition replica, and a broker process holds no vote
in the metadata quorum. Killing all 3 controllers cannot, by itself,
take a single partition offline; killing all 3 brokers cannot, by
itself, break the metadata quorum. This is also what real production
Kafka deployments at meaningful scale actually do — see the lab README
and conceptual doc for why.

## Topology

```text
kafka-controller-1   (node.id=1, controller only, no client-facing port)
kafka-controller-2   (node.id=2, controller only, no client-facing port)
kafka-controller-3   (node.id=3, controller only, no client-facing port)

kraft-quorum-broker-1   (node.id=4, broker only)   localhost:9096
kraft-quorum-broker-2   (node.id=5, broker only)   localhost:9097
kraft-quorum-broker-3   (node.id=6, broker only)   localhost:9098
```

Node IDs are unique across the **whole** cluster — KRaft shares one ID
namespace between controllers and brokers, unlike (for example)
`group.id`, which is scoped per-purpose.

This is a **separate** environment from `platform/kafka/` (WP-02,
single combined node) and `platform/kafka-cluster/` (WP-07, three
combined broker+controller nodes) — different container names,
network, volumes, and host ports (9096-9098 here). All three can run
simultaneously. The broker containers are named
`kraft-quorum-broker-N`, not `kafka-broker-N`, specifically so this
environment never collides with WP-07's containers of nearly the same
purpose.

## Why controllers have no host port

Nothing outside the Docker network ever needs to reach a controller's
`CONTROLLER` listener directly — not a real client, and not this lab's
own tooling. `AdminClient.describeMetadataQuorum()` and
`kafka-metadata-quorum.sh --bootstrap-server <broker>` both work by
bootstrapping through an ordinary **broker** connection; the broker
forwards the request to the controller quorum's current leader
transparently. (`kafka-metadata-quorum.sh` also supports
`--bootstrap-controller <controller>:<port>` for talking to a
controller directly — real, verified against this pinned
`apache/kafka:4.3.1` build — but this lab's tooling never needs it
because the broker-forwarding path already works and matches how a
real client would interact with the cluster.) Never publishing a
controller's listener to the host is also simply correct production
practice: nothing outside the cluster's own control plane should ever
be able to open a connection to it.

## Startup

```bash
docker compose -f platform/kraft-quorum/docker-compose.yml up -d
```

Wait for the brokers to report healthy:

```bash
docker compose -f platform/kraft-quorum/docker-compose.yml ps
```

Verify the quorum formed and the brokers registered as **observers**
(not voters) — the direct, real proof that broker and controller roles
are actually separated in this topology:

```bash
docker exec kraft-quorum-broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kraft-quorum-broker-1:19092 describe --status
```

Real, captured output:

```text
ClusterId:              6Zy-q9GlR3u9dQkH6Aj_4A
LeaderId:               3
LeaderEpoch:            1
CurrentVoters:          [{"id": 1, ...}, {"id": 2, ...}, {"id": 3, ...}]
CurrentObservers:       [{"id": 4, ...}, {"id": 5, ...}, {"id": 6, ...}]
```

Contrast this directly with WP-07's `platform/kafka-cluster/` — the
same command against that cluster shows an **empty** `CurrentObservers`
list, because every node there is also a voter. This lab's
`CurrentObservers: [4, 5, 6]` (the 3 brokers) is the concrete,
observable meaning of "the brokers replicate the metadata log but
cannot vote in it."

## Shutdown

```bash
docker compose -f platform/kraft-quorum/docker-compose.yml down        # keeps data
docker compose -f platform/kraft-quorum/docker-compose.yml down -v     # deletes everything
```

## CLI commands must use the internal listener, not `localhost`

Same rule as WP-07's environment, for the same reason: from inside a
`docker exec` shell, `localhost` means that container itself, not the
Docker host. Always bootstrap CLI tools via a broker's **internal**
listener (`kraft-quorum-broker-N:19092`) when running them via
`docker exec`; use `localhost:9096`/`9097`/`9098` only from a process
genuinely running on your host machine (this lab's Java apps).

## Cluster ID

`6Zy-q9GlR3u9dQkH6Aj_4A` — generated for real via `kafka-storage.sh
random-uuid` against the pinned `apache/kafka:4.3.1` image, distinct
from both `platform/kafka/`'s and `platform/kafka-cluster/`'s cluster
IDs. Shared by every node (controllers and brokers alike) in this
cluster, because that shared value is what makes them one cluster.
