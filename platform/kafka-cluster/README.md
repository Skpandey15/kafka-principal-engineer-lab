# Multi-Broker KRaft Cluster (WP-07)

A real, 3-node KRaft Kafka cluster for
[`labs/lab-06-replication-isr-broker-failure`](../../labs/lab-06-replication-isr-broker-failure/README.md).
This is a **separate environment** from [`platform/kafka/`](../kafka/README.md)
(the WP-02 single-node cluster every earlier lab uses) — different
container names, different Docker network, different named volumes,
different host ports. Both can run at the same time without conflict,
and nothing here modifies or requires stopping the WP-02 environment.

## Topology

```text
kafka-broker-1  (node.id=1, broker+controller)  localhost:9093
kafka-broker-2  (node.id=2, broker+controller)  localhost:9094
kafka-broker-3  (node.id=3, broker+controller)  localhost:9095
```

All three nodes combine the broker and controller roles, exactly like
the WP-02 single-node setup does — this environment is built for
**data-plane replication and broker failure** (WP-07), not
controller-quorum failure (WP-08's dedicated subject). Combining roles
on all three nodes does have one real consequence worth knowing before
you experiment: it couples controller-quorum majority (needs 2 of 3
nodes) with anything requiring 2 of 3 *brokers* down — see the lab
README's Experiment 8 for a real, discovered example of this coupling,
and why a production cluster typically separates the two roles instead.

## Why three different host ports

Each broker's `PLAINTEXT_HOST` advertised listener must tell a client
outside Docker exactly the address that client actually reached it on.
A client on your machine connecting to `localhost:9093` must be told
back `localhost:9093` — not `localhost:9094` or `localhost:9095`, which
are the *other* brokers' own ports. Get this wrong and a client that
successfully bootstraps against one broker will fail the moment it tries
to connect to another broker using the wrong advertised address (a
`Connection to node ... could not be established` — see
Troubleshooting).

For traffic *between* the three containers (replication, controller
quorum), a second, internal `PLAINTEXT` listener is advertised using
each container's own Docker network hostname (`kafka-broker-1:19092`,
etc.) on a consistent internal port — this is what `docker exec ...
kafka-topics.sh --bootstrap-server kafka-broker-1:19092 ...` uses, and
it is **not** reachable from your host machine directly (only from
another container on the same Docker network).

## Startup

```bash
docker compose -f platform/kafka-cluster/docker-compose.yml up -d
```

Wait for all three to report healthy:

```bash
docker compose -f platform/kafka-cluster/docker-compose.yml ps
```

Verify all three brokers actually registered (not just that the
containers are running):

```bash
docker exec kafka-broker-1 /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092
```

Real, captured output:

```text
kafka-broker-3:19092 (id: 3 rack: null isFenced: false) -> (
kafka-broker-1:19092 (id: 1 rack: null isFenced: false) -> (
kafka-broker-2:19092 (id: 2 rack: null isFenced: false) -> (
```

And the controller quorum itself:

```bash
docker exec kafka-broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-broker-1:19092 describe --status
```

Real, captured output:

```text
ClusterId:              K2cByfqsRAO_ktpfonxKLw
LeaderId:               3
CurrentVoters:          [{"id": 1, ...}, {"id": 2, ...}, {"id": 3, ...}]
CurrentObservers:       []
```

**Important — CLI commands run via `docker exec` must use the internal
`kafka-broker-N:19092` listener, never `localhost:909X`.** Inside a
container's own network namespace, `localhost` means that container
itself; the `localhost:909X` addresses only resolve correctly from a
process genuinely running on your host machine (which is exactly what
this lab's Java apps are). Using `localhost:909X` from inside a
`docker exec` shell produces confusing connection failures once
`AdminClient` tries to reach a *different* broker's advertised address —
discovered for real while building this lab; see the lab README's
Troubleshooting section.

## Shutdown

```bash
docker compose -f platform/kafka-cluster/docker-compose.yml down        # keeps data
docker compose -f platform/kafka-cluster/docker-compose.yml down -v     # deletes everything
```

## Internal-topic replication defaults

Unlike the WP-02 single-node environment (forced to RF=1 for its
internal topics, since only one broker exists), this cluster has three
real brokers, so `__consumer_offsets` and the transaction/share
coordinator state topics use `replication.factor=3`,
`min.insync.replicas=2` — realistic multi-broker defaults, not a lab
simplification.

## Cluster ID

`K2cByfqsRAO_ktpfonxKLw` — generated for real against the pinned
`apache/kafka:4.3.1` image (`kafka-storage.sh random-uuid`), not copied
from a tutorial. Not a secret, but not meaningful to reuse outside this
lab's own three containers, which is why every node's `CLUSTER_ID`
value here must match exactly (it is what makes these three processes
one cluster instead of three unrelated single-node ones).

## Observability (WP-16)

Each broker now also runs a real Prometheus JMX exporter Java agent
(`jmx-exporter/`, `KAFKA_OPTS`), exposing broker-side metrics on
7071/7072/7073 for `platform/observability/`'s Prometheus to scrape.
Run `jmx-exporter/fetch-jmx-exporter.sh` once before `docker compose up`.
Purely additive — no listener, node ID, or quorum setting above changed.
See [`labs/lab-15-observability/README.md`](../../labs/lab-15-observability/README.md)
and [`docs/observability/KAFKA_OBSERVABILITY.md`](../../docs/observability/KAFKA_OBSERVABILITY.md)
for the full depth, including a real finding about why the healthcheck
below needed `KAFKA_OPTS=` added to its own command.
