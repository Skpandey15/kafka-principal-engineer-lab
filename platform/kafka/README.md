# Local Kafka Platform (KRaft, single node)

This directory defines the local Kafka environment used by
[`labs/lab-01-first-kafka-cluster`](../../labs/lab-01-first-kafka-cluster/README.md)
and, unless a later work package introduces a different topology for a
specific experiment, by the labs that follow it. It runs one Kafka node, in
KRaft mode, with no ZooKeeper.

**This is a local learning environment, not a production reference
architecture.** Every simplification below is called out explicitly, with
what production would do differently and why. See
["Why this is not production architecture"](#why-this-is-not-production-architecture)
at the end of this document.

## Selected Kafka version

**`apache/kafka:4.3.1`**, pinned explicitly — never `latest`.

Kafka 4.0 (released March 2025) removed ZooKeeper support entirely; every
4.x release, including 4.3.1, is KRaft-only. That makes the whole 4.x line a
natural fit for a repository whose stated position is "modern Kafka first,"
and it means we are not choosing KRaft over ZooKeeper as a lab preference —
current Kafka does not offer ZooKeeper as an alternative any more.

4.3.1 was the newest stable release of Apache Kafka, and of the official
`apache/kafka` Docker image, at the time this lab was written. Version
numbers in this repository are pins, not endorsements of permanence — before
bumping this pin, check
[the Apache Kafka release announcements](https://kafka.apache.org/blog/releases/)
and the [`apache/kafka` image tags on Docker Hub](https://hub.docker.com/r/apache/kafka/tags),
and re-verify the environment variables below against the current
[Kafka Docker image usage guide](https://github.com/apache/kafka/blob/trunk/docker/examples/README.md)
in `apache/kafka`, since KRaft's configuration surface has changed between
minor versions before.

## Image choice

We use the **official Apache Kafka image, `apache/kafka`**, not a
third-party distribution. Reasons:

- It is published by the Apache Kafka project itself, so its behavior is the
  reference behavior this repository is trying to teach — not a vendor's
  interpretation of it.
- It supports KRaft mode configuration entirely through environment
  variables (see below), which keeps the Docker Compose file legible without
  a mounted properties file.
- It requires no ZooKeeper image or service at all, consistent with this
  repository's "modern Kafka first" position (see
  [`docs/roadmap/KAFKA_ZERO_TO_PRINCIPAL_ENGINEER.md`](../../docs/roadmap/KAFKA_ZERO_TO_PRINCIPAL_ENGINEER.md)).

There is also an `apache/kafka-native` image (a GraalVM native-image build,
optimized for faster startup and lower memory). It is not used here because
the standard JVM-based image is the one whose behavior and logs match what
you will see running Kafka anywhere else — a learning environment should not
introduce a second variable.

## KRaft mode, broker role, controller role, combined mode

KRaft (Kafka Raft) is Kafka's own metadata-consensus protocol. Instead of a
separate ZooKeeper ensemble holding cluster metadata (which brokers exist,
which partitions they lead, ACLs, and so on), a subset of Kafka nodes runs a
Raft-based **controller quorum** that replicates that metadata as a log,
the same durable-log abstraction Kafka already uses for topic data. A deeper
treatment of *why* Kafka moved to this design belongs to `docs/kraft/` (a
later work package covering multi-node controller quorums and controller
failure); here, the goal is only enough to understand what this one node is
doing.

Every KRaft node declares a `process.roles`:

- **`broker`** — serves produce/fetch requests, hosts partition data.
- **`controller`** — participates in the metadata Raft quorum: proposes and
  commits changes to cluster metadata (topic creation, partition leadership,
  broker registration, and so on).
- **`broker,controller`** — both at once, in a single JVM process. This is
  **combined mode**.

This environment sets `KAFKA_PROCESS_ROLES=broker,controller` — combined
mode on one node. That is what makes a single container a legitimate,
complete Kafka cluster with no ZooKeeper and no second node: the same
process is both the thing storing your topic's records and the thing
deciding "this node is the leader of `orders`-partition-0." Combined mode is
appropriate for local development and this curriculum's early labs
specifically *because* it minimizes moving parts while you build the mental
model; WP-07 introduces a multi-node controller quorum and controller
failure, where the broker/controller split starts to matter operationally.

## Listeners and advertised listeners

These are two different concepts that are easy to conflate, and doing so is
the single most common cause of "the container is healthy but nothing can
talk to it" — see
[Troubleshooting → CLI cannot connect](../../labs/lab-01-first-kafka-cluster/README.md#cli-cannot-connect)
in the lab.

- **`listeners`** — the addresses this Kafka process binds to and accepts
  TCP connections on, from *inside its own network namespace* (the
  container). Ours: `CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092`.
- **`advertised.listeners`** — the addresses this process tells *clients* to
  use when connecting. This can legitimately differ from `listeners`,
  because a client outside Docker and a client inside the same Docker
  network need different addresses to reach the same container. Ours:
  `PLAINTEXT_HOST://localhost:9092,PLAINTEXT://kafka:19092`.

Concretely: your host machine's Kafka CLI (or, in later labs, a Java
application running directly on your machine) connects via
`localhost:9092`, because that is the port Docker Compose publishes to the
host. A second container joining this Docker network in a later lab (a UI,
another service) would instead use `kafka:19092`, because `localhost` inside
that other container would mean *that container*, not this one. Neither
listener is exposed to something that shouldn't use it: the `CONTROLLER`
listener (`29093`) is never advertised to clients at all — it exists purely
for controller-quorum (Raft) traffic, and Kafka refuses client connections on
it by design via `controller.listener.names`.

## Controller listener and controller quorum voters

`KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER` tells the node which listener
carries controller-quorum traffic. `KAFKA_CONTROLLER_QUORUM_VOTERS` lists
every voting member of that quorum as `nodeId@host:port` — here, just
`1@kafka:29093`, because there is exactly one node and it is both the only
broker and the only controller voter.

A one-voter quorum has **zero fault tolerance for the control plane**: if
this node's controller role goes down, so does the cluster's ability to
agree on any new metadata — this is one of the specific things a single-node
lab cannot teach you about production Kafka, and it is why the Production
Contrast table in the lab explicitly separates "cluster survives *data-plane*
node loss" from "cluster survives *control-plane* node loss." Real quorum
tolerance (`2N+1` voters, majority agreement) is the subject of WP-07.

## Cluster ID

KRaft clusters are identified by a `cluster.id` written into the on-disk
storage of every node in the cluster the first time it starts. `CLUSTER_ID`
here is set to `4L6g3nShT-eMCtK--X86sw` — the same fixed, non-secret example
ID used in Apache Kafka's own official Docker Compose examples
(`docker/examples/docker-compose-files` in the `apache/kafka` repository).
Reusing it is a deliberate choice for reproducibility: your output should
match what you'd see following upstream's own examples. It is **not** a
credential and carries no security meaning — for anything beyond a local,
disposable learning cluster, generate a fresh one with
`kafka-storage.sh random-uuid` instead of copying this value.

The entrypoint script formats the node's storage directory with this cluster
ID the first time it finds no existing formatted storage there — you can see
this in the container's startup logs as `Using provided cluster id
4L6g3nShT-eMCtK--X86sw ...`. Once storage is formatted, that ID (and
everything else in the log) persists until the storage is deleted, which is
exactly what Experiments 9 and 10 in the lab demonstrate.

## Storage and the data volume

`KAFKA_LOG_DIRS=/var/lib/kafka/data`, backed by the named volume
`kafka-data` (declared at the bottom of `docker-compose.yml`, so it has a
stable, inspectable name rather than an anonymous one Compose would
otherwise generate). This single directory holds **both**:

- Every topic-partition's log segments (the actual record data).
- The KRaft metadata log itself (`metadata.log.dir` is not set separately,
  so it defaults to the first entry of `log.dirs`).

That means one volume, deleted or preserved, determines the fate of both
your data *and* your cluster's own metadata history — which is precisely
the point Experiments 9 and 10 are built to make concrete: the container can
be destroyed and recreated freely as long as the volume survives, but
deleting the volume is indistinguishable, from Kafka's point of view, from
creating a brand-new cluster that happens to reuse the same `CLUSTER_ID`.

The default path baked into the image is `/tmp/kraft-combined-logs`; we
override it because a path under `/tmp` invites confusion about whether data
loss came from Kafka's storage lifecycle or from something in the container
runtime clearing `/tmp`. Using an explicit path under `/var/lib/kafka`,
mapped to a named volume we control, keeps the storage lifecycle entirely in
your hands.

## Container networking and host access

Docker Compose creates a bridge network for this project (`kafka_default`)
even though there is currently only one service on it — this is Compose's
normal behavior, and it is what lets a later lab add a second container
(for example, a cluster UI) that can already reach this broker at
`kafka:19092` without touching this file's listener configuration. Only port
`9092` (the `PLAINTEXT_HOST` listener) is published to the host, in
`ports:`; the controller port (`29093`) and the internal listener (`19092`)
are reachable only from inside that Docker network, which is correct — a
host process has no legitimate reason to speak the controller protocol or
use the container-internal advertised name.

## Why this configuration is appropriate locally

- **Zero external dependencies beyond Docker.** No ZooKeeper, no separate
  controller nodes, no shared infrastructure — `docker compose up` on a
  laptop is enough.
- **Deterministic and disposable.** A fixed image tag, a fixed cluster ID,
  and an explicit named volume mean the same commands produce the same
  observable behavior every time, and "start over" is one documented command
  away (see the lab's Cleanup section).
- **Small enough to reason about completely.** One process, one log
  directory, three listeners you can name the purpose of individually — the
  goal of WP-02 is to fully explain everything running, not to hide
  complexity behind defaults.

## Why this is not production architecture

| This local lab | Production consideration |
|---|---|
| One node, combined broker + controller | Multiple brokers; a controller quorum is commonly run on dedicated nodes once cluster size or blast-radius requirements justify separating the two roles |
| One controller voter (no control-plane fault tolerance) | An odd-sized quorum (typically 3 or 5 voters) so the cluster keeps agreeing on metadata after losing a minority of controllers |
| `PLAINTEXT` listeners, no authentication | TLS and/or SASL, plus ACLs, chosen based on the organization's security requirements (`docs/security/`, a later WP) |
| Replication factor 1 everywhere (forced by having one broker) | A replication factor chosen deliberately per the durability the data needs — see `docs/replication/`, a later WP |
| A single Docker named volume on one machine | Storage sized, monitored, and placed according to the failure domains the platform needs to survive |
| No monitoring | Broker, controller, and client-side metrics exported and alerted on (`docs/observability/`, a later WP) |
| Manual `docker compose` commands | Provisioned and changed through the organization's standard infrastructure automation |

None of the production-side entries above are universal defaults to copy —
"3 controller voters" and "replication factor 3" are common starting points,
not laws, and the right answer always depends on the workload, the SLOs, and
what failure the system must survive. That reasoning is exactly what the
later work packages (replication, KRaft quorum failure, capacity planning,
the Principal Engineer decision framework) exist to build.

## Commands

From this directory (`platform/kafka/`), using modern `docker compose`
(the `docker-compose` standalone binary is not used anywhere in this
repository):

```bash
# Start (or restart, if already created) the environment in the background.
docker compose up -d

# Check container status and the healthcheck's current state.
docker compose ps

# Follow the broker's logs.
docker compose logs -f

# Safe stop: containers are removed, the named volume (your data) is kept.
docker compose down

# Destructive reset: also deletes the named volume. Irreversible.
docker compose down -v
```

These commands are identical whether your terminal is Bash or PowerShell —
`docker compose` is a single executable with no shell-specific syntax here.
Where a lab step *does* differ between Bash and PowerShell (mainly piping
input into `kafka-console-producer.sh`), the lab README calls it out
explicitly with both forms.

See [`labs/lab-01-first-kafka-cluster/README.md`](../../labs/lab-01-first-kafka-cluster/README.md)
for the full walkthrough that uses this environment.
