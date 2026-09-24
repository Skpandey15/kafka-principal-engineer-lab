# Kafbat UI

A visual browser for the WP-07 3-broker cluster (topics, partitions,
consumer groups and their lag, brokers, and — if `platform/schema-registry/`
is also running — registered schemas). Optional, secondary tooling: use it
*after* the CLI-based labs it sits alongside, never as a substitute for
understanding raw Kafka state (topics, offsets, consumer groups) by hand
first. See
[`docs/references/REFERENCE_REPOSITORIES.md`](../../docs/references/REFERENCE_REPOSITORIES.md#kafbatkafka-ui)
for this repository's own reasoning on tooling.

## Why `kafbat/kafka-ui`, not `provectus/kafka-ui`

Several older tutorials and blog posts still reference
`provectuslabs/kafka-ui`. Provectus paused active development in September
2023, and the project went without meaningful maintenance from that point —
including an unpatched remote-code-execution vulnerability
(CVE-2023-52251) for roughly six months. The original maintainers' work
continues as an active community fork, `kafbat/kafka-ui`, which is what
this repository uses instead. Treat any reference to
`provectuslabs/kafka-ui` (or the bare `provectus/kafka-ui` image) as
pointing at abandoned, security-relevant software.

Also distinct from **Kafka Manager** (Yahoo's original tool, later renamed
CMAK) — that project is ZooKeeper-only and has no place in a KRaft-only
curriculum (see the roadmap's non-goals).

## Prerequisite

`platform/kafka-cluster/` (WP-07) must already be running. This file
attaches to that project's `kafka-cluster_default` network as an external
network — the same pattern `platform/schema-registry/`,
`platform/kafka-connect/`, and `platform/observability/` already
established. `platform/schema-registry/` is optional — Kafbat UI still
works for topics/consumer-groups/brokers without it; it just can't show
registered schemas.

## Setup

```bash
cd platform/kafka-cluster && docker compose up -d   # if not already running
cd ../kafka-ui && docker compose up -d
```

Open [http://localhost:8080](http://localhost:8080) — no login, no auth in
front of it (see "Production considerations" below).

### Kubernetes (k3d) alternative

```bash
platform-k8s/bootstrap-cluster.sh   # once (or re-run if you added this after an earlier bootstrap -- see that script's port table)
platform-k8s/kafka-cluster/setup.sh
platform-k8s/kafka-ui/setup.sh
```

Same host port (`localhost:8080`) as Docker Compose. See
[`platform-k8s/README.md`](../../platform-k8s/README.md) for the general
k3d gotchas. Cleanup: `platform-k8s/kafka-ui/cleanup.sh`.

## Verification

- [ ] The UI loads at `http://localhost:8080` and lists the 3-broker
      cluster under its configured name.
- [ ] Every topic you've created in any other lab reusing
      `platform/kafka-cluster/` is visible, with real partition/replica/ISR
      state matching what `kafka-topics.sh --describe` already showed you.
- [ ] A consumer group you've run against this cluster shows real,
      matching lag — cross-check it against `kafka-consumer-groups.sh
      --describe` (the same "two independently-computed views of the same
      broker-side state" cross-check WP-16's `LagInspector` already
      demonstrates for Prometheus).
- [ ] If `platform/schema-registry/` is running, registered subjects from
      `lab-09` are visible under the Schema Registry tab.

## Cleanup

```bash
cd platform/kafka-ui && docker compose down
```

This does not affect `platform/kafka-cluster/`, `platform/schema-registry/`,
or any other environment — same isolation every other `platform/*`
add-on already has.

## Production considerations

| This environment | Production consideration |
|---|---|
| No authentication at all | Kafbat UI supports OAuth2 (GitHub/GitLab/Google), LDAP, and basic auth, plus role-based access control — none configured here, appropriate only for this local, single-user learning environment |
| `DYNAMIC_CONFIG_ENABLED=true` (cluster config editable from the UI) | A real deployment would likely disable this and manage cluster connections as code, for the same change-control reasons this repository never lets a lab's own Java code create topics silently |
| One statically-configured cluster | Kafbat UI supports monitoring multiple clusters from one instance — genuinely useful at platform scale, not built here since this repository's labs only ever run one cluster at a time |
| No TLS/SASL awareness configured | `lab-17-security`'s SASL_SSL broker is a SEPARATE environment (`platform/kafka-security/`) this UI is not currently pointed at — connecting a UI to a secured cluster needs its own `KAFKA_CLUSTERS_0_*` SASL/SSL properties, not built here |
