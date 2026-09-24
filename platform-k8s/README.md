# Kubernetes (k3d) environments

Every `platform/*/docker-compose.yml` environment in this repository has a
Kubernetes equivalent here, under `platform-k8s/*/`. **Docker Compose stays
the primary, documented path for every lab** — these are an additional,
optional way to run the exact same infrastructure on Kubernetes, for the
same reason most real Kafka deployments eventually run there: `platform/`
is unchanged, and every lab's README still documents the Compose workflow
first.

## Why this exists

Plain Kubernetes manifests (Deployment/StatefulSet + Service + ConfigMap),
hand-written — no operator, no Helm chart. This mirrors the same
"understand the primitives first" approach this repository already uses
for native Java before Spring Kafka: you see exactly what each broker's
`KAFKA_*` configuration is, translated line by line from the Compose file
it mirrors, not hidden behind a CRD. Strimzi (the real, dominant
production pattern for running Kafka on Kubernetes) is covered
conceptually in
[`docs/multi-cluster/KAFKA_MULTI_CLUSTER_AND_DR.md`](../docs/multi-cluster/KAFKA_MULTI_CLUSTER_AND_DR.md#8-kafka-on-kubernetes--strimzi-conceptual-only-by-pre-existing-scope-decision)
— these manifests are not a substitute for that discussion, they're the
"what does Kafka's own configuration look like on this platform" half of
it.

## Prerequisites

- [k3d](https://k3d.io/) (a real k3s-in-Docker Kubernetes distribution)
- `kubectl`
- Docker (the same Docker/Rancher Desktop every lab's docker-compose usage
  already needs)

## One-time setup

```bash
platform-k8s/bootstrap-cluster.sh
```

Creates a single shared k3d cluster, `kafka-lab`, with every environment's
host port pre-mapped (see that script's own comments for the full port
table). Every environment below deploys into its own Kubernetes namespace
on this one cluster — exactly like every `platform/*/docker-compose.yml`
is its own isolated Compose project on the shared Docker daemon. Run this
once; it's a no-op if the cluster already exists.

This writes a **dedicated kubeconfig file**
(`~/.kube/config-kafka-lab.yaml`), never your default `~/.kube/config` —
every script here sources `_lib/common.sh`, which exports `KUBECONFIG` to
that dedicated file automatically. If you want to run `kubectl` commands
by hand outside these scripts:

```bash
export KUBECONFIG=~/.kube/config-kafka-lab.yaml
```

## Environments and which labs use them

| Environment | Mirrors | Used by |
|---|---|---|
| `kafka/` | `platform/kafka/` | lab-01 through lab-05 |
| `kafka-cluster/` | `platform/kafka-cluster/` | lab-06, lab-08 through lab-16, lab-18 (production-simulation) |
| `kraft-quorum/` | `platform/kraft-quorum/` | lab-07 |
| `schema-registry/` | `platform/schema-registry/` | lab-09 (needs `kafka-cluster/` applied first) |
| `kafka-connect/` | `platform/kafka-connect/` | lab-10, lab-11, lab-12 (needs `kafka-cluster/` applied first) |
| `observability/` | `platform/observability/` | lab-15 (needs `kafka-cluster/` applied first) |
| `kafka-security/` | `platform/kafka-security/` | lab-17 (standalone) |
| `multi-cluster-dr/` | lab-19's own `TwoClusterEnvironment` (no docker-compose counterpart) | lab-19 (standalone) |

Each environment directory has:

- `manifests.yaml` — every Kubernetes object for that environment
- `setup.sh` — `kubectl apply` + wait for real readiness (the k8s
  equivalent of `docker compose up -d`)
- `cleanup.sh` — delete the Deployments/Services, keeping data
  (`docker compose down`); `cleanup.sh --wipe` deletes the namespace
  entirely, including PersistentVolumeClaims (`docker compose down -v`)

## Usage

```bash
platform-k8s/kafka-cluster/setup.sh
# ... work through the lab, using kubectl exec instead of docker exec ...
platform-k8s/kafka-cluster/cleanup.sh
```

Every lab's own README has a "Kubernetes (k3d)" subsection under Setup
pointing at the right environment, plus the two real, load-bearing
gotchas below.

## Two real findings that apply to every environment here

**1. Use the INTERNAL listener, not the host-facing one, when running a
CLI tool via `kubectl exec`.** Every broker here has two client-facing
listeners, exactly like `platform/kafka-cluster/`'s own "why three
different host ports" reasoning: `PLAINTEXT_HOST` (advertised as
`localhost:<NodePort>`, for clients on your actual machine) and
`PLAINTEXT` (advertised as the broker's in-cluster FQDN, for clients
inside the same Kubernetes cluster). A `kubectl exec`'d CLI command is an
in-cluster client — connecting it to `localhost:<NodePort>` fails the
moment it needs a second, metadata-directed connection (confirmed the
hard way while validating `kafka-security` and `multi-cluster-dr`), for
the exact reason `platform/kafka/README.md`'s "CLI cannot connect"
section already explains for Docker. Use the broker's own internal port
(e.g. `localhost:19092` from inside a `kafka-cluster` broker pod, or the
broker's short/FQDN hostname from a different pod in the same
environment) instead.

**2. Git Bash on Windows mangles container-side paths passed to `kubectl
exec`.** `kubectl exec ... -- /opt/kafka/bin/kafka-topics.sh` gets
silently rewritten into a bogus `C:/Program Files/Git/opt/kafka/...`
path by MSYS's path-conversion layer — the exact same class of issue
`platform/kafka-security/certs/generate-certs.sh`'s own comments already
document for `docker run -v`. Prefix the container-side path with an
extra leading slash (`kubectl exec ... -- //opt/kafka/bin/kafka-topics.sh`)
to stop MSYS from touching it, without needing `MSYS_NO_PATHCONV=1`
(which also breaks `KUBECONFIG` path resolution — confirmed the hard way).
