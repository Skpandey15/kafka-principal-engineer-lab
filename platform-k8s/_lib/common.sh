#!/usr/bin/env bash
# Shared helpers for every platform-k8s/*/setup.sh and cleanup.sh script.
# Source this, don't execute it: `source "$(dirname "$0")/../_lib/common.sh"`
#
# Every environment under platform-k8s/ deploys into its own Kubernetes
# namespace on ONE shared k3d cluster (see platform-k8s/bootstrap-cluster.sh)
# -- mirroring how every platform/*/docker-compose.yml is its own isolated
# Docker Compose project, just sharing one k3d node instead of the Docker
# daemon. Namespace isolation is what lets `cleanup.sh` for one lab's
# environment never touch another's, exactly like `docker compose down`
# scoped to one compose file already doesn't touch a different one.

set -euo pipefail

KAFKA_LAB_CLUSTER_NAME="kafka-lab"

# A DEDICATED kubeconfig file, not the shared ~/.kube/config -- this
# repository's scripts never touch a user's default kubeconfig (on at least
# one real Windows/Rancher Desktop setup, ~/.kube/config is a reparse point
# into Rancher Desktop's own config directory, and merging into it turned
# out to be fragile there). Every script in platform-k8s/ sources this file
# and gets KUBECONFIG exported to this dedicated path instead.
export KUBECONFIG="${HOME}/.kube/config-kafka-lab.yaml"

# Fails fast, with a clear message, if the shared cluster doesn't exist yet --
# every environment's setup.sh depends on it, but none of them should
# silently create it themselves (that's bootstrap-cluster.sh's one job,
# since the full port-mapping list has to be declared at cluster-creation
# time in k3d, not added later).
require_cluster() {
  if ! k3d cluster list "${KAFKA_LAB_CLUSTER_NAME}" >/dev/null 2>&1; then
    echo "The shared '${KAFKA_LAB_CLUSTER_NAME}' k3d cluster doesn't exist yet." >&2
    echo "Run platform-k8s/bootstrap-cluster.sh once first (see platform-k8s/README.md)." >&2
    exit 1
  fi
  if [[ ! -f "${KUBECONFIG}" ]]; then
    k3d kubeconfig write "${KAFKA_LAB_CLUSTER_NAME}" -o "${KUBECONFIG}" >/dev/null
  fi
}

# Waits for every pod matching a label selector, in a namespace, to report
# Ready -- the same "don't proceed until actually usable" discipline this
# repository's docker-compose healthchecks already apply, not just "the pod
# object exists."
wait_for_pods_ready() {
  local namespace="$1"
  local selector="$2"
  local timeout="${3:-180s}"
  echo "Waiting for pods matching '${selector}' in namespace '${namespace}' to be ready (timeout ${timeout})..."
  # `kubectl apply` returning doesn't guarantee the Deployment's pod object
  # exists yet -- `kubectl wait` fails immediately with "no matching
  # resources found" if run before it does, rather than waiting for it to
  # appear. Poll briefly for the pod to exist first, then do the real wait.
  local i=0
  while [[ -z "$(kubectl -n "${namespace}" get pods -l "${selector}" -o name 2>/dev/null)" ]]; do
    i=$((i + 1))
    if [[ $i -gt 30 ]]; then
      echo "No pod matching '${selector}' ever appeared in namespace '${namespace}'." >&2
      exit 1
    fi
    sleep 1
  done
  kubectl -n "${namespace}" wait --for=condition=Ready pod -l "${selector}" --timeout="${timeout}"
}

# Idempotent namespace creation -- `kubectl apply` on a Namespace object
# already is idempotent, but this gives every setup.sh a single, readable
# call instead of repeating the apply/describe dance.
ensure_namespace() {
  local namespace="$1"
  kubectl create namespace "${namespace}" --dry-run=client -o yaml | kubectl apply -f -
}
