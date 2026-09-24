#!/usr/bin/env bash
# Deletes the ENTIRE shared k3d cluster and everything running on it, across
# every lab's environment. This is the nuclear option -- prefer each
# environment's own cleanup.sh (deletes just that one namespace) unless you
# genuinely want to tear the whole thing down and start over.

set -euo pipefail

CLUSTER_NAME="kafka-lab"

if ! k3d cluster list "${CLUSTER_NAME}" >/dev/null 2>&1; then
  echo "k3d cluster '${CLUSTER_NAME}' doesn't exist -- nothing to do."
  exit 0
fi

k3d cluster delete "${CLUSTER_NAME}"
echo "Cluster '${CLUSTER_NAME}' deleted."
