#!/usr/bin/env bash
# Kubernetes (k3d) equivalent of `docker compose -f platform/schema-registry/docker-compose.yml up -d`.
# Prerequisite: platform-k8s/kafka-cluster/setup.sh must already be applied.
# Used by lab-09.

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster
if ! kubectl get namespace kafka-cluster >/dev/null 2>&1; then
  echo "platform-k8s/kafka-cluster/setup.sh must be applied first (Schema Registry needs its brokers)." >&2
  exit 1
fi
kubectl apply -f manifests.yaml
wait_for_pods_ready schema-registry app=schema-registry 180s

echo
echo "Schema Registry is up: http://localhost:8081"
