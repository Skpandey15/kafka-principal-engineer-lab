#!/usr/bin/env bash
# Kubernetes (k3d) equivalent of `docker compose -f platform/kafka-ui/docker-compose.yml up -d`.
# Prerequisite: platform-k8s/kafka-cluster/setup.sh must already be applied.
# platform-k8s/schema-registry is optional (schema browsing only).

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster
if ! kubectl get namespace kafka-cluster >/dev/null 2>&1; then
  echo "platform-k8s/kafka-cluster/setup.sh must be applied first (Kafbat UI needs its brokers)." >&2
  exit 1
fi
kubectl apply -f manifests.yaml
wait_for_pods_ready kafka-ui app=kafka-ui 120s

echo
echo "Kafbat UI is up: http://localhost:8080"
