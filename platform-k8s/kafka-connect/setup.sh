#!/usr/bin/env bash
# Kubernetes (k3d) equivalent of `docker compose -f platform/kafka-connect/docker-compose.yml up -d`.
# Prerequisite: platform-k8s/kafka-cluster/setup.sh must already be applied.
# Used by lab-10, lab-11, lab-12.

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster
if ! kubectl get namespace kafka-cluster >/dev/null 2>&1; then
  echo "platform-k8s/kafka-cluster/setup.sh must be applied first (Connect needs its brokers)." >&2
  exit 1
fi
kubectl apply -f manifests.yaml
wait_for_pods_ready kafka-connect app=postgres 120s
wait_for_pods_ready kafka-connect app=connect-worker 180s

echo
echo "Postgres: localhost:5432 (db=inventory, user=postgres, password=postgres)"
echo "Kafka Connect REST API: http://localhost:8083"
