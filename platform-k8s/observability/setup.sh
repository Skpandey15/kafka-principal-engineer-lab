#!/usr/bin/env bash
# Kubernetes (k3d) equivalent of `docker compose -f platform/observability/docker-compose.yml up -d`.
# Prerequisite: platform-k8s/kafka-cluster/setup.sh must already be applied
# (with its JMX exporter agents). Used by lab-15.

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster
if ! kubectl get namespace kafka-cluster >/dev/null 2>&1; then
  echo "platform-k8s/kafka-cluster/setup.sh must be applied first (Prometheus needs its brokers' JMX ports)." >&2
  exit 1
fi
kubectl apply -f manifests.yaml
wait_for_pods_ready observability app=prometheus 120s
wait_for_pods_ready observability app=kafka-exporter 60s
wait_for_pods_ready observability app=grafana 120s

echo
echo "Prometheus: http://localhost:9090"
echo "kafka-exporter metrics: http://localhost:9308/metrics"
echo "Grafana: http://localhost:3000 (anonymous admin access)"
