#!/usr/bin/env bash
# Kubernetes (k3d) equivalent of `docker compose -f platform/kafka/docker-compose.yml up -d`.
# Used by lab-01 through lab-05. Requires platform-k8s/bootstrap-cluster.sh
# to have been run once already.

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster
kubectl apply -f manifests.yaml
wait_for_pods_ready kafka app=kafka

echo
echo "Kafka is up. Bootstrap server: localhost:9092"
echo "Run CLI tools the same way the lab README shows, e.g.:"
echo "  kubectl -n kafka exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
