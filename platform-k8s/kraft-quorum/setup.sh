#!/usr/bin/env bash
# Kubernetes (k3d) equivalent of `docker compose -f platform/kraft-quorum/docker-compose.yml up -d`.
# Used by lab-07. Requires platform-k8s/bootstrap-cluster.sh to have run once.

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster
kubectl apply -f manifests.yaml
wait_for_pods_ready kraft-quorum "app in (kafka-controller-1,kafka-controller-2,kafka-controller-3)" 180s
wait_for_pods_ready kraft-quorum "app in (kraft-quorum-broker-1,kraft-quorum-broker-2,kraft-quorum-broker-3)" 240s

echo
echo "Dedicated-role cluster is up. Broker bootstrap servers: localhost:9096, localhost:9097, localhost:9098"
echo "Controllers have no host-published port -- same as docker-compose."
