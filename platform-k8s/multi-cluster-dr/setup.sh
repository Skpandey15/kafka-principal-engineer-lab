#!/usr/bin/env bash
# Kubernetes (k3d) equivalent of lab-19's own TwoClusterEnvironment -- two
# real Kafka clusters plus a real MirrorMaker 2 process bridging them.
# Standalone -- does not depend on any other platform-k8s environment.
# Used by lab-19.

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster
kubectl apply -f manifests.yaml
wait_for_pods_ready multi-cluster-dr app=primary-kafka 120s
wait_for_pods_ready multi-cluster-dr app=secondary-kafka 120s

# MM2's dedicated-mode herder has no REST API to probe for readiness (see
# manifests.yaml's comment) -- just confirm the container is Running.
echo "Waiting for the mirror-maker pod to be Running..."
kubectl -n multi-cluster-dr wait --for=jsonpath='{.status.phase}'=Running pod -l app=mirror-maker --timeout=120s

echo
echo "Primary cluster:   localhost:19191"
echo "Secondary cluster: localhost:19192"
echo "MirrorMaker 2 is replicating primary -> secondary."
echo "A topic created on primary shows up on secondary as primary.<topic>."
