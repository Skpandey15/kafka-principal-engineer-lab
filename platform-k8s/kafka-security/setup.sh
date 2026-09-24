#!/usr/bin/env bash
# Kubernetes (k3d) equivalent of `docker compose -f platform/kafka-security/docker-compose.yml up -d`.
# Used by lab-17. Standalone -- generates its own TLS certs fresh via an
# initContainer (the k8s equivalent of certs/generate-certs.sh), no manual
# step needed first. NOTE: the host port is 9196, not docker-compose's 9096
# -- see platform-k8s/bootstrap-cluster.sh's port table for why.

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster
kubectl apply -f manifests.yaml
wait_for_pods_ready kafka-security app=security-broker 180s

echo
echo "Security-hardened broker is up. Bootstrap server: localhost:9196"
echo "This is SASL_SSL + SCRAM-SHA-512 -- a plain kafka-topics.sh call will"
echo "fail without a client config. From inside the pod, use the checked-in"
echo "client properties, e.g.:"
echo "  kubectl -n kafka-security exec deploy/security-broker -- /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9094 --command-config /etc/kafka/secrets/admin-client.properties"
