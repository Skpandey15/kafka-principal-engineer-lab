#!/usr/bin/env bash
# Kubernetes (k3d) equivalent of `docker compose -f platform/kafka-ui/docker-compose.yml up -d`.
#
# Which Kafka it browses is selected with KAFKA_UI_TARGET (default: kafka-cluster):
#   KAFKA_UI_TARGET=kafka-cluster ./setup.sh  -- the 3-broker cluster
#                                                (prerequisite: platform-k8s/kafka-cluster/setup.sh;
#                                                 platform-k8s/schema-registry is optional, schema browsing only)
#   KAFKA_UI_TARGET=kafka ./setup.sh          -- the single-node lab-01..05 broker
#                                                (prerequisite: platform-k8s/kafka/setup.sh) -- the lightweight
#                                                choice when memory is tight, one JVM instead of three
#
# Host port is 8088 (not 8080) -- see platform-k8s/bootstrap-cluster.sh's port table.

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

TARGET="${KAFKA_UI_TARGET:-kafka-cluster}"

require_cluster
case "${TARGET}" in
  kafka-cluster)
    if ! kubectl get namespace kafka-cluster >/dev/null 2>&1; then
      echo "platform-k8s/kafka-cluster/setup.sh must be applied first (Kafbat UI needs its brokers)." >&2
      exit 1
    fi
    kubectl apply -f manifests.yaml
    ;;
  kafka)
    if ! kubectl get namespace kafka >/dev/null 2>&1; then
      echo "platform-k8s/kafka/setup.sh must be applied first (Kafbat UI needs its broker)." >&2
      exit 1
    fi
    # manifests.yaml targets the 3-broker cluster; swap in the single node's
    # internal listener (kafka.kafka.svc.cluster.local:19092) before applying,
    # so the pod starts with the right config instead of restarting after.
    sed \
      -e 's|value: "wp07-3-broker-cluster"|value: "lab01-single-broker"|' \
      -e 's|value: "kafka-broker-1\.kafka-cluster[^"]*"|value: "kafka.kafka.svc.cluster.local:19092"|' \
      manifests.yaml | kubectl apply -f -
    ;;
  *)
    echo "KAFKA_UI_TARGET must be 'kafka-cluster' or 'kafka' (got '${TARGET}')." >&2
    exit 1
    ;;
esac

wait_for_pods_ready kafka-ui app=kafka-ui 120s

echo
echo "Kafbat UI is up: http://localhost:8088  (browsing: ${TARGET})"
