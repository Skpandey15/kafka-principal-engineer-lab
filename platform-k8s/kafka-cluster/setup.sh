#!/usr/bin/env bash
# Kubernetes (k3d) equivalent of `docker compose -f platform/kafka-cluster/docker-compose.yml up -d`.
# Used by lab-06, lab-08 through lab-16, and lab-18's production-simulation
# experiment. Requires platform-k8s/bootstrap-cluster.sh to have run once.

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster
kubectl apply -f manifests.yaml
wait_for_pods_ready kafka-cluster "app in (kafka-broker-1,kafka-broker-2,kafka-broker-3)" 240s

echo
echo "3-broker cluster is up. Bootstrap servers: localhost:9093, localhost:9094, localhost:9095"
echo "JMX exporter metrics: localhost:7071, localhost:7072, localhost:7073"
echo
echo "Running a Kafka CLI tool via 'kubectl exec' against a broker needs KAFKA_OPTS"
echo "cleared for that one invocation, same as the docker-compose healthcheck already"
echo "does -- otherwise the CLI tool inherits the container-wide KAFKA_OPTS (the JMX"
echo "javaagent) and fails to bind its HTTP port a second time. Example:"
echo "  kubectl -n kafka-cluster exec deploy/kafka-broker-1 -- sh -c \"KAFKA_OPTS= /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-broker-1:19092 --list\""
