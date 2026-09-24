#!/usr/bin/env bash
# Creates the ONE shared k3d cluster every platform-k8s/*/setup.sh deploys
# into, with every lab's host port pre-mapped. Run this once, before any
# individual environment's setup.sh. Safe to re-run -- does nothing if the
# cluster already exists.
#
# Why one shared cluster instead of one per environment: k3d's host<->node
# port mappings are fixed at cluster-creation time and can't be added to an
# already-running cluster. Since every lab's environment lives in its own
# Kubernetes namespace (see _lib/common.sh), one cluster with every port
# pre-declared lets each lab's setup.sh/cleanup.sh manage only its own
# namespace, exactly like each platform/*/docker-compose.yml already manages
# only its own Compose project on the shared Docker daemon.
#
# Port table (host -> NodePort -> which lab/environment):
#   9092  -> 30092  platform-k8s/kafka             (lab-01 .. lab-05)
#   9093  -> 30093  platform-k8s/kafka-cluster      broker-1 (lab-06, lab-08 through lab-16)
#   9094  -> 30094  platform-k8s/kafka-cluster      broker-2
#   9095  -> 30095  platform-k8s/kafka-cluster      broker-3
#   7071  -> 30071  platform-k8s/kafka-cluster      broker-1 JMX exporter (lab-15)
#   7072  -> 30072  platform-k8s/kafka-cluster      broker-2 JMX exporter
#   7073  -> 30073  platform-k8s/kafka-cluster      broker-3 JMX exporter
#   9096  -> 30096  platform-k8s/kraft-quorum        broker-1 (lab-07)
#   9097  -> 30097  platform-k8s/kraft-quorum        broker-2
#   9098  -> 30098  platform-k8s/kraft-quorum        broker-3
#   8081  -> 30081  platform-k8s/schema-registry     (lab-09)
#   5432  -> 30432  platform-k8s/kafka-connect       postgres (lab-10, lab-11, lab-12)
#   8083  -> 30083  platform-k8s/kafka-connect       Connect REST API
#   9090  -> 30090  platform-k8s/observability       Prometheus (lab-15)
#   9308  -> 30308  platform-k8s/observability       kafka-exporter
#   3000  -> 30300  platform-k8s/observability       Grafana
#   9196  -> 30196  platform-k8s/kafka-security      (lab-17 -- 9196, not docker-compose's
#                                                      9096, specifically so this environment
#                                                      can coexist on the shared cluster with
#                                                      kraft-quorum's broker-1 without a port clash;
#                                                      the two never share a host port in
#                                                      docker-compose only because they're never
#                                                      run at the same time there)
#   19191 -> 30191  platform-k8s/multi-cluster-dr    primary cluster (lab-19)
#   19192 -> 30192  platform-k8s/multi-cluster-dr    secondary cluster (lab-19)

set -euo pipefail

CLUSTER_NAME="kafka-lab"

if k3d cluster list "${CLUSTER_NAME}" >/dev/null 2>&1; then
  echo "k3d cluster '${CLUSTER_NAME}' already exists -- nothing to do."
  echo "(To rebuild it from scratch, run destroy-cluster.sh first.)"
  exit 0
fi

k3d cluster create "${CLUSTER_NAME}" \
  --servers 1 --agents 0 \
  -p "9092:30092@server:0" \
  -p "9093:30093@server:0" -p "9094:30094@server:0" -p "9095:30095@server:0" \
  -p "7071:30071@server:0" -p "7072:30072@server:0" -p "7073:30073@server:0" \
  -p "9096:30096@server:0" -p "9097:30097@server:0" -p "9098:30098@server:0" \
  -p "8081:30081@server:0" \
  -p "5432:30432@server:0" -p "8083:30083@server:0" \
  -p "9090:30090@server:0" -p "9308:30308@server:0" -p "3000:30300@server:0" \
  -p "9196:30196@server:0" \
  -p "19191:30191@server:0" -p "19192:30192@server:0" \
  --wait

# A dedicated kubeconfig file, not the shared ~/.kube/config -- see
# _lib/common.sh for why. rm first: k3d's own write refuses to overwrite a
# kubeconfig pointing at a since-deleted cluster (a broken symlink-follow
# error), which only ever matters if you deleted and recreated this cluster.
KUBECONFIG_PATH="${HOME}/.kube/config-kafka-lab.yaml"
rm -f "${KUBECONFIG_PATH}"
k3d kubeconfig write "${CLUSTER_NAME}" -o "${KUBECONFIG_PATH}"

echo
echo "Cluster '${CLUSTER_NAME}' is up. Dedicated kubeconfig: ${KUBECONFIG_PATH}"
echo "Now run whichever lab environment's setup.sh you need, e.g.:"
echo "  platform-k8s/kafka/setup.sh"
