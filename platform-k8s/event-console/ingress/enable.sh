#!/usr/bin/env bash
# Exposes the Event Console through the cluster's Traefik ingress:
#   UI      http://localhost:8090
#   MongoDB localhost:27017   (for MongoDB Compass; authenticated, bound to 127.0.0.1 only)
#
# Opt-in on purpose -- it opens a database port. Prerequisite: event-console/setup.sh.
# Undo with ./disable.sh.

set -euo pipefail
cd "$(dirname "$0")"
source ../../_lib/common.sh

require_cluster
if ! kubectl get namespace event-console >/dev/null 2>&1; then
  echo "platform-k8s/event-console/setup.sh must be applied first." >&2
  exit 1
fi

echo "Adding a TCP entrypoint for MongoDB to the bundled Traefik..."
kubectl apply -f traefik-mongo-entrypoint.yaml
# k3s re-runs the Helm chart; wait until Traefik's Service really exposes 27017 and has rolled out.
for i in $(seq 1 60); do
  if kubectl -n kube-system get svc traefik -o jsonpath='{.spec.ports[*].port}' | grep -qw 27017; then break; fi
  [[ $i -eq 60 ]] && { echo "Traefik never exposed port 27017." >&2; exit 1; }
  sleep 2
done
kubectl -n kube-system rollout status deploy/traefik --timeout=180s

# Host<->cluster port mappings are fixed when a k3d cluster is created; add these two to the
# load balancer in place. 127.0.0.1 keeps them off your network (only this machine can connect).
add_ports=()
docker ps --format '{{.Ports}}' | grep -q ':8090->' || add_ports+=(--port-add "127.0.0.1:8090:80@loadbalancer")
docker ps --format '{{.Ports}}' | grep -q ':27017->' || add_ports+=(--port-add "127.0.0.1:27017:27017@loadbalancer")
if [[ ${#add_ports[@]} -gt 0 ]]; then
  echo "Mapping host ports on the '${KAFKA_LAB_CLUSTER_NAME}' load balancer..."
  k3d cluster edit "${KAFKA_LAB_CLUSTER_NAME}" "${add_ports[@]}"
fi

kubectl apply -f ingress.yaml

echo
echo "UI:      http://localhost:8090"
echo "MongoDB: mongodb://root:<PASSWORD>@localhost:27017/?authSource=admin&directConnection=true"
echo
echo "Print the password (it is never shown by these scripts):"
echo "  kubectl --kubeconfig ${KUBECONFIG} -n event-console get secret event-console-mongo -o jsonpath='{.data.MONGO_PASSWORD}' | base64 -d; echo"
