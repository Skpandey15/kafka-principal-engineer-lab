#!/usr/bin/env bash
# ./cleanup.sh          -- like `docker compose down`      (keeps data)
# ./cleanup.sh --wipe   -- like `docker compose down -v`   (deletes data)

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster

if [[ "${1:-}" == "--wipe" ]]; then
  echo "Deleting the entire 'kafka-cluster' namespace, including all three brokers' PersistentVolumeClaims -- permanently deletes all data. No undo."
  kubectl delete namespace kafka-cluster --ignore-not-found
else
  echo "Deleting the Deployments and Services, keeping all three PersistentVolumeClaims -- data survives."
  for n in 1 2 3; do
    kubectl -n kafka-cluster delete deployment "kafka-broker-${n}" --ignore-not-found
    kubectl -n kafka-cluster delete service "kafka-broker-${n}" --ignore-not-found
  done
fi
