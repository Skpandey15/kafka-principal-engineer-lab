#!/usr/bin/env bash
# ./cleanup.sh          -- like `docker compose down`      (keeps data)
# ./cleanup.sh --wipe   -- like `docker compose down -v`   (deletes data)

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster

if [[ "${1:-}" == "--wipe" ]]; then
  echo "Deleting the entire 'kraft-quorum' namespace -- permanently deletes all data. No undo."
  kubectl delete namespace kraft-quorum --ignore-not-found
else
  echo "Deleting Deployments and Services, keeping PersistentVolumeClaims -- data survives."
  for n in kafka-controller-1 kafka-controller-2 kafka-controller-3 kraft-quorum-broker-1 kraft-quorum-broker-2 kraft-quorum-broker-3; do
    kubectl -n kraft-quorum delete deployment "$n" --ignore-not-found
    kubectl -n kraft-quorum delete service "$n" --ignore-not-found
  done
fi
