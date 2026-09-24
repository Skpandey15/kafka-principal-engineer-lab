#!/usr/bin/env bash
# ./cleanup.sh          -- like `docker compose down`      (keeps data)
# ./cleanup.sh --wipe   -- like `docker compose down -v`   (deletes data)

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster

if [[ "${1:-}" == "--wipe" ]]; then
  echo "Deleting the entire 'kafka-security' namespace -- permanently deletes all data. No undo."
  kubectl delete namespace kafka-security --ignore-not-found
else
  echo "Deleting the Deployment and Service, keeping the PersistentVolumeClaim -- data survives."
  kubectl -n kafka-security delete deployment security-broker --ignore-not-found
  kubectl -n kafka-security delete service security-broker --ignore-not-found
fi
