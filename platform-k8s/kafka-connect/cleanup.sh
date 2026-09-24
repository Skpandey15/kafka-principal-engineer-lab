#!/usr/bin/env bash
# ./cleanup.sh          -- like `docker compose down`      (keeps Postgres data)
# ./cleanup.sh --wipe   -- like `docker compose down -v`   (deletes Postgres data)

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster

if [[ "${1:-}" == "--wipe" ]]; then
  echo "Deleting the entire 'kafka-connect' namespace, including Postgres's PersistentVolumeClaim -- permanently deletes all data. No undo."
  kubectl delete namespace kafka-connect --ignore-not-found
else
  echo "Deleting Deployments and Services, keeping Postgres's PersistentVolumeClaim -- data survives."
  kubectl -n kafka-connect delete deployment postgres connect-worker --ignore-not-found
  kubectl -n kafka-connect delete service postgres connect-worker --ignore-not-found
fi
