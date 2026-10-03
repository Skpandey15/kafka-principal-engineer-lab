#!/usr/bin/env bash
# ./cleanup.sh          -- like `docker compose down`      (keeps MongoDB data and the Secrets)
# ./cleanup.sh --wipe   -- like `docker compose down -v`   (deletes the namespace: data AND Secrets)

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster

if [[ "${1:-}" == "--wipe" ]]; then
  echo "Deleting the entire 'event-console' namespace, including MongoDB's PersistentVolumeClaim and all its Secrets -- permanently deletes all data. No undo."
  kubectl delete namespace event-console --ignore-not-found
else
  echo "Deleting the Deployments and Services, keeping the MongoDB PersistentVolumeClaim and Secrets -- data survives."
  for d in event-console-frontend event-console-producer event-console-consumer mongo; do
    kubectl -n event-console delete deployment "${d}" --ignore-not-found
  done
  kubectl -n event-console delete service event-console-frontend event-console-producer event-console-consumer mongo --ignore-not-found
  kubectl -n event-console delete job mongo-users --ignore-not-found
fi
