#!/usr/bin/env bash
# ./cleanup.sh          -- keeps both clusters' data
# ./cleanup.sh --wipe   -- deletes everything, no undo

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster

if [[ "${1:-}" == "--wipe" ]]; then
  echo "Deleting the entire 'multi-cluster-dr' namespace, including both clusters' PersistentVolumeClaims -- permanently deletes all data. No undo."
  kubectl delete namespace multi-cluster-dr --ignore-not-found
else
  echo "Deleting Deployments and Services, keeping both PersistentVolumeClaims -- data survives."
  kubectl -n multi-cluster-dr delete deployment primary-kafka secondary-kafka mirror-maker --ignore-not-found
  kubectl -n multi-cluster-dr delete service primary-kafka secondary-kafka --ignore-not-found
fi
