#!/usr/bin/env bash
# Kubernetes (k3d) equivalent of platform/kafka's two docker-compose cleanup
# modes:
#   ./cleanup.sh          -- like `docker compose down`      (keeps data)
#   ./cleanup.sh --wipe   -- like `docker compose down -v`   (deletes data)

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster

if [[ "${1:-}" == "--wipe" ]]; then
  echo "Deleting the entire 'kafka' namespace, including its PersistentVolumeClaim -- this permanently deletes all data. No undo."
  kubectl delete namespace kafka --ignore-not-found
else
  echo "Deleting the Deployment and Service, keeping the PersistentVolumeClaim (kafka-data) -- data survives."
  kubectl -n kafka delete deployment kafka --ignore-not-found
  kubectl -n kafka delete service kafka --ignore-not-found
fi
