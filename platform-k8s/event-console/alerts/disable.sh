#!/usr/bin/env bash
# Removes the Event Console's Prometheus (nothing else is touched; metrics history is dropped).
set -euo pipefail
cd "$(dirname "$0")"
source ../../_lib/common.sh

require_cluster
kubectl delete -f prometheus.yaml --ignore-not-found
kubectl -n event-console delete configmap event-console-alert-rules --ignore-not-found
