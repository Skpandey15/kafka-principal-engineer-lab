#!/usr/bin/env bash
# No persistent data of its own (Prometheus/Grafana state is not preserved
# in this lab environment, same as docker-compose never declaring a volume
# for either) -- one cleanup mode.

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster
kubectl delete namespace observability --ignore-not-found
