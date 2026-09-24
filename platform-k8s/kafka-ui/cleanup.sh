#!/usr/bin/env bash
# No persistent data of its own beyond what DYNAMIC_CONFIG_ENABLED writes
# into the container's own filesystem (lost on pod recreation either way,
# same as Docker Compose never declaring a volume for it) -- one cleanup
# mode.

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster
kubectl delete namespace kafka-ui --ignore-not-found
