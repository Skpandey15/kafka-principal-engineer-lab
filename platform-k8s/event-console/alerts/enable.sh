#!/usr/bin/env bash
# Starts a small Prometheus inside the event-console namespace that scrapes the producer and consumer
# and evaluates event-console.rules.yml. Optional (about 100-250 MiB). Look at it with:
#
#   kubectl -n event-console port-forward svc/prometheus 9090:9090      # then http://localhost:9090/alerts
#
# ./disable.sh removes it again.
set -euo pipefail
cd "$(dirname "$0")"
source ../../_lib/common.sh

require_cluster
kubectl get namespace event-console >/dev/null 2>&1 || { echo "Deploy platform-k8s/event-console/setup.sh first." >&2; exit 1; }

# The rules live in a ConfigMap built straight from the file, so the file in git is what runs.
kubectl -n event-console create configmap event-console-alert-rules \
  --from-file=event-console.rules.yml --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -f prometheus.yaml
# A changed ConfigMap is not picked up by a running pod on its own.
kubectl -n event-console rollout restart deployment/prometheus >/dev/null
kubectl -n event-console rollout status deployment/prometheus --timeout=180s
echo
echo "Prometheus is up. Open it with:  kubectl -n event-console port-forward svc/prometheus 9090:9090"
echo "  Alerts:   http://localhost:9090/alerts        Targets: http://localhost:9090/targets"
