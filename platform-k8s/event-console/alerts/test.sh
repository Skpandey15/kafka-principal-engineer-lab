#!/usr/bin/env bash
# Validates the alert rules and runs their unit tests with promtool (from the Prometheus image).
# Needs only Docker; no Prometheus server is started.
set -euo pipefail
cd "$(dirname "$0")"
IMAGE="${PROMETHEUS_IMAGE:-prom/prometheus:v3.5.0}"
run() { docker run --rm --entrypoint promtool -v "$PWD:/rules:ro" -w /rules "${IMAGE}" "$@"; }
echo "== promtool check rules =="
run check rules event-console.rules.yml
echo "== promtool test rules =="
run test rules event-console.rules.test.yml
