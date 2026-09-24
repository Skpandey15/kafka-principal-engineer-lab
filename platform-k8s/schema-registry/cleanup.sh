#!/usr/bin/env bash
# Schema Registry keeps no persistent state of its own (its state lives in
# the `_schemas` Kafka topic, in kafka-cluster) -- so there is only one
# cleanup mode, unlike the Kafka environments' keep-data/wipe-data split.

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

require_cluster
kubectl delete namespace schema-registry --ignore-not-found
