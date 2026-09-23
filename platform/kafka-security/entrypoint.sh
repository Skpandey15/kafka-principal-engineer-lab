#!/usr/bin/env bash
# A fully custom entrypoint (overriding the image's own
# /etc/kafka/docker/run, the same pattern WP-11's Connect worker
# already established) -- needed because provisioning a REAL SCRAM
# credential at storage-format time (`kafka-storage.sh format
# --add-scram`) is not something the image's own docker wrapper
# (KafkaDockerWrapper) exposes through KAFKA_* environment variables.
set -euo pipefail

CONFIG=/opt/kafka/config/server.properties
CLUSTER_ID="K18ssQeW0XX-Db6Vr4Sk8Ih"

/opt/kafka/bin/kafka-storage.sh format \
    --cluster-id "$CLUSTER_ID" \
    --config "$CONFIG" \
    --add-scram 'SCRAM-SHA-512=[name=admin,password=admin-secret]' \
    --add-scram 'SCRAM-SHA-512=[name=reader,password=reader-secret]' \
    --ignore-formatted

exec /opt/kafka/bin/kafka-server-start.sh "$CONFIG"
