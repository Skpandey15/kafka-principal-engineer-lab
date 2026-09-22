#!/usr/bin/env bash
# Downloads the two Connect plugins this WP's worker needs into
# platform/kafka-connect/plugins/, where CONNECT_PLUGIN_PATH points. Run
# this once before `docker compose up` -- see
# platform/kafka-connect/README.md, "Setup."
#
# Both are pinned to explicit versions, never a moving "latest" tag, and
# both were verified reachable via a real HTTP request before being
# pinned here -- not assumed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_DIR="${SCRIPT_DIR}/plugins"
mkdir -p "${PLUGIN_DIR}"

# --- connect-file: the FileStream source/sink connectors used for
# Phase 1 (Connect fundamentals). A REAL, verified finding building this
# lab: these connectors are NOT on connect-distributed.sh's default
# classpath in kafka-clients 4.3.1 -- despite connect-file-4.3.1.jar
# physically existing under /opt/kafka/libs/ in the apache/kafka:4.3.1
# image, the actual running Java process's -cp argument does not include
# it (confirmed by inspecting /proc/1/cmdline inside the container).
# They have to be added via plugin.path explicitly, exactly like any
# external connector -- this is NOT the "already just works, no setup
# needed" assumption a first read of the Kafka Connect quickstart docs
# might suggest. Fetched from Maven Central here rather than copied out
# of a running container, so this step is reproducible without Docker
# already running.
KAFKA_VERSION="4.3.1"
CONNECT_FILE_DIR="${PLUGIN_DIR}/connect-file"
CONNECT_FILE_URL="https://repo1.maven.org/maven2/org/apache/kafka/connect-file/${KAFKA_VERSION}/connect-file-${KAFKA_VERSION}.jar"

if [ -f "${CONNECT_FILE_DIR}/connect-file-${KAFKA_VERSION}.jar" ]; then
  echo "Already present: ${CONNECT_FILE_DIR}/connect-file-${KAFKA_VERSION}.jar (delete it to re-fetch)"
else
  echo "Fetching connect-file ${KAFKA_VERSION} (FileStream source/sink connectors)..."
  mkdir -p "${CONNECT_FILE_DIR}"
  curl -fsSL -o "${CONNECT_FILE_DIR}/connect-file-${KAFKA_VERSION}.jar" "${CONNECT_FILE_URL}"
  echo "Done: ${CONNECT_FILE_DIR}/connect-file-${KAFKA_VERSION}.jar"
fi

# --- debezium-connector-postgres: the PostgreSQL CDC connector used for
# Phase 2. Pinned to 3.6.3.Final, the latest stable release on Maven
# Central at the time this was verified. The "-plugin" classifier is
# Debezium's own self-contained bundle (the connector plus every
# dependency it needs, ~5MB) -- not the bare connector JAR alone, which
# would be missing its dependencies.
DEBEZIUM_VERSION="3.6.3.Final"
DEBEZIUM_ARCHIVE_URL="https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz"
DEBEZIUM_ARCHIVE_PATH="${PLUGIN_DIR}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz"

if [ -d "${PLUGIN_DIR}/debezium-connector-postgres" ]; then
  echo "Already present: ${PLUGIN_DIR}/debezium-connector-postgres (delete it to re-fetch)"
else
  echo "Fetching Debezium PostgreSQL connector ${DEBEZIUM_VERSION} plugin bundle..."
  curl -fsSL -o "${DEBEZIUM_ARCHIVE_PATH}" "${DEBEZIUM_ARCHIVE_URL}"
  echo "Extracting into ${PLUGIN_DIR}..."
  tar -xzf "${DEBEZIUM_ARCHIVE_PATH}" -C "${PLUGIN_DIR}"
  rm "${DEBEZIUM_ARCHIVE_PATH}"
  echo "Done: ${PLUGIN_DIR}/debezium-connector-postgres"
fi
