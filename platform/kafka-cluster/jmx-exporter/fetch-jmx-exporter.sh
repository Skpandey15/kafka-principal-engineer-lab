#!/usr/bin/env bash
# Downloads the real Prometheus JMX exporter Java agent (WP-16,
# observability) -- idempotent, skips if already present. Mirrors
# WP-11's platform/kafka-connect/fetch-plugins.sh convention: fetch
# external, separately-governed artifacts explicitly and once, rather
# than baking them into a custom image.
#
# io.prometheus.jmx:jmx_prometheus_javaagent -- a real, independently
# governed Prometheus community project (github.com/prometheus/jmx_exporter),
# not part of Apache Kafka or Confluent.
set -euo pipefail

VERSION="1.0.1"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="$DIR/jmx_prometheus_javaagent.jar"
URL="https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${VERSION}/jmx_prometheus_javaagent-${VERSION}.jar"

if [ -f "$JAR" ]; then
    echo "jmx_prometheus_javaagent.jar already present at $JAR, skipping download."
else
    echo "Downloading jmx_prometheus_javaagent ${VERSION} from Maven Central..."
    curl -fsSL -o "$JAR" "$URL"
    echo "Downloaded to $JAR"
fi
