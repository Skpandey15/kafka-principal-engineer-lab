#!/usr/bin/env bash
# Registers the event contract (default: event-v1.json) under the subject the services read and
# sets its compatibility rule. Idempotent: registering a schema that is already the latest version
# changes nothing and prints the same id.
#
#   ./register.sh [schema-file]
#
# A schema that breaks the compatibility rule is REFUSED by the registry (HTTP 409) and this script
# fails -- that is the point. Run check-compat.sh first to find out without registering.

set -euo pipefail
cd "$(dirname "$0")"
source ./lib.sh

FILE="${1:-event-v1.json}"
[[ -f "${FILE}" ]] || { echo "No such schema file: ${FILE}" >&2; exit 1; }

echo "Waiting for the Schema Registry..."
kubectl -n "${NS}" rollout status deploy/schema-registry --timeout=240s >/dev/null

echo "Setting compatibility for '${SUBJECT}' to ${COMPATIBILITY}..."
out="$(printf '{"compatibility":"%s"}' "${COMPATIBILITY}" | registry_curl -X PUT -d @- "http://localhost:8081/config/${SUBJECT}")"
[[ "$(echo "${out}" | http_code)" == "200" ]] || { echo "Could not set compatibility: $(echo "${out}" | http_body)" >&2; exit 1; }

echo "Registering ${FILE}..."
out="$(schema_request_body "${FILE}" | registry_curl -X POST -d @- "http://localhost:8081/subjects/${SUBJECT}/versions")"
code="$(echo "${out}" | http_code)"; body="$(echo "${out}" | http_body)"
case "${code}" in
  200) echo "Registered: ${body}" ;;
  409) echo "REFUSED: ${FILE} is not ${COMPATIBILITY}-compatible with the latest version of '${SUBJECT}'." >&2; echo "${body}" >&2; exit 1 ;;
  *)   echo "Registry answered HTTP ${code}: ${body}" >&2; exit 1 ;;
esac

echo -n "Latest version of '${SUBJECT}': "
registry_curl "http://localhost:8081/subjects/${SUBJECT}/versions/latest" </dev/null | http_body | grep -oE '"(version|id)":[0-9]+' | paste -sd' ' -
