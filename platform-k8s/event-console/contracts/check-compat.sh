#!/usr/bin/env bash
# Asks the registry whether a candidate schema is compatible with the latest registered version
# of the subject, WITHOUT registering it. Exits 0 if compatible, 1 if not -- suitable for CI.
#
#   ./check-compat.sh contracts/event-v2.json
set -euo pipefail
cd "$(dirname "$0")"
source ./lib.sh

FILE="${1:?usage: check-compat.sh <schema-file>}"
[[ -f "${FILE}" ]] || { echo "No such schema file: ${FILE}" >&2; exit 1; }

out="$(schema_request_body "${FILE}" | registry_curl -X POST -d @- \
  "http://localhost:8081/compatibility/subjects/${SUBJECT}/versions/latest?verbose=true")"
code="$(echo "${out}" | http_code)"; body="$(echo "${out}" | http_body)"
if [[ "${code}" != "200" ]]; then
  echo "Registry answered HTTP ${code}: ${body}" >&2
  exit 2
fi
if echo "${body}" | grep -q '"is_compatible":true'; then
  echo "COMPATIBLE   ${FILE}  (${COMPATIBILITY} with the latest version of '${SUBJECT}')"
  exit 0
fi
echo "INCOMPATIBLE ${FILE}  (${COMPATIBILITY} with the latest version of '${SUBJECT}')"
echo "${body}" | sed 's/^/  /'
exit 1
