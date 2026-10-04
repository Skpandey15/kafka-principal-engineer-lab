#!/usr/bin/env bash
# Runs the pipeline on the local Jenkins and waits for the result, streaming the console.
#
#   ci/jenkins/trigger.sh "lab-02b-spring-boot-event-console/producer,lab-02b-spring-boot-event-console/consumer"
#
# The argument is the LABS parameter (comma-separated lab paths). An EMPTY argument runs every lab,
# which needs a machine far larger than a laptop -- so it is refused unless you pass --all.
set -euo pipefail

PORT="${JENKINS_PORT:-8082}"
BASE="http://localhost:${PORT}"
JOB=kafka-lab
LABS="${1:-}"
if [[ -z "${LABS}" && "${1:-}" != "--all" ]]; then
  echo "Give the labs to run (see the top of the Jenkinsfile), or --all for everything." >&2
  exit 2
fi
[[ "${LABS}" == "--all" ]] && LABS=""

JAR="$(mktemp)"; trap 'rm -f "${JAR}"' EXIT
CRUMB="$(curl -fsS -c "${JAR}" "${BASE}/crumbIssuer/api/xml?xpath=concat(//crumbRequestField,%22:%22,//crumb)")"
LOCATION="$(curl -fsS -b "${JAR}" -H "${CRUMB}" -X POST -D - -o /dev/null \
  --data-urlencode "LABS=${LABS}" "${BASE}/job/${JOB}/buildWithParameters" | tr -d '\r' | awk -F': ' 'tolower($1)=="location"{print $2}')"
[[ -n "${LOCATION}" ]] || { echo "Jenkins did not accept the build." >&2; exit 1; }
echo "Queued: ${LOCATION}"

echo -n "Waiting for an executor"
BUILD_URL=""
for _ in $(seq 1 120); do
  BUILD_URL="$(curl -fsS "${LOCATION}api/json" | grep -o '"executable":{[^}]*}' | grep -o 'http[^"]*' | head -1 || true)"
  [[ -n "${BUILD_URL}" ]] && break
  echo -n "."; sleep 2
done
[[ -n "${BUILD_URL}" ]] || { echo; echo "The build never started." >&2; exit 1; }
echo; echo "Running: ${BUILD_URL}"

OFFSET=0
while true; do
  RESP="$(mktemp)"
  curl -fsS -D "${RESP}.h" "${BUILD_URL}logText/progressiveText?start=${OFFSET}" -o "${RESP}"
  cat "${RESP}"
  OFFSET="$(tr -d '\r' < "${RESP}.h" | awk -F': ' 'tolower($1)=="x-text-size"{print $2}')"
  MORE="$(tr -d '\r' < "${RESP}.h" | awk -F': ' 'tolower($1)=="x-more-data"{print $2}')"
  rm -f "${RESP}" "${RESP}.h"
  [[ "${MORE}" == "true" ]] || break
  sleep 3
done

RESULT="$(curl -fsS "${BUILD_URL}api/json" | grep -o '"result":"[A-Z]*"' | cut -d'"' -f4)"
echo; echo "RESULT: ${RESULT}"
[[ "${RESULT}" == "SUCCESS" ]]
