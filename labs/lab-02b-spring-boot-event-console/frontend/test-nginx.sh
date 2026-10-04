#!/usr/bin/env bash
# Tests the edge for real: builds the UI image, runs it next to a stand-in backend, and checks who
# gets in, who may change things, that credentials never reach the services, and that writes are
# rate-limited. Needs only Docker.
#
# Every request is made by a client container ON THE TEST NETWORK (docker exec), never through a
# published host port. That way it behaves the same on a laptop and inside a CI container that talks to
# the host's Docker daemon, where "127.0.0.1:<published port>" would point at the wrong machine.
#
#   ./test-nginx.sh
set -euo pipefail
cd "$(dirname "$0")"

NET="ec-edge-test-$$"
TMP="$(mktemp -d)"
IMAGE="kafkalab/event-console-frontend:edge-test"
CLIENT="${NET}-client"
URL="http://ui:8080"
cleanup() {
  docker rm -f "${NET}-ui" "${NET}-backend" "${CLIENT}" >/dev/null 2>&1 || true
  docker network rm "${NET}" >/dev/null 2>&1 || true
  rm -rf "${TMP}"
}
trap cleanup EXIT

fail=0
check() { # check <description> <expected> <actual>
  if [[ "$2" == "$3" ]]; then printf '  ok    %s -> %s\n' "$1" "$3"; else printf '  FAIL  %s -> %s (expected %s)\n' "$1" "$3" "$2"; fail=1; fi
}

echo "Building the image..."
docker build -t "${IMAGE}" . >/dev/null 2>&1 || { echo "image build failed; re-run: docker build -t ${IMAGE} ."; exit 1; }

# Two accounts, like setup.sh makes: `viewers` has both, `operators` only the operator.
VIEWER="viewer:$(openssl passwd -apr1 viewer-secret)"
OPERATOR="operator:$(openssl passwd -apr1 operator-secret)"
printf '%s\n%s\n' "${VIEWER}" "${OPERATOR}" > "${TMP}/viewers.htpasswd"
printf '%s\n' "${OPERATOR}" > "${TMP}/operators.htpasswd"

# A stand-in for both services: it answers 200 and echoes the Authorization header it received.
cat > "${TMP}/backend.conf" <<'CONF'
server { listen 8080; location / { default_type text/plain; return 200 "auth=[$http_authorization]"; } }
CONF

# The files are handed to the containers by copying them in, not by bind-mounting this directory: a bind
# mount names a path on the DOCKER HOST, which is not this machine when the script runs inside a CI container.
docker network create "${NET}" >/dev/null
docker create --name "${NET}-backend" --network "${NET}" --network-alias backend nginx:1.27-alpine >/dev/null
docker cp "${TMP}/backend.conf" "${NET}-backend:/etc/nginx/conf.d/default.conf"
docker start "${NET}-backend" >/dev/null
docker create --name "${NET}-ui" --network "${NET}" --network-alias ui \
  -e PRODUCER_UPSTREAM=http://backend:8080 -e CONSUMER_UPSTREAM=http://backend:8080 -e DNS_RESOLVER=127.0.0.11 \
  "${IMAGE}" >/dev/null
docker cp "${TMP}/viewers.htpasswd" "${NET}-ui:/etc/nginx/viewers.htpasswd.tmp"
docker cp "${TMP}/operators.htpasswd" "${NET}-ui:/etc/nginx/operators.htpasswd.tmp"
docker start "${NET}-ui" >/dev/null

# The auth directory is read-only in the image's filesystem layout for the unprivileged user, so place the
# files with a one-shot root exec, then reload.
docker exec -u 0 "${NET}-ui" sh -c 'mkdir -p /etc/nginx/auth && mv /etc/nginx/viewers.htpasswd.tmp /etc/nginx/auth/viewers.htpasswd \
  && mv /etc/nginx/operators.htpasswd.tmp /etc/nginx/auth/operators.htpasswd && chmod 644 /etc/nginx/auth/* && nginx -s reload'

docker run -d --name "${CLIENT}" --network "${NET}" --entrypoint sleep curlimages/curl:8.10.1 infinity >/dev/null
c() { docker exec "${CLIENT}" curl -s "$@"; }                       # curl, on the test network
code() { c -o /dev/null -w '%{http_code}' "$@"; }
for _ in $(seq 1 30); do [[ "$(code "${URL}/healthz")" == "200" ]] && break; sleep 1; done
V=(-u viewer:viewer-secret); O=(-u operator:operator-secret)

echo "Who gets in:"
check "health check, no login (Kubernetes probes cannot log in)" 200 "$(code "${URL}/healthz")"
check "UI without a login" 401 "$(code "${URL}/")"
check "UI as a wrong user" 401 "$(code -u viewer:wrong "${URL}/")"
check "UI as viewer" 200 "$(code "${V[@]}" "${URL}/")"
check "UI as operator" 200 "$(code "${O[@]}" "${URL}/")"
check "read the events as viewer" 200 "$(code "${V[@]}" "${URL}/api/events")"
check "read the config as viewer" 200 "$(code "${V[@]}" "${URL}/api/config")"

echo "Who may change things:"
check "publish as viewer" 401 "$(code "${V[@]}" -X POST "${URL}/api/publish/bulk")"
check "publish as operator" 200 "$(code "${O[@]}" -X POST "${URL}/api/publish/bulk")"
check "requeue as viewer" 401 "$(code "${V[@]}" -X POST "${URL}/api/events/abc/requeue")"
check "requeue as operator" 200 "$(code "${O[@]}" -X POST "${URL}/api/events/abc/requeue")"
check "bulk requeue as viewer" 401 "$(code "${V[@]}" -X POST "${URL}/api/events/requeue-dead")"
check "clear as viewer" 401 "$(code "${V[@]}" -X DELETE "${URL}/api/events")"
check "clear as operator" 200 "$(code "${O[@]}" -X DELETE "${URL}/api/events")"

echo "Credentials never reach the services:"
check "Authorization header seen by the backend" "auth=[]" "$(c "${V[@]}" "${URL}/api/events")"
check "...also on a write" "auth=[]" "$(c "${O[@]}" -X POST "${URL}/api/publish/bulk")"

# The bursts run INSIDE the client container, in one shell, so they are as fast as a real flood.
echo "Writes are rate-limited (5/s, burst 10), reads are not:"
sleep 2
writes_429=$(docker exec "${CLIENT}" sh -c "n=0; for i in \$(seq 1 40); do c=\$(curl -s -o /dev/null -w '%{http_code}' -u operator:operator-secret -X POST ${URL}/api/publish/bulk); [ \"\$c\" = 429 ] && n=\$((n+1)); done; echo \$n")
check "40 quick publishes: at least 20 were refused with 429" yes "$([[ ${writes_429} -ge 20 ]] && echo yes || echo "no (${writes_429})")"
sleep 2
reads_429=$(docker exec "${CLIENT}" sh -c "n=0; for i in \$(seq 1 40); do c=\$(curl -s -o /dev/null -w '%{http_code}' -u viewer:viewer-secret ${URL}/api/events); [ \"\$c\" = 429 ] && n=\$((n+1)); done; echo \$n")
check "40 quick reads: none refused" 0 "${reads_429}"

echo
if [[ ${fail} -eq 0 ]]; then
  echo "edge: all checks passed"
else
  echo "edge: FAILED -- the edge's own log, for diagnosis:"
  docker logs "${NET}-ui" 2>&1 | grep -E "\[(error|crit|emerg|warn)\]" | tail -8 | sed 's/^/    /'
  exit 1
fi
