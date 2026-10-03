#!/usr/bin/env bash
# Deploys labs/lab-02b-spring-boot-event-console (React UI + Spring Boot API + MongoDB)
# onto the shared k3d cluster. Prerequisite: platform-k8s/kafka/setup.sh.
#
#   SKIP_BUILD=1 ./setup.sh   -- reuse the already-built images instead of rebuilding
#
# Needs on the machine running it: JDK 21 (for ./gradlew bootJar), Docker, k3d, kubectl.
# UI: http://localhost:8089

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

LAB_DIR="$(cd ../../labs/lab-02b-spring-boot-event-console && pwd)"
BACKEND_IMAGE="kafkalab/event-console-backend:0.1.0"
FRONTEND_IMAGE="kafkalab/event-console-frontend:0.1.0"
NS="event-console"

require_cluster
if ! kubectl get namespace kafka >/dev/null 2>&1; then
  echo "platform-k8s/kafka/setup.sh must be applied first (the API needs the Kafka broker)." >&2
  exit 1
fi

# k3d fixes host<->node port mappings when a cluster is CREATED. A cluster made before
# this environment existed has no 8089 mapping; add it to the load balancer in place
# (no cluster rebuild, no data loss) rather than telling the user to start over.
if ! docker ps --format '{{.Ports}}' | grep -q ':8089->'; then
  echo "Host port 8089 is not mapped on '${KAFKA_LAB_CLUSTER_NAME}' yet -- adding it to the load balancer..."
  k3d cluster edit "${KAFKA_LAB_CLUSTER_NAME}" --port-add "8089:30089@loadbalancer"
fi

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  echo "Building the backend jar..."
  (cd "${LAB_DIR}" && chmod +x gradlew && ./gradlew --console=plain -q bootJar -x test)
  echo "Building images..."
  docker build -q -t "${BACKEND_IMAGE}" "${LAB_DIR}"
  docker build -q -t "${FRONTEND_IMAGE}" "${LAB_DIR}/frontend"
fi

# The cluster's nodes are containers with their own image store: a locally built image
# is invisible to Kubernetes until it is imported.
k3d image import -c "${KAFKA_LAB_CLUSTER_NAME}" "${BACKEND_IMAGE}" "${FRONTEND_IMAGE}"

ensure_namespace "${NS}"

# Generate the MongoDB password once and keep it. Re-running setup must NOT rotate it:
# MongoDB only reads MONGO_INITDB_ROOT_PASSWORD when it first initializes an empty data
# directory, so a new secret value against an old volume would lock the app out.
if ! kubectl -n "${NS}" get secret event-console-mongo >/dev/null 2>&1; then
  PASSWORD="$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
  kubectl -n "${NS}" create secret generic event-console-mongo \
    --from-literal=MONGO_PASSWORD="${PASSWORD}" \
    --from-literal=MONGODB_URI="mongodb://root:${PASSWORD}@mongo.${NS}.svc.cluster.local:27017/eventconsole?authSource=admin" \
    >/dev/null
  echo "Created Secret event-console-mongo (random password, not printed)."
fi

kubectl apply -f manifests.yaml
wait_for_pods_ready "${NS}" app=mongo 180s
wait_for_pods_ready "${NS}" app=event-console-backend 240s
wait_for_pods_ready "${NS}" app=event-console-frontend 120s

echo
echo "Event Console is up: http://localhost:8089"
