#!/usr/bin/env bash
# Deploys labs/lab-02b-spring-boot-event-console onto the shared k3d cluster:
#   React UI (nginx) + PRODUCER service + CONSUMER service + MongoDB.
# Prerequisite: platform-k8s/kafka/setup.sh.
#
#   SKIP_BUILD=1 ./setup.sh   -- reuse the already-built images instead of rebuilding
#
# Needs on the machine running it: Docker, k3d, kubectl, and a JDK able to run Gradle (the Gradle
# toolchain then downloads JDK 26 itself if it is missing).
# UI: http://localhost:8089

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

LAB_DIR="$(cd ../../labs/lab-02b-spring-boot-event-console && pwd)"
VERSION="0.2.0"
PRODUCER_IMAGE="kafkalab/event-console-producer:${VERSION}"
CONSUMER_IMAGE="kafkalab/event-console-consumer:${VERSION}"
FRONTEND_IMAGE="kafkalab/event-console-frontend:${VERSION}"
NS="event-console"

require_cluster
if ! kubectl get namespace kafka >/dev/null 2>&1; then
  echo "platform-k8s/kafka/setup.sh must be applied first (the services need the Kafka broker)." >&2
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
  for svc in producer consumer; do
    echo "Building the ${svc} jar..."
    (cd "${LAB_DIR}/${svc}" && chmod +x gradlew && ./gradlew --console=plain -q bootJar -x test -x integrationTest)
  done
  echo "Building images..."
  docker build -q -t "${PRODUCER_IMAGE}" "${LAB_DIR}/producer"
  docker build -q -t "${CONSUMER_IMAGE}" "${LAB_DIR}/consumer"
  docker build -q -t "${FRONTEND_IMAGE}" "${LAB_DIR}/frontend"
fi

# The cluster's nodes are containers with their own image store: a locally built image
# is invisible to Kubernetes until it is imported.
k3d image import -c "${KAFKA_LAB_CLUSTER_NAME}" "${PRODUCER_IMAGE}" "${CONSUMER_IMAGE}" "${FRONTEND_IMAGE}"

ensure_namespace "${NS}"

# Remove what the previous, combined "backend" deployment left behind (harmless if absent).
kubectl -n "${NS}" delete deployment event-console-backend --ignore-not-found >/dev/null
kubectl -n "${NS}" delete service event-console-backend --ignore-not-found >/dev/null
kubectl -n "${NS}" delete networkpolicy mongo-from-backend-only backend-from-frontend-only --ignore-not-found >/dev/null

random_password() { head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'; }

# Passwords are generated once and kept. Re-running setup must NOT rotate the ROOT one: MongoDB
# only reads MONGO_INITDB_ROOT_PASSWORD when it first initializes an empty data directory, so a
# new secret value against an old volume would lock the Job (and you) out. The per-service
# passwords are safe to keep too: the mongo-users Job re-applies them idempotently.
if ! kubectl -n "${NS}" get secret event-console-mongo >/dev/null 2>&1; then
  kubectl -n "${NS}" create secret generic event-console-mongo \
    --from-literal=MONGO_PASSWORD="$(random_password)" >/dev/null
  echo "Created Secret event-console-mongo (root, random password, not printed)."
fi
for svc in producer consumer; do
  if ! kubectl -n "${NS}" get secret "${svc}-mongo" >/dev/null 2>&1; then
    PASSWORD="$(random_password)"
    DB="eventconsole_${svc}"
    # authSource is the service's own database: the user is defined there and can touch only it.
    kubectl -n "${NS}" create secret generic "${svc}-mongo" \
      --from-literal=PASSWORD="${PASSWORD}" \
      --from-literal=MONGODB_URI="mongodb://${svc}:${PASSWORD}@mongo.${NS}.svc.cluster.local:27017/${DB}?authSource=${DB}" \
      >/dev/null
    echo "Created Secret ${svc}-mongo (least-privilege user for database ${DB}, not printed)."
  fi
done

# A Job's spec is immutable, so recreate it; it is idempotent (create-or-update each user).
kubectl -n "${NS}" delete job mongo-users --ignore-not-found >/dev/null
kubectl apply -f manifests.yaml

wait_for_pods_ready "${NS}" app=mongo 180s
kubectl -n "${NS}" wait --for=condition=complete job/mongo-users --timeout=180s
wait_for_pods_ready "${NS}" app=event-console-producer 240s
wait_for_pods_ready "${NS}" app=event-console-consumer 240s
wait_for_pods_ready "${NS}" app=event-console-frontend 120s

echo
echo "Event Console is up: http://localhost:8089"
