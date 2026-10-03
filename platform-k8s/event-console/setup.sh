#!/usr/bin/env bash
# Deploys labs/lab-02b-spring-boot-event-console onto the shared k3d cluster:
#   React UI (nginx) + PRODUCER service + CONSUMER service + MongoDB + Schema Registry (the event contract).
# Prerequisite: platform-k8s/kafka/setup.sh.
#
#   SKIP_BUILD=1 ./setup.sh        -- reuse the already-built images instead of rebuilding
#   MONGO_HA=1 ./setup.sh          -- MongoDB as a 3-member replica set instead of one instance
#                                     (needs ~600 MiB more memory; switching modes starts an EMPTY database)
#   SINGLE_REPLICA=1 ./setup.sh    -- one replica of each service (the manifests default to two, which is
#                                     what high availability means; a small laptop cluster may not afford it)
#
# Needs on the machine running it: Docker, k3d, kubectl, openssl, and a JDK able to run Gradle (the Gradle
# toolchain then downloads JDK 26 itself if it is missing).
# UI: http://localhost:8089   (logins: ./credentials.sh)

set -euo pipefail
cd "$(dirname "$0")"
source ../_lib/common.sh

LAB_DIR="$(cd ../../labs/lab-02b-spring-boot-event-console && pwd)"
VERSION="0.4.2"
PRODUCER_IMAGE="kafkalab/event-console-producer:${VERSION}"
CONSUMER_IMAGE="kafkalab/event-console-consumer:${VERSION}"
FRONTEND_IMAGE="kafkalab/event-console-frontend:${VERSION}"
NS="event-console"
MONGO_HA="${MONGO_HA:-0}"

require_cluster
if ! kubectl get namespace kafka >/dev/null 2>&1; then
  echo "platform-k8s/kafka/setup.sh must be applied first (the services need the Kafka broker)." >&2
  exit 1
fi
command -v openssl >/dev/null || { echo "openssl is required (login hashes, replica-set key file)." >&2; exit 1; }

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
secret_value() { kubectl -n "${NS}" get secret "$1" -o "jsonpath={.data.$2}" 2>/dev/null | base64 -d 2>/dev/null || true; }

# Passwords are generated once and kept. Re-running setup must NOT rotate the ROOT one: MongoDB
# only reads MONGO_INITDB_ROOT_PASSWORD when it first initializes an empty data directory, so a
# new secret value against an old volume would lock the Job (and you) out. The per-service
# passwords are safe to keep too: the mongo-users Job re-applies them idempotently.
if ! kubectl -n "${NS}" get secret event-console-mongo >/dev/null 2>&1; then
  kubectl -n "${NS}" create secret generic event-console-mongo \
    --from-literal=MONGO_PASSWORD="$(random_password)" >/dev/null
  echo "Created Secret event-console-mongo (root, random password, not printed)."
fi

# How the services reach MongoDB. One instance: its Service name. A replica set: every member (so the
# driver can find the primary after a failover) plus the set name, and w=majority so a write only
# counts once two members have it. authSource is the service's own database: the user is defined
# there and can touch only it.
if [[ "${MONGO_HA}" == "1" ]]; then
  MONGO_HOSTS="mongo-0.mongo-hs.${NS}.svc.cluster.local:27017,mongo-1.mongo-hs.${NS}.svc.cluster.local:27017,mongo-2.mongo-hs.${NS}.svc.cluster.local:27017"
  MONGO_OPTIONS="&replicaSet=rs0&w=majority"
else
  MONGO_HOSTS="mongo.${NS}.svc.cluster.local:27017"
  MONGO_OPTIONS=""
fi
RESTART_SERVICES=0
for svc in producer consumer; do
  PASSWORD="$(secret_value "${svc}-mongo" PASSWORD)"
  [[ -n "${PASSWORD}" ]] || PASSWORD="$(random_password)"
  DB="eventconsole_${svc}"
  URI="mongodb://${svc}:${PASSWORD}@${MONGO_HOSTS}/${DB}?authSource=${DB}${MONGO_OPTIONS}"
  if [[ "$(secret_value "${svc}-mongo" MONGODB_URI)" != "${URI}" ]]; then
    kubectl -n "${NS}" create secret generic "${svc}-mongo" --from-literal=PASSWORD="${PASSWORD}" \
      --from-literal=MONGODB_URI="${URI}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    echo "Set Secret ${svc}-mongo (least-privilege user for database ${DB}, not printed)."
    RESTART_SERVICES=1
  fi
done

# The replica set's members authenticate to each other with a shared key file.
if [[ "${MONGO_HA}" == "1" ]] && ! kubectl -n "${NS}" get secret mongo-keyfile >/dev/null 2>&1; then
  kubectl -n "${NS}" create secret generic mongo-keyfile --from-literal=keyfile="$(openssl rand -base64 756 | tr -d '\n')" >/dev/null
  echo "Created Secret mongo-keyfile (replica-set key file, not printed)."
fi

# Who may use the UI and API. Two accounts: `viewer` can read, `operator` can also publish, requeue
# and clear. nginx checks them (HTTP Basic); the passwords are random, generated once and kept.
# They are never printed here -- run ./credentials.sh when you need them.
if ! kubectl -n "${NS}" get secret event-console-auth >/dev/null 2>&1; then
  VIEWER_PASSWORD="$(random_password)"
  OPERATOR_PASSWORD="$(random_password)"
  VIEWER_LINE="viewer:$(openssl passwd -apr1 "${VIEWER_PASSWORD}")"
  OPERATOR_LINE="operator:$(openssl passwd -apr1 "${OPERATOR_PASSWORD}")"
  # `viewers` lets BOTH accounts read; `operators` is only the operator.
  kubectl -n "${NS}" create secret generic event-console-auth \
    --from-literal=viewers.htpasswd="${VIEWER_LINE}"$'\n'"${OPERATOR_LINE}" \
    --from-literal=operators.htpasswd="${OPERATOR_LINE}" \
    --from-literal=VIEWER_PASSWORD="${VIEWER_PASSWORD}" \
    --from-literal=OPERATOR_PASSWORD="${OPERATOR_PASSWORD}" >/dev/null
  echo "Created Secret event-console-auth (accounts 'viewer' and 'operator', random passwords, not printed; see ./credentials.sh)."
fi

# Jobs are immutable: recreate them. Both are idempotent.
kubectl -n "${NS}" delete job mongo-users mongo-rs-init --ignore-not-found >/dev/null

# Exactly one MongoDB mode may exist: both would carry the label the Service and the
# NetworkPolicies select on. Remove the other mode's workloads (never its volumes).
if [[ "${MONGO_HA}" == "1" ]]; then
  kubectl -n "${NS}" delete deployment mongo --ignore-not-found >/dev/null
  MONGO_MANIFEST="mongo-ha.yaml"
else
  kubectl -n "${NS}" delete statefulset mongo --ignore-not-found >/dev/null
  kubectl -n "${NS}" delete poddisruptionbudget mongo --ignore-not-found >/dev/null
  kubectl -n "${NS}" delete service mongo-hs --ignore-not-found >/dev/null
  MONGO_MANIFEST="mongo-single.yaml"
fi
kubectl apply -f manifests.yaml -f "${MONGO_MANIFEST}"

if [[ "${SINGLE_REPLICA:-0}" == "1" ]]; then
  kubectl -n "${NS}" scale deployment event-console-producer event-console-consumer event-console-frontend --replicas=1 >/dev/null
fi
if [[ "${RESTART_SERVICES}" == "1" ]]; then
  # A changed Secret is not picked up by running pods.
  kubectl -n "${NS}" rollout restart deployment event-console-producer event-console-consumer >/dev/null
fi

# `rollout status`, not a wait on the pods' label: after a change to MongoDB's spec the OLD pod is still
# terminating, and a label wait would be pinned to it and time out.
if [[ "${MONGO_HA}" == "1" ]]; then
  kubectl -n "${NS}" rollout status statefulset/mongo --timeout=300s
  kubectl -n "${NS}" wait --for=condition=complete job/mongo-rs-init --timeout=300s
else
  kubectl -n "${NS}" rollout status deployment/mongo --timeout=300s
fi
kubectl -n "${NS}" wait --for=condition=complete job/mongo-users --timeout=300s

# The event contract: the registry must be up and hold the schema BEFORE the services take traffic.
# Registering is idempotent, and a schema that breaks the compatibility rule is refused here.
kubectl -n "${NS}" rollout status deployment/schema-registry --timeout=300s
bash contracts/register.sh
kubectl -n "${NS}" rollout status deployment/event-console-producer --timeout=300s
kubectl -n "${NS}" rollout status deployment/event-console-consumer --timeout=300s
kubectl -n "${NS}" rollout status deployment/event-console-frontend --timeout=180s

echo
echo "Event Console is up: http://localhost:8089   (logins: platform-k8s/event-console/credentials.sh)"
