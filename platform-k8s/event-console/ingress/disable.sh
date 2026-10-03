#!/usr/bin/env bash
# Removes the Event Console's ingress routes, the NetworkPolicy that admitted Traefik to MongoDB,
# and the MongoDB entrypoint from Traefik.
#
# The host port mappings (8090, 27017) stay on the k3d load balancer -- k3d cannot remove a
# mapping from a running cluster -- but with the routes gone nothing answers on them.

set -euo pipefail
cd "$(dirname "$0")"
source ../../_lib/common.sh

require_cluster
kubectl delete -f ingress.yaml --ignore-not-found
kubectl delete -f traefik-mongo-entrypoint.yaml --ignore-not-found
echo "Ingress routes removed. MongoDB is reachable only from inside the cluster again."
