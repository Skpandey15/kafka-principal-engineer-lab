#!/usr/bin/env bash
# Shared helpers for the contract scripts. Source this file.
#
# The scripts talk to the Schema Registry by running curl INSIDE the registry pod
# (kubectl exec): nothing is exposed on the host, and the NetworkPolicy that limits the registry
# to the two services does not need an exception for them.

NS="${NS:-event-console}"
SUBJECT="${SUBJECT:-event-console-value}"
COMPATIBILITY="${COMPATIBILITY:-BACKWARD}"
SR_CONTENT_TYPE='Content-Type: application/vnd.schemaregistry.v1+json'

registry_curl() { # registry_curl <curl args...>   (reads a request body from stdin when given -d @-)
  kubectl -n "${NS}" exec -i deploy/schema-registry -- curl -s -w '\n%{http_code}' -H "${SR_CONTENT_TYPE}" "$@"
}

# Wraps a JSON Schema file as the registry's request body: {"schemaType":"JSON","schema":"<escaped>"}
schema_request_body() { # schema_request_body <file>
  local escaped
  escaped="$(tr -d '\r' < "$1" | tr '\n' ' ' | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g')"
  printf '{"schemaType":"JSON","schema":"%s"}' "${escaped}"
}

http_body() { sed '$d'; }          # all but the last line (the status code)
http_code() { tail -n 1; }
