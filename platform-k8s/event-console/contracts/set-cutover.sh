#!/usr/bin/env bash
# Records where the topic's LEGACY history ends, so that records written before the contract existed
# (they carry no x-schema-id header) are excused, and every record after that point must carry one.
#
#   ./set-cutover.sh                          cut over at the topic's CURRENT end offsets (per partition)
#   ./set-cutover.sh --first-contract-record  cut over at the first record that already carries x-schema-id
#   ./set-cutover.sh --show                   print what is configured now and change nothing
#   ./set-cutover.sh --force ...              replace an existing cut-over
#
# Why a per-partition OFFSET and not a timestamp or a header: the broker assigns offsets, so no producer can
# forge one. A header or a timestamp can be set by whoever writes the record, which would let a record skip
# validation by claiming to be old. Offsets only ever grow, so "below N" cannot be reached by new traffic.
#
# The cut-over is deliberately write-once: moving it UP would excuse records that were never legacy, so the
# script refuses to change an existing one without --force. The result is the ConfigMap `event-console-cutover`,
# mounted into the consumers as an optional config file; the consumers are restarted to pick it up.
set -euo pipefail

NS="${NS:-event-console}"
TOPIC="${TOPIC:-event-console}"
KAFKA_NS="${KAFKA_NS:-kafka}"
KAFKA_BIN="/opt/kafka/bin"
CM="event-console-cutover"
MODE="end"
FORCE=0

for arg in "$@"; do
  case "${arg}" in
    --first-contract-record) MODE="first" ;;
    --show)                  MODE="show" ;;
    --force)                 FORCE=1 ;;
    *) echo "unknown argument: ${arg}" >&2; exit 64 ;;
  esac
done

kafka_exec() { kubectl -n "${KAFKA_NS}" exec -i deploy/kafka -- "$@"; }

current() { kubectl -n "${NS}" get cm "${CM}" -o jsonpath='{.data.cutover\.yml}' 2>/dev/null || true; }

if [[ "${MODE}" == "show" ]]; then
  yml="$(current)"
  if [[ -z "${yml}" ]]; then
    echo "No cut-over configured: nothing is legacy, the contract covers the whole topic."
  else
    echo "${yml}"
  fi
  exit 0
fi

existing="$(current)"
if [[ -n "${existing}" && "${FORCE}" != "1" ]]; then
  echo "A cut-over is already configured (the topic's legacy history was already defined):" >&2
  echo "${existing}" | sed 's/^/  /' >&2
  echo "Refusing to change it: raising it would excuse records that were never legacy. Use --force if you mean it." >&2
  exit 1
fi

declare -A LIMIT=()

if [[ "${MODE}" == "end" ]]; then
  # kafka-get-offsets prints topic:partition:offset -- the offset the NEXT record will get.
  while IFS=: read -r _ partition offset; do
    [[ -n "${partition}" ]] && LIMIT["${partition}"]="${offset}"
  done < <(kafka_exec "${KAFKA_BIN}/kafka-get-offsets.sh" --bootstrap-server localhost:9092 --topic "${TOPIC}" --time latest | tr -d '\r')
else
  # Scan every partition from the start for the first record that has the header; everything below it is legacy.
  # A partition with no such record yet takes its end offset (all of it is legacy).
  while IFS=: read -r _ partition offset; do
    [[ -n "${partition}" ]] && LIMIT["${partition}"]="${offset}"
  done < <(kafka_exec "${KAFKA_BIN}/kafka-get-offsets.sh" --bootstrap-server localhost:9092 --topic "${TOPIC}" --time latest | tr -d '\r')
  for partition in "${!LIMIT[@]}"; do
    end="${LIMIT[${partition}]}"
    [[ "${end}" == "0" ]] && continue
    first="$(kafka_exec "${KAFKA_BIN}/kafka-console-consumer.sh" --bootstrap-server localhost:9092 --topic "${TOPIC}" \
      --partition "${partition}" --offset earliest --timeout-ms 8000 \
      --property print.offset=true --property print.headers=true --property print.value=false --property print.key=false \
      2>/dev/null | tr -d '\r' | awk '/x-schema-id/ { sub(/^.*Offset:/, ""); print $1; exit }' || true)"
    [[ -n "${first}" ]] && LIMIT["${partition}"]="${first}"
  done
fi

if [[ "${#LIMIT[@]}" -eq 0 ]]; then
  echo "Could not read the topic's offsets; nothing changed." >&2
  exit 1
fi

yml="app:
  schema:
    legacy-until-offsets:"
for partition in $(printf '%s\n' "${!LIMIT[@]}" | sort -n); do
  yml+="
      ${partition}: ${LIMIT[${partition}]}"
done

echo "Cut-over (records below these offsets are legacy and excused from the contract):"
echo "${yml}" | sed 's/^/  /'

kubectl -n "${NS}" create configmap "${CM}" --from-literal="cutover.yml=${yml}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "Restarting the consumers to pick it up..."
kubectl -n "${NS}" rollout restart deploy/event-console-consumer >/dev/null
kubectl -n "${NS}" rollout status deploy/event-console-consumer --timeout=240s
echo "Done. Records at or above these offsets must carry x-schema-id and pass the contract."
