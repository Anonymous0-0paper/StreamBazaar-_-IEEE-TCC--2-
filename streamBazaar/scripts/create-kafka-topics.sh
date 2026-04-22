#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONTAINER=${KAFKA_CONTAINER:-kafka}
BOOTSTRAP_SERVER=${BOOTSTRAP_SERVER:-kafka:9092}
PARTITIONS=${PARTITIONS:-3}
REPLICATION_FACTOR=${REPLICATION_FACTOR:-1}
TENANT_IDS=${TENANT_IDS:-tenant-fraud,tenant-clickstream,tenant-ml}
INPUT_TOPIC_TEMPLATE=${INPUT_TOPIC_TEMPLATE:-}
OUTPUT_TOPIC_TEMPLATE=${OUTPUT_TOPIC_TEMPLATE:-}
if [[ -z "${INPUT_TOPIC_TEMPLATE}" ]]; then
  INPUT_TOPIC_TEMPLATE='tenant.{tenant_id}.input'
fi
if [[ -z "${OUTPUT_TOPIC_TEMPLATE}" ]]; then
  OUTPUT_TOPIC_TEMPLATE='tenant.{tenant_id}.output'
fi
BIDS_TOPIC=${BIDS_TOPIC:-streamBazaar.bids}
ALLOC_TOPIC=${ALLOC_TOPIC:-streamBazaar.allocations}
PREEMPT_TOPIC=${PREEMPT_TOPIC:-streamBazaar.preemptions}
METRICS_TOPIC=${METRICS_TOPIC:-streamBazaar.metrics}

TOPICS=(
  "${BIDS_TOPIC}"
  "${ALLOC_TOPIC}"
  "${PREEMPT_TOPIC}"
  "${METRICS_TOPIC}"
)

IFS=',' read -r -a TENANT_ARRAY <<<"${TENANT_IDS}"
for tenant_id in "${TENANT_ARRAY[@]}"; do
  t="$(echo "${tenant_id}" | xargs)"
  [[ -z "${t}" ]] && continue
  input_topic="$(echo "${INPUT_TOPIC_TEMPLATE}" | sed "s/{tenant_id}/${t}/g")"
  output_topic="$(echo "${OUTPUT_TOPIC_TEMPLATE}" | sed "s/{tenant_id}/${t}/g")"
  TOPICS+=("${input_topic}" "${output_topic}")
done

for topic in "${TOPICS[@]}"; do
  docker compose exec -T "${KAFKA_CONTAINER}" kafka-topics \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION_FACTOR}" >/dev/null
  echo "topic ready: ${topic}"
done
