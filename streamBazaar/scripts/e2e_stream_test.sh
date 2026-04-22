#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "${ROOT_DIR}"

./scripts/wait-for-services.sh
./scripts/create-kafka-topics.sh

python3 scripts/run_workloads.py --duration-sec 15 --records-per-tenant 20

echo "--- topic offset check ---"
for topic in streamBazaar.bids streamBazaar.allocations streamBazaar.preemptions streamBazaar.metrics tenant.tenant-fraud.input tenant.tenant-clickstream.input tenant.tenant-ml.input; do
  echo "${topic}:"
  docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list kafka:9092 \
    --topic "${topic}" \
    --time -1 | cat
  echo

done

alloc_total=$(docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic streamBazaar.allocations --time -1 | awk -F: '{sum += $3} END {print sum+0}')
metrics_total=$(docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic streamBazaar.metrics --time -1 | awk -F: '{sum += $3} END {print sum+0}')

if [[ "${alloc_total}" -le 0 ]]; then
  echo "ERROR: streamBazaar.allocations has no events; coordinator loop is not producing allocation decisions." >&2
  exit 1
fi

if [[ "${metrics_total}" -le 0 ]]; then
  echo "ERROR: streamBazaar.metrics has no events; coordinator loop is not producing control metrics." >&2
  exit 1
fi

echo "--- coordinator health ---"
curl -fsS http://localhost:18085/health | cat
echo

echo "--- sample messages from streamBazaar.bids ---"
docker compose exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic streamBazaar.bids \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 5000 | cat

echo "--- sample messages from streamBazaar.allocations ---"
docker compose exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic streamBazaar.allocations \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 5000 | cat

echo "E2E stream test completed successfully."
