#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "${ROOT_DIR}"

echo "[1/5] service health checks"
./scripts/wait-for-services.sh

echo "[2/5] API smoke calls"
curl -fsS -X POST http://localhost:18081/price -H 'Content-Type: application/json' \
  -d '{"tenant_id":"tenant-fraud","utilization":0.72,"sla_pressure":0.8}' >/dev/null
curl -fsS -X POST http://localhost:18080/bid -H 'Content-Type: application/json' \
  -d '{"tenant_id":"tenant-fraud","bid_price":1.37}' >/dev/null
curl -fsS -X POST http://localhost:18083/allocate -H 'Content-Type: application/json' \
  -d '{"tenant_id":"tenant-fraud","requested_slots":4}' >/dev/null
curl -fsS -X POST http://localhost:18084/migrate -H 'Content-Type: application/json' \
  -d '{"tenant_id":"tenant-fraud","source":"tm-1","target":"tm-2"}' >/dev/null

echo "[3/5] tenant initialization"
python3 scripts/init-tenants.py

echo "[4/5] E2E workload->Kafka stream"
./scripts/e2e_stream_test.sh

echo "[5/5] evaluation report smoke run"
python3 evaluation/run_evaluation.py --duration 0

echo "All smoke tests passed."
