#!/usr/bin/env bash
set -euo pipefail

# Deploy core StreamBazaar stack
docker compose up -d --build

# Wait for control-plane services
./scripts/wait-for-services.sh

echo "StreamBazaar control plane is running."
echo "Start evaluation with: python evaluation/run_evaluation.py --duration 120"
