#!/usr/bin/env bash
set -euo pipefail

services=(
  "http://localhost:18080/health"
  "http://localhost:18081/health"
  "http://localhost:18082/health"
  "http://localhost:18083/health"
  "http://localhost:18084/health"
  "http://localhost:18085/health"
)

for endpoint in "${services[@]}"; do
  echo "Waiting for ${endpoint}"
  until curl -fsS "${endpoint}" >/dev/null 2>&1; do
    sleep 2
  done
  echo "Ready: ${endpoint}"
done
