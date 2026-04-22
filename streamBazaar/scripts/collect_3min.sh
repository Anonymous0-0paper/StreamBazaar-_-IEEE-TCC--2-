#!/bin/bash
PROMETHEUS_URL="http://localhost:19090"
OUTPUT_DIR="metrics_export_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTPUT_DIR"
echo "Starting collection. Output: $OUTPUT_DIR"
python3 scripts/run_workloads.py --datasets iot-sensors --tenant-ids tenant-iot --duration-sec 180 --records-per-tenant 1000 --disable-synthetic-fallback --skip-download &
wait $!
echo "Collection complete in $OUTPUT_DIR"
