#!/bin/bash
# StreamBazaar Metrics Collection - 3 Minutes with Auto-Save

set -e

PROMETHEUS_URL="http://localhost:19090"
OUTPUT_DIR="metrics_export_$(date +%Y%m%d_%H%M%S)"
DURATION_SEC=${1:-180}  # 3 minutes default

mkdir -p "$OUTPUT_DIR"

echo "=========================================="
echo "StreamBazaar 3-Min Metrics Collection"
echo "=========================================="
echo "Duration: $DURATION_SEC seconds"
echo "Output: $OUTPUT_DIR"
echo ""

# Export metric time-series to CSV
export_metric() {
  local query=$1
  local filename=$2
  
  echo "  → Exporting $filename..."
  
  curl -s -G "$PROMETHEUS_URL/api/v1/query_range" \
    --data-urlencode "query=$query" \
    --data-urlencode "start=$(date -u -d '5 minutes ago' +%Y-%m-%dT%H:%M:%SZ)" \
    --data-urlencode "end=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --data-urlencode "step=5s" | \
    jq -r '.data.result[] | .metric.tenant_id as $t | .values[] | "\($t // "cluster"),\(.[0]),\(.[1])"' \
    > "$OUTPUT_DIR/$filename.csv" 2>/dev/null || echo "N/A"
  
  if [ -s "$OUTPUT_DIR/$filename.csv" ]; then
    sed -i '1i tenant_id,timestamp,value' "$OUTPUT_DIR/$filename.csv"
    LINES=$(wc -l < "$OUTPUT_DIR/$filename.csv")
    echo "     ✓ $filename.csv ($LINES rows)"
  fi
}

echo "Step 1: Starting 3-minute workload..."
python3 scripts/run_workloads.py \
  --datasets iot-sensors \
  --tenant-ids tenant-iot \
  --duration-sec $DURATION_SEC \
  --records-per-tenant 1000 \
  --disable-synthetic-fallback \
  --skip-download &

WORKLOAD_PID=$!
echo "Workload PID: $WORKLOAD_PID"
echo ""

# Wait for workload
wait $WORKLOAD_PID 2>/dev/null || true

echo ""
echo "Step 2: Exporting metrics..."
echo ""

sleep 3

echo "LATENCY METRICS:"
export_metric 'streambazaar_latency_p50_ms' 'p50_latency'
export_metric 'streambazaar_latency_p90_ms' 'p90_latency'
export_metric 'streambazaar_latency_p95_ms' 'p95_latency'
export_metric 'streambazaar_latency_p99_ms' 'p99_latency'
export_metric 'streambazaar_latency_p999_ms' 'p999_latency'
echo ""

echo "RESOURCE METRICS:"
export_metric 'streambazaar_resource_utilization_efficiency{scope="cluster"}' 'rue_efficiency'
export_metric 'streambazaar_checkpoint_cpu_utilization_percent{scope="cluster"}' 'cpu_util'
export_metric 'streambazaar_checkpoint_memory_utilization_percent{scope="cluster"}' 'memory_util'
export_metric 'streambazaar_checkpoint_network_utilization_percent{scope="cluster"}' 'network_util'
echo ""

echo "SLA METRICS:"
export_metric 'streambazaar_tail_latency_violation_rate' 'tlvr_violations'
export_metric 'sum(streambazaar_tenant_backlog)' 'total_backlog'
echo ""

echo "ECONOMIC METRICS:"
export_metric 'streambazaar_economic_efficiency_index' 'eei_index'
export_metric 'streambazaar_fairness_performance_product' 'fpp_product'
export_metric 'streambazaar_migration_impact_score' 'mis_score'
echo ""

echo "THROUGHPUT & AUCTION:"
export_metric 'streambazaar_system_throughput_msgs_per_sec' 'system_throughput'
export_metric 'streambazaar_clearing_cycles_total' 'clearing_cycles'
echo ""

echo "=========================================="
echo "✅ COLLECTION COMPLETE"
echo "=========================================="
echo "Output directory: $OUTPUT_DIR"
echo ""
echo "Files created:"
ls -lh "$OUTPUT_DIR"/*.csv | awk '{print "  " $9 " (" $5 ")"}'
echo ""
echo "Next steps:"
echo "  1. Import CSVs into Excel/Python"
echo "  2. Analyze with pandas:"
echo "     python3 << 'EOF'"
echo "     import pandas as pd"
echo "     df = pd.read_csv('$OUTPUT_DIR/p99_latency.csv')"
echo "     print(df.describe())"
echo "     EOF"
echo ""

