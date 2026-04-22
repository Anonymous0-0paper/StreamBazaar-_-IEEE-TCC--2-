# 🚀 StreamBazaar Metrics Collection - Quick Start

## Option 1: Auto-Collect for 3 Minutes (EASIEST)

Just run this one command:

```bash
cd streamBazaar
bash scripts/collect_metrics_3min.sh
```

**What it does:**
- ✅ Starts workload automatically
- ✅ Runs for exactly 3 minutes (180 seconds)
- ✅ Collects benchmark data from 3 tenants
- ✅ Auto-exports all latency, resource, and throughput metrics to CSV
- ✅ Creates folder: `metrics_export_YYYYMMDD_HHMMSS/`

**Output:**
```
metrics_export_20260325_140000/
├── p50_latency.csv
├── p99_latency.csv          ← Most important SLA metric
├── p999_latency.csv
├── rue_efficiency.csv       ← Resource utilization
├── tlvr_violations.csv      ← SLA violation rate
├── system_throughput.csv
├── eei_index.csv
├── fpp_product.csv
└── ... (15+ more CSV files)
```

---

## Option 2: Continuous Collection (Until You Stop)

**Terminal 1: Start continuous collection**
```bash
python3 scripts/continuous_collector.py

# Collects every 30 seconds until Ctrl+C
# Saves to: metrics_live_YYYYMMDD_HHMMSS.csv
```

**Terminal 2: Start a workload (in parallel)**
```bash
python3 scripts/run_workloads.py \
  --datasets fraud-detection \
  --tenants tenant-fraud,tenant-clickstream,tenant-ml \
  --rate-per-sec 100000 \
  --duration-sec 600 \
  --records-per-tenant 1000
```

**Terminal 3: Watch live in Grafana (while running)**
```
http://localhost:13000
→ Dashboards → StreamBazaar Comprehensive Metrics Dashboard
```

**Stop collection:**
- Press `Ctrl+C` in Terminal 1
- Ctrl+C in Terminal 2 (or wait for duration)
- File automatically saved with final stats

---

## Option 3: Prometheus Browser Queries (Manual)

Open: `http://localhost:19090/graph`

### Most Important Queries

**P99 Latency** (the SLA metric)
```promql
streambazaar_latency_p99_ms
```

**System Throughput**
```promql
streambazaar_system_throughput_msgs_per_sec
```

**Resource Utilization (RUE)**
```promql
streambazaar_resource_utilization_efficiency{scope="cluster"}
```

**SLA Violations (TLVR)**
```promql
streambazaar_tail_latency_violation_rate
```

**Backlog** (queue building up?)
```promql
sum(streambazaar_tenant_backlog)
```

### Save as CSV from Browser

1. Enter query
2. Click "Table" tab
3. Right-click → "Copy"
4. Paste into Excel/Google Sheets

---

## Option 4: Grafana Dashboard (Visual)

**URL:** `http://localhost:13000`
**Username:** admin  
**Password:** admin

**Steps:**
1. Open link above
2. Click "Dashboards" 
3. Select "StreamBazaar Comprehensive Metrics Dashboard"
4. Watch 12 panels auto-update every 5 seconds

**Panels included:**
- RUE (Resource Utility Efficiency) - Gauge
- TLVR (Tail Latency Violations) - Gauge  
- EEI/FPP (Economic/Fairness) - Time-series
- MIS (Migration Impact) - Time-series
- Throughput - Line chart
- Per-tenant latencies - Multi-line
- Backlog - Area chart
- Bid prices - Line chart
- Resource utilization - Stacked area
- Clearing frequency - Bar chart

---

## Analyzing Your Data

### Load in Python
```python
import pandas as pd

# Load latency data
df = pd.read_csv('metrics_export_XXX/p99_latency.csv')

# Basic statistics
print(df['value'].describe())
# Output:
# count    36.0
# mean     125.3
# std      34.5
# min      89.2
# 25%     105.1
# 50%     118.5
# 75%     142.8
# max     198.4

# Peak latency
print(f"Peak P99: {df['value'].max():.1f} ms")
print(f"Avg P99: {df['value'].mean():.1f} ms")

# Plot
df.plot(x='timestamp', y='value', title='P99 Latency Over Time')
```

### Load in Excel
1. Open Excel
2. File → Import → Select CSV
3. Click Insert → Chart → Time-series
4. See latency trends

### Compare Metrics
```python
import pandas as pd

latency = pd.read_csv('p99_latency.csv')
throughput = pd.read_csv('system_throughput.csv')
rue = pd.read_csv('rue_efficiency.csv')

print("When throughput increases, does latency increase?")
print(f"Max throughput: {throughput['value'].max():.0f} msgs/sec")
print(f"Latency at that time: {latency['value'].loc[throughput['value'].idxmax()]:.1f} ms")
```

---

## Expected Values

### Healthy System (Low Load)
| Metric | Expected |
|--------|----------|
| P50 Latency | 20-50 ms |
| P99 Latency | 80-150 ms |
| P999 Latency | 150-300 ms |
| RUE | 20-40% |
| Throughput | 1-10 msgs/sec |
| TLVR | 0.0 (no violations) |
| Backlog | 0 |

### Moderate Load (100K msgs/sec)
| Metric | Expected |
|--------|----------|
| P50 Latency | 30-80 ms |
| P99 Latency | 100-200 ms |
| P999 Latency | 200-400 ms |
| RUE | 50-70% |
| Throughput | 100,000 msgs/sec |
| TLVR | 0.01-0.05 |
| Backlog | 0-100 |

### High Load (250K msgs/sec)
| Metric | Expected |
|--------|----------|
| P50 Latency | 80-150 ms |
| P99 Latency | 200-400 ms |
| P999 Latency | 400-800 ms |
| RUE | 80-95% |
| Throughput | 250,000 msgs/sec |
| TLVR | 0.15-0.35 |
| Backlog | 100-1000 |

---

## Troubleshooting

**No data in Prometheus?**
```bash
# Check if Prometheus is scraping targets
curl http://localhost:19090/api/v1/targets | jq '.data.activeTargets[].labels | {job, state}'

# Should show: state: "up"
```

**No data in Grafana?**
- Open http://localhost:13000 → Settings → Data Sources
- Test "Prometheus" connection
- Should show: "Success"

**Workload not running?**
```bash
# Check if broker is ready
docker compose logs kafka | tail -5

# Check if coordinator running
docker compose logs stream-coordinator | grep "listening"
```

**Metrics collecting slowly?**
- Prometheus scrapes every 5 seconds
- Grafana dashboard refreshes every 5 seconds
- Wait 30-60 seconds for stable trend

---

## Summary: 3 Quick Commands

```bash
# 1️⃣  Auto-collect for 3 minutes (EASIEST)
bash scripts/collect_metrics_3min.sh

# 2️⃣ Continuous collection (until Ctrl+C)
python3 scripts/continuous_collector.py &
python3 scripts/run_workloads.py --duration-sec 600 &

# 3️⃣ Watch live in browser
open http://localhost:13000
```

**Then:** Analyze CSVs in Excel or Python (see "Analyzing Your Data" section above)

---

## Complete Query Reference

See: `PROMETHEUS_QUERIES_GUIDE.md` for all 20+ queries you can use

Examples:
- Per-tenant metrics
- Advanced calculations (RUE = (CPU + Memory + Network) / 3)
- Multi-metric correlations
- Migration impact tracking

---

## Next Steps

1. ✅ Run one of the collection options above
2. ✅ Export CSV files
3. ✅ Load into Excel/Python for analysis
4. ✅ Check if metrics match expected values
5. ✅ Compare to paper baselines (see MONITORING.md)

**For detailed parameter tuning**, see: `PARAMETER_TUNING.md`

