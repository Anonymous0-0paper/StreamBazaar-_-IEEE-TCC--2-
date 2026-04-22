# 📊 StreamBazaar Metrics Collection - Complete Guide

## What You Now Have ✅

You now have **3 ways to collect and analyze all StreamBazaar metrics** for 3 minutes or continuously until you stop:

### File Summary
- ✅ **QUICK_START_METRICS.md** ← START HERE (5 min read)
- ✅ **PROMETHEUS_QUERIES_GUIDE.md** (Browser queries + CSV export)
- ✅ **scripts/collect_metrics_3min.sh** (Auto-collect script)
- ✅ **scripts/continuous_collector.py** (Continuous until Ctrl+C)
- ✅ **MONITORING.md** (50+ metrics reference)
- ✅ **PARAMETER_TUNING.md** (See effects of changes)
- ✅ **Grafana Dashboard** (12 panels, auto-refreshing)

---

## 🎯 What to Do RIGHT NOW

### Option A: Auto-Collect (3 Minutes) - EASIEST
```bash
bash scripts/collect_metrics_3min.sh
# ✓ Runs workload for 3 minutes
# ✓ Auto-saves all latency/throughput/resource data to CSV
# ✓ Done in: metrics_export_YYYYMMDD_HHMMSS/
```

### Option B: Continuous (Until You Stop)
```bash
# Terminal 1:
python3 scripts/continuous_collector.py
# Ctrl+C to stop, saves to metrics_live_YYYYMMDD_HHMMSS.csv

# Terminal 2:
python3 scripts/run_workloads.py --duration-sec 600
```

### Option C: Browser Prometheus Queries
```
http://localhost:19090/graph
→ Copy query below
→ Click "Table" tab
→ Copy data → Paste in Excel

Query: streambazaar_latency_p99_ms
```

### Option D: Grafana Live Dashboard
```
http://localhost:13000
Username: admin
Password: admin
→ Dashboards → StreamBazaar Comprehensive Metrics Dashboard
```

---

## 📈 Key Metrics to Track

| Metric | Query | What It Means | Target |
|--------|-------|--------------|--------|
| **P99 Latency** | `streambazaar_latency_p99_ms` | SLA metric (tail latency) | <200ms |
| **RUE** | `streambazaar_resource_utilization_efficiency` | Resource % being used | 60-80% |
| **TLVR** | `streambazaar_tail_latency_violation_rate` | % SLA violations | <0.10 |
| **Throughput** | `streambazaar_system_throughput_msgs_per_sec` | Messages/second | High |
| **Backlog** | `sum(streambazaar_tenant_backlog)` | Queue size (0 = good) | 0 |
| **EEI** | `streambazaar_economic_efficiency_index` | Allocation quality | 0.8-0.95 |
| **FPP** | `streambazaar_fairness_performance_product` | Fairness score | >0.7 |
| **MIS** | `streambazaar_migration_impact_score` | Migration impact | <0.5 |

---

## 📂 Output Files

When you run auto-collect, you get these CSVs:

```
metrics_export_20260325_140000/
├── p50_latency.csv              ← 50th percentile latency
├── p99_latency.csv              ← 99th percentile (important!)
├── p999_latency.csv             ← 99.9th percentile (tail)
├── rue_efficiency.csv           ← Resource utilization (RUE)
├── cpu_util.csv                 ← CPU percentage
├── memory_util.csv              ← Memory percentage
├── network_util.csv             ← Network percentage
├── tlvr_violations.csv          ← SLA violation rate (TLVR)
├── total_backlog.csv            ← Total queue backlog
├── eei_index.csv                ← Economic efficiency (EEI)
├── fpp_product.csv              ← Fairness-performance (FPP)
├── mis_score.csv                ← Migration impact (MIS)
├── system_throughput.csv        ← Messages/second
├── clearing_cycles.csv          ← Auction clearings
└── ... (2-3 more)
```

**Each CSV has format:**
```
tenant_id,timestamp,value
cluster,1711357200.5,125.3
cluster,1711357205.5,127.8
...
```

Load into Excel/Python for analysis.

---

## 🔍 Analyze Your Data

### Python Example
```python
import pandas as pd

# Load
df = pd.read_csv('metrics_export_XXX/p99_latency.csv')

# Summarize
print(df['value'].describe())
# count 36
# mean  125.3
# std   34.5

# Peak
print(f"Peak latency: {df['value'].max():.1f} ms")
print(f"Average latency: {df['value'].mean():.1f} ms")

# Chart
import matplotlib.pyplot as plt
plt.figure(figsize=(12,5))
plt.plot(df['timestamp'], df['value'])
plt.xlabel('Time')
plt.ylabel('Latency (ms)')
plt.title('P99 Latency During 3-Min Workload')
plt.show()
```

### Excel Example
1. Open `p99_latency.csv` in Excel
2. Insert → Column Chart → Time Series
3. See latency trend over 3 minutes

### Correlations
```python
# Do latency and throughput correlate?
latency = pd.read_csv('p99_latency.csv')
throughput = pd.read_csv('system_throughput.csv')

print(latency['value'].corr(throughput['value']))
# 0.85 = strong positive correlation
# Higher throughput → higher latency (expected)
```

---

## 📊 Expected Results

### 3-Minute Collection with 3 Tenants

| Metric | Light Load | Medium Load | High Load |
|--------|-----------|-------------|-----------|
| **P99 Latency** | 50-100ms | 100-200ms | 200-400ms |
| **RUE** | 20-40% | 50-70% | 80-95% |
| **TLVR** | 0.0 | 0.01-0.05 | 0.15-0.35 |
| **Throughput** | 1K msg/s | 100K msg/s | 250K msg/s |
| **Backlog** | 0 | 0-100 | 100-1000 |

### Paper Comparison

StreamBazaar improvements (vs baselines):
- **RUE**: +38% better than Flink-Default
- **TLVR**: 6.6× fewer SLA violations
- **Throughput**: 2.7× higher (470K vs 170K msg/s)
- **MIS**: 3.3× lower migration impact

Try these in your collection to match paper results.

---

## 🔄 Continuous Collection (Until You Stop)

If you want metrics **continuously collected while you run experiments**:

```bash
# Start collector (runs until Ctrl+C)
python3 scripts/continuous_collector.py &

# Start different workloads, one at a time
python3 scripts/run_workloads.py --datasets fraud-detection --duration-sec 300
python3 scripts/run_workloads.py --datasets web-analytics --duration-sec 300
python3 scripts/run_workloads.py --datasets clickstream-analytics --duration-sec 300

# Stop collector
# Ctrl+C in collector terminal

# Analyze single CSV with all data points
python3 << 'EOF'
import pandas as pd
df = pd.read_csv('metrics_live_YYYYMMDD_HHMMSS.csv')
print(f"Total points collected: {len(df)}")
print(f"Duration: {(df.iloc[-1]['timestamp'] - df.iloc[0]['timestamp']).total_seconds()} seconds")
print(f"\nP99 Latency Stats:")
print(df['p99_latency_ms'].describe())
EOF
```

---

## 🎛️ Adjust Parameters & See Effects

Want to see how changes affect metrics?

See `PARAMETER_TUNING.md` for detailed examples:

```bash
# Before collecting, try modifying docker-compose:
docker-compose.yml:
  environment:
    - CLUSTER_SLOTS=50        # More resources
    - DEFAULT_SLA_TARGET_MS=50 # Stricter SLA
    - CLEAR_INTERVAL_SEC=1.0   # Faster auctions

docker compose restart stream-coordinator

# Now collect and compare metrics
bash scripts/collect_metrics_3min.sh
# RUE will decrease (more idle capacity)
# TLVR might increase (stricter SLA)
# Clearing cycles will increase (faster auctions)
```

---

## 🚨 Common Issues

| Problem | Fix |
|---------|-----|
| Prometheus returns no data | Check: http://localhost:19090/targets (should show UP) |
| Grafana shows "No Data" | Click settings → Datasources → Test Prometheus connection |
| Collection script fails | Ensure workload can run: `python3 scripts/run_workloads.py --help` |
| CSV is empty | Wait 30s for Prometheus to collect data |
| Very large CSV | Use Python to downsample: `df.iloc[::2]` (every 2nd row) |

---

## 📚 Reference Files

**For quick answers:**
- `QUICK_START_METRICS.md` ← 4 options to collect data

**For all possible queries:**
- `PROMETHEUS_QUERIES_GUIDE.md` ← 20+ queries

**For metric explanations:**
- `MONITORING.md` ← What each metric means

**For parameter effects:**
- `PARAMETER_TUNING.md` ← How to tune system

**For implementation details:**
- `METRICS_IMPLEMENTATION_SUMMARY.md` ← Architecture

---

## ⚡ TL;DR - Do This Now

```bash
# 1. Run 3-minute auto-collection
bash scripts/collect_metrics_3min.sh

# 2. Wait for it to finish (creates metrics_export_XXX folder)

# 3. Open Grafana to see live dashboard
open http://localhost:13000

# 4. Analyze CSV files in Python/Excel
python3 << 'EOF'
import pandas as pd
df = pd.read_csv('metrics_export_XXX/p99_latency.csv')
print(df.describe())
EOF
```

**Done!** You now have all metrics saved to CSV + visible in Grafana.

