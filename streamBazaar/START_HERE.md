# 🎯 START HERE - Metrics Collection for 3 Minutes (or Until You Stop)

## What You Have

You now have **everything you need to collect, store, and analyze metrics** for StreamBazaar.

## ✅ What's Ready

```
✓ 3 collection scripts
✓ 4 guide documents  
✓ Prometheus metrics (50+ available)
✓ Grafana dashboard (12 live panels)
✓ All queries pre-made for browser
```

---

## 🚀 Quick Start (5 seconds)

### **FASTEST: Auto-collect for 3 minutes**

```bash
bash scripts/collect_metrics_3min.sh
```

**That's it!**

This will:
1. ✅ Start workload automatically (fraud-detection, 3 tenants)
2. ✅ Run for exactly 3 minutes
3. ✅ Collect ALL metrics automatically
4. ✅ Save 15+ CSV files to folder: `metrics_export_YYYYMMDD_HHMMSS/`
5. ✅ Done. Files ready for Excel/Python analysis.

---

## Alternative 1: Continuous Collection (Runs Until You Stop)

```bash
# Terminal 1: Start continuous collector
python3 scripts/continuous_collector.py

# Terminal 2: Start workload (in parallel)
python3 scripts/run_workloads.py --duration-sec 600

# Stop whenever you want: Ctrl+C in Terminal 1
# File saved automatically with all data points
```

---

## Alternative 2: Browser Queries (Manual)

**Prometheus:**
```
http://localhost:19090/graph
Copy → Paste query → Click "Table" → Copy → Excel
```

**Sample query:**
```promql
streambazaar_latency_p99_ms  # Most important SLA metric
```

**Grafana (Visual):**
```
http://localhost:13000
→ Dashboards → StreamBazaar Comprehensive Metrics Dashboard
→ Watch 12 panels update live
```

---

## 📊 What You'll Get

### Auto-Collect Output (Option 1)
Folder: `metrics_export_20260325_140000/` containing:

| File | What It Shows |
|------|---------------|
| `p50_latency.csv` | Median latency |
| `p99_latency.csv` | **SLA metric** (most important) |
| `p999_latency.csv` | Tail latency |
| `rue_efficiency.csv` | Resource utilization |
| `tlvr_violations.csv` | SLA violation rate |
| `system_throughput.csv` | Messages/second |
| `eei_index.csv` | Economic efficiency |
| `fpp_product.csv` | Fairness score |
| `mis_score.csv` | Migration impact |
| `total_backlog.csv` | Queue backlog |
| `cpu_util.csv` | CPU % |
| `memory_util.csv` | Memory % |
| `clearing_cycles.csv` | Auction executions |
| + 2-3 more | Additional metrics |

**Each CSV format:**
```
tenant_id,timestamp,value
cluster,1711357200.5,125.3
cluster,1711357205.5,127.8
```

### Continuous Collection Output (Option 2)
Single file: `metrics_live_YYYYMMDD_HHMMSS.csv` with columns:
```
timestamp, p50_latency_ms, p99_latency_ms, p999_latency_ms, 
rue_percent, cpu_percent, memory_percent, network_percent,
tlvr, throughput, backlog, eei, fpp, mis
```

All rows collected every 30 seconds until you stop.

---

## 📈 Analyze Your Data

### Python (2 lines)
```python
import pandas as pd
df = pd.read_csv('metrics_export_XXX/p99_latency.csv')
print(df['value'].describe())  # min/max/avg/std
```

### Excel
1. File → Open → Select CSV
2. Insert → Chart → Time-series
3. See trends instantly

### Expected Values (3-min run with 3 tenants)
| Metric | Expected |
|--------|----------|
| P99 Latency | 80-200 ms |
| RUE | 30-70% |
| Throughput | 1-100K msg/sec |
| TLVR | 0.0-0.05 |
| Backlog | 0-50 messages |

---

## 🔄 Continuous Collection Details

If you want to collect while doing **different experiments**:

```bash
# Terminal 1: Start collector (until Ctrl+C)
python3 scripts/continuous_collector.py
# Collecting to: metrics_live_20260325_140000.csv

# Terminal 2: Run various workloads (one at a time)
python3 scripts/run_workloads.py --datasets fraud-detection --duration-sec 180
python3 scripts/run_workloads.py --datasets web-analytics --duration-sec 180 
python3 scripts/run_workloads.py --datasets clickstream-analytics --duration-sec 180

# When done: Ctrl+C in Terminal 1
# Single CSV has all 3 workloads' metrics mixed together
# Use timestamp column to separate them
```

---

## 📍 All Browser Access Points

| URL | Purpose |
|-----|---------|
| http://localhost:19090/graph | Query metrics manually |
| http://localhost:13000 | Grafana dashboards (admin/admin) |
| http://localhost:18088 | Flink UI (job details) |

---

## 📚 Documentation Index

| File | What For |
|------|----------|
| **QUICK_START_METRICS.md** | Overview of all 4 options |
| **PROMETHEUS_QUERIES_GUIDE.md** | All 20+ queries to paste in browser |
| **MONITORING.md** | Complete metric reference (50+) |
| **PARAMETER_TUNING.md** | How to change system and see effects |
| **METRICS_IMPLEMENTATION_SUMMARY.md** | Architecture details |

---

## ⚡ Choose Your Path

### 👶 Absolute Beginners
```bash
bash scripts/collect_metrics_3min.sh
# Done. CSVs in metrics_export_XXX/
# Open in Excel, see trends
```

### 🎯 Want to Understand Metrics
```
1. Read: QUICK_START_METRICS.md (5 min)
2. Run: bash scripts/collect_metrics_3min.sh
3. Analyze: Python/Excel analysis (10 min)
```

### 🔬 Researchers/Parameter Tuning
```
1. Read: PARAMETER_TUNING.md
2. Modify: docker-compose.yml
3. Collect: bash scripts/collect_metrics_3min.sh
4. Compare: Metrics before/after
```

### 🚀 Advanced / Continuous Monitoring
```
1. python3 scripts/continuous_collector.py
2. Run different workloads in parallel
3. All data in single continuous CSV
4. Analyze with Python (correlation, trends)
```

---

## 🔧 System Status Check

Before collecting, verify everything is running:

```bash
# Should show 14 containers "Up"
docker compose ps

# Should show 6 "UP" targets
curl http://localhost:19090/api/v1/targets | jq '.data.activeTargets[].labels.job'

# Should return a number (like 24)
curl http://localhost:19090/api/v1/query?query=streambazaar_clearing_cycles_total | jq '.data.result[0].value[1]'
```

---

## 💡 Pro Tips

**Tip 1: Fast Analysis**
```bash
for csv in metrics_export_*/p*_latency.csv; do
  echo "$csv:"
  tail -n +2 $csv | awk -F',' '{print $3}' | sort -n | awk 'BEGIN{c=0} {a[c++]=$1} END{print "Min:"a[0], "Max:"a[c-1], "Avg:"(a[int(c/2)])}'
done
```

**Tip 2: Watch live during collection**
```bash
# In separate terminal
watch -n 1 'curl -s "http://localhost:19090/api/v1/query?query=streambazaar_latency_p99_ms" | jq ".data.result[0].value[1]"'
# Updates every 1 second
```

**Tip 3: Compare before/after**
```bash
# Run 1st experiment
bash scripts/collect_metrics_3min.sh
mv metrics_export_* exp1_metrics/

# Change parameter in docker-compose.yml
# Run 2nd experiment  
bash scripts/collect_metrics_3min.sh
mv metrics_export_* exp2_metrics/

# Compare: exp1_metrics/p99_latency.csv vs exp2_metrics/p99_latency.csv
```

---

## 🎬 Ready to Start?

```bash
# Copy and paste this:
bash scripts/collect_metrics_3min.sh

# Wait 3 minutes for completion
# Check for: metrics_export_YYYYMMDD_HHMMSS/ folder
# Open CSVs in Excel or Python
# Done!
```

**Questions?** See the appropriate guide file:
- Quick answers → `QUICK_START_METRICS.md`
- All queries → `PROMETHEUS_QUERIES_GUIDE.md`
- Metric meanings → `MONITORING.md`
- Parameter effects → `PARAMETER_TUNING.md`

