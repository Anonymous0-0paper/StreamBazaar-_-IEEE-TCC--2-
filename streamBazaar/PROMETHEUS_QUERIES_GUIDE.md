## Prometheus & Grafana Queries - Browser Guide

### **Quick Access URLs**

**Prometheus (Direct Queries)**:
```
http://localhost:19090/graph
```

**Grafana (Visual Dashboards)**:
```
http://localhost:13000
Username: admin
Password: admin
→ Dashboard: StreamBazaar Comprehensive Metrics Dashboard
```

---

## Part 1: Running 3-Minute Workload with Auto-Metrics Collection

### **Option A: Auto-Collect Script (EASIEST)**

```bash
# This runs workload for 3 minutes and automatically saves all metrics to CSV
cd streamBazaar
bash scripts/collect_metrics_3min.sh

# Output: Creates metrics_export_YYYYMMDD_HHMMSS/ folder with all CSV files
```

### **Option B: Manual Run - Continuous (Until You Stop with Ctrl+C)**

```bash
# Terminal 1: Run continuous workload
cd streamBazaar
python3 scripts/run_workloads.py \
  --datasets fraud-detection \
  --tenants tenant-fraud,tenant-clickstream,tenant-ml \
  --rate-per-sec 100000 \
  --records-per-tenant 1000

# Terminal 2: Continuous metrics collection (see Python script below)
python3 scripts/continuous_metrics_collector.py
```

---

## Part 2: Browser Queries to See & Save Data

### Step 1: Open Prometheus Query Interface
Go to: `http://localhost:19090/graph`

### Step 2: Copy-Paste Queries Below

#### **KEY LATENCY METRICS** 📊

**1. P99 Latency** (Most Important SLA)
```promql
streambazaar_latency_p99_ms
```

**2. P99.9 Latency** (Tail SLA)
```promql
streambazaar_latency_p999_ms
```

**3. All Percentiles**
```promql
{__name__=~"streambazaar_latency_p(50|90|95|99|999)_ms"}
```

**4. Per-Tenant P99**
```promql
streambazaar_latency_p99_ms{tenant_id!=""}
```

---

#### **RESOURCE UTILIZATION** 💻

**5. RUE - Resource Efficiency**
```promql
streambazaar_resource_utilization_efficiency
```
Expected: 30-80% during workload

**6. CPU Utilization**
```promql
streambazaar_checkpoint_cpu_utilization_percent
```

**7. Memory Utilization**
```promql
streambazaar_checkpoint_memory_utilization_percent
```

**8. Network Utilization**
```promql
streambazaar_checkpoint_network_utilization_percent
```

---

#### **SLA & BACKLOG** ⚠️

**9. TLVR - SLA Violation Rate** (0-1)
```promql
streambazaar_tail_latency_violation_rate
```
0.0 = Perfect | 0.05 = Good | 0.2+ = Bad

**10. Per-Tenant Backlog**
```promql
streambazaar_tenant_backlog
```
Expected: 0 (keeping up)

**11. Per-Tenant P99 Latency**
```promql
streambazaar_tenant_p99_latency_ms
```

---

#### **THROUGHPUT** 📈

**12. System Throughput** (all tenants)
```promql
streambazaar_system_throughput_msgs_per_sec
```

**13. Per-Tenant Throughput**
```promql
streambazaar_throughput_msgs_per_sec
```

---

#### **ECONOMIC METRICS** 💰

**14. EEI - Efficiency Index** (0-1)
```promql
streambazaar_economic_efficiency_index
```
1.0 = Perfect | 0.8+ = Good

**15. FPP - Fairness Product** (0-1)
```promql
streambazaar_fairness_performance_product
```

**16. MIS - Migration Impact**
```promql
streambazaar_migration_impact_score
```

---

## Part 3: Export as CSV

### **Method 1: From Prometheus Table**

1. Enter query in browser
2. Click "Table" tab
3. Right-click → Copy all
4. Paste into Excel

### **Method 2: Export Time-Range Query**

Use this URL (replace query and times):
```
http://localhost:19090/api/v1/query_range?query=streambazaar_latency_p99_ms&start=2026-03-25T10:00:00Z&end=2026-03-25T10:30:00Z&step=5s
```

Save JSON response and convert to CSV with Python:
```python
import json, csv
with open('prometheus.json') as f:
    data = json.load(f)
with open('output.csv', 'w') as out:
    w = csv.writer(out)
    w.writerow(['timestamp', 'value', 'tenant'])
    for result in data['data']['result']:
        tenant = result['metric'].get('tenant_id', 'cluster')
        for ts, val in result['values']:
            w.writerow([ts, val, tenant])
```

### **Method 3: Grafana Export**

1. Open: `http://localhost:13000/d/streamBazaar`
2. Click **⋮** on any panel
3. Select **"Inspect"** → **"Data"**
4. Click **"Download as CSV"**

---

## Part 4: Continuous Collection Scripts

### **Bash Script** (Runs 3 min or until Ctrl+C)

```bash
#!/bin/bash
OUTPUT="metrics_$(date +%s).csv"
echo "timestamp,p50,p99,p999,rue,throughput,backlog" > $OUTPUT

while true; do
  TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  P99=$(curl -s "http://localhost:19090/api/v1/query?query=streambazaar_latency_p99_ms" | jq '.data.result[0].value[1]' 2>/dev/null)
  RUE=$(curl -s "http://localhost:19090/api/v1/query?query=streambazaar_resource_utilization_efficiency{scope=%22cluster%22}" | jq '.data.result[0].value[1]' 2>/dev/null)
  echo "$TS,$P99,$RUE" >> $OUTPUT
  sleep 30
done
```

### **Python Script** (Continuous, all metrics)

```bash
# Run:
python3 scripts/continuous_metrics_collector.py

# Collects every 30 seconds until Ctrl+C
# Saves to: metrics_continuous_YYYYMMDD_HHMMSS.csv
```

---

## Part 5: Real Example Workflow

```bash
# Terminal 1: Start 3-minute auto-collection
bash scripts/collect_metrics_3min.sh

# While that runs...
# Terminal 2: Open Grafana dashboard
# http://localhost:13000 → StreamBazaar Dashboard
# Watch all panels update in real-time

# After 3 minutes: Analyze exported data
cd metrics_export_*/
python3 << 'EOF'
import pandas as pd
df = pd.read_csv('latency_p99.csv')
print(df.describe())
