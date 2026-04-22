# How To Run StreamBazaar And Verify Outputs

## 1) Start Services
```bash
cd /home/user/Downloads/StreamBazaar\ _\ IEEE\ TCC\ \(2\)/streamBazaar
docker compose up -d --build
./scripts/wait-for-services.sh
```

## 2) Initialize Topics and Tenant Metadata
```bash
python3 scripts/init-tenants.py
TENANT_IDS=tenant-fraud,tenant-clickstream,tenant-ml bash ./scripts/create-kafka-topics.sh
```

## 3) Run Configurable Workload (counts, rates, bytes)
```bash
python3 scripts/run_workloads.py \
  --datasets fraud,web-analytics,network-intrusion,iot-sensors \
  --tenant-ids tenant-fraud,tenant-web,tenant-intrusion,tenant-iot \
  --records-per-dataset fraud=50000,web-analytics=200000,network-intrusion=60000,iot-sensors=60000 \
  --input-rates fraud=120000,web-analytics=500000,network-intrusion=100000,iot-sensors=80000 \
  --criteo-subset-lines 500000 \
  --compress-time-window 12 \
  --payload-bytes-map fraud=256,web-analytics=1024,network-intrusion=512,iot-sensors=512 \
  --duration-sec 120
```

## 4) Export Full Metrics To CSV Every Second
```bash
python3 evaluation/export_prometheus_csv.py \
  --duration-sec 120 \
  --interval-sec 1 \
  --tenants tenant-fraud,tenant-clickstream,tenant-ml \
  --out-dir evaluation/results/csv
```

CSV output path:
- `evaluation/results/csv/prometheus_metrics_YYYYMMDD_HHMMSS.csv`

CSV includes:
- Throughput (system and per-tenant in/out/total)
- Latency p50/p90/p95/p99/p99.9 per tenant
- Migration downtime and transfer time (instant + total)
- Checkpoint CPU/memory/network utilization (cluster and per-tenant)
- Core KPIs (RUE, TLVR, EEI, FPP, MIS)
- Message rates and byte rates

## 5) Run Full Experiment Pipeline (Reports + Figures)
```bash
python3 evaluation/run_paper_experiments.py \
  --runs 2 \
  --warmup-sec 15 \
  --steady-sec 30 \
  --records-per-tenant 150 \
  --csv-monitor-sec 30
```

Generated outputs:
- `evaluation/results/raw/exp_YYYYMMDD_HHMMSS/run_XX_evaluation_report_*.json`
- `evaluation/results/raw/exp_YYYYMMDD_HHMMSS/summary.json`
- `evaluation/results/raw/exp_YYYYMMDD_HHMMSS/figures/*.png`
- `evaluation/results/raw/exp_YYYYMMDD_HHMMSS/figures_publication/*.png`
- `evaluation/results/raw/exp_YYYYMMDD_HHMMSS/csv/prometheus_metrics_*.csv`

## 6) Browser-Based Verification

### Prometheus
1. Open `http://localhost:19090`
2. Go to `Graph`
3. Run queries:
```promql
streambazaar_resource_utilization_efficiency{scope="cluster",tenant_id="all"}
streambazaar_tail_latency_violation_rate{scope="cluster",tenant_id="all"}
streambazaar_economic_efficiency_index
streambazaar_fairness_performance_product
streambazaar_migration_impact_score
sum(rate(streambazaar_messages_in_total[1m]))
sum(rate(streambazaar_message_bytes_in_total[1m]))
streambazaar_latency_p99_ms{tenant_id="tenant-fraud"}
streambazaar_checkpoint_cpu_utilization_percent{scope="cluster",tenant_id="all"}
```
4. Switch to `Table` mode to inspect numeric values.

### Grafana
1. Open `http://localhost:13000`
2. Login (default `admin/admin` unless changed)
3. Create dashboard panels with Prometheus datasource and these queries:
- KPI single stats: RUE, TLVR, EEI, FPP, MIS
- Time-series: message rates, byte rates, latency percentile series
- Time-series: checkpoint CPU/memory/network utilization
- Time-series/Bar: migration downtime and transfer time

## 7) Check Files Locally
```bash
ls -1 evaluation/results/csv | tail -5
ls -1 evaluation/results/raw/exp_*/figures_publication | tail -20
```

## 8) Publication-Ready Plot Generation From Existing CSV
```bash
python3 evaluation/analysis-scripts/plot_publication_metrics.py \
  --csv evaluation/results/csv/prometheus_metrics_YYYYMMDD_HHMMSS.csv \
  --fig-dir evaluation/results/figures_publication \
  --tenants tenant-fraud,tenant-clickstream,tenant-ml
```

This creates:
- KPI time-series line charts
- Traffic time-series line charts
- Checkpoint utilization line charts
- Latency time-series per tenant
- Latency box plots per tenant (p50/p90/p95/p99/p99.9)
- Throughput box plot across tenants
- Migration transfer/downtime box plots

## 9) Stop Stack
```bash
docker compose down
```
