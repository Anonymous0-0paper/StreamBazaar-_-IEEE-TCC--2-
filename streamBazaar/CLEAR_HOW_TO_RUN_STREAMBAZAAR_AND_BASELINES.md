# Clear How To Run StreamBazaar And Baselines

This is a step-by-step guide to:
- run StreamBazaar with Docker Compose,
- verify health and Kafka topics,
- send workload by dataset type,
- export all metrics to CSV,
- visualize and compare StreamBazaar vs TALOS/DS2/CAPSys/Flink Default,
- switch scheduler modes at runtime,
- and change configuration safely.

## ⚠️ Important: Latency Metrics Bug Fix

**Latency metrics now correctly use milliseconds (ms), not nanoseconds (ns).**

- Previous bug: Latencies displayed as 9,000,000+ ms (unrealistic)
- Fixed now: Latencies display as 9-10 ms (realistic)
- All CSV exports use corrected values
- Use `true_measured_improvement_report_FIXED.txt` for publication

For more details, see [BUG_FIX_REPORT.md](BUG_FIX_REPORT.md).

## 0) Prerequisites

- Docker + Docker Compose installed.
- Python 3 available.
- Working directory:

```bash
cd "/home/user/Downloads/StreamBazaar _ IEEE TCC (2)/streamBazaar"
```

## 1) Start Full Stack

```bash
docker compose up -d --build
./scripts/wait-for-services.sh
```

Expected: all core services become healthy.

## 2) Health Check

### 2.1 Docker services

```bash
docker compose ps
```

Expected: `Up` for services like `kafka`, `prometheus`, `grafana`, `stream-coordinator`, `flink-*`.

### 2.2 API health endpoints

```bash
curl -fsS http://localhost:18080/health && echo
curl -fsS http://localhost:18081/health && echo
curl -fsS http://localhost:18082/health && echo
curl -fsS http://localhost:18083/health && echo
curl -fsS http://localhost:18084/health && echo
curl -fsS http://localhost:18085/health && echo
```

### 2.3 Prometheus target health

```bash
curl -s http://localhost:19090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health: .health}'
```

Expected: `health: "up"` for active targets.

## 3) Initialize Tenants And Kafka Topics

```bash
python3 scripts/init-tenants.py
TENANT_IDS=tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot bash ./scripts/create-kafka-topics.sh
```

## 4) Check Kafka Topics

### 4.1 List topics

```bash
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list
```

### 4.2 Describe one topic

```bash
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic tenant.tenant-iot.input
```

### 4.3 Verify message flow (optional)

```bash
docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic tenant.tenant-iot.input --time -1
```

If offsets increase over time, data is being produced.

## 5) Send Workload By Dataset Type

## Important real-data note

- To force real data only (no synthetic fallback), always include:
  - `--disable-synthetic-fallback`
  - `--skip-download` (use local dataset files)
- In current environment, `iot-sensors` is the validated real dataset.

### 5.1 Real-data workload (IoT sensors)

```bash
python3 scripts/run_workloads.py \
  --datasets iot-sensors \
  --tenant-ids tenant-iot \
  --records-per-tenant 50000 \
  --input-rate 100000 \
  --duration-sec 180 \
  --disable-synthetic-fallback \
  --skip-download
```

### 5.2 Multi-dataset workload (requires those datasets locally available)

```bash
python3 scripts/run_workloads.py \
  --datasets fraud,web-analytics,network-intrusion,iot-sensors \
  --tenant-ids tenant-fraud,tenant-web,tenant-intrusion,tenant-iot \
  --records-per-dataset fraud=50000,web-analytics=200000,network-intrusion=60000,iot-sensors=60000 \
  --input-rates fraud=120000,web-analytics=500000,network-intrusion=100000,iot-sensors=80000 \
  --duration-sec 120
```

### 5.3 Dry-run validation before sending data

```bash
python3 scripts/run_workloads.py \
  --datasets iot-sensors \
  --tenant-ids tenant-iot \
  --disable-synthetic-fallback \
  --skip-download \
  --dry-run
```

### 5.4 Real Application Pipelines (Strict Operator Counts)

The following pipelines are implemented in `datasets/workload_generators/*.py` and consume real files from `datasets/*` through `datasets/dataset_loaders/*.py`.

#### A) Credit Card Fraud Detection + IEEE-CIS Fraud (3 operators)

Kafka ingestion:
- Topic: `tenant.<tenant_id>.input` (written by `scripts/run_workloads.py`)
- Source records: `datasets/fraud-detection/train_transaction.csv` + `train_identity.csv`

Operators (exactly 3):
1. `transaction_parser`
  - Role: clean and normalize amount/card/address/device fields.
  - Why it matters: fraud scoring is highly sensitive to malformed values.
2. `feature_extractor`
  - Role: compute risk features (`feature_amount_log`, `feature_card_addr_hash`, `feature_device_risk`).
  - Why it matters: these features capture behavioral and profile risk signals.
3. `ml_inference`
  - Role: compute `model_score` and `predicted_fraud`.
  - Why it matters: final fraud detection decision for downstream alerting.

Run:
```bash
python3 scripts/run_individual_applications.py --datasets fraud --samples 3
```

#### B) Web Analytics + Criteo Click Logs (12 operators)

Kafka ingestion:
- Topic: `tenant.<tenant_id>.input`
- Source records: `datasets/web-analytics/train.txt` or `train.csv` or `random_submission.csv`

Operators (exactly 12):
1. `raw_parser` - parse click/user/campaign identifiers.
2. `bot_filter` - mark suspicious bot-like traffic.
3. `sessionizer` - construct per-user session keys.
4. `url_normalizer` - normalize path and remove query noise.
5. `geo_enrichment` - attach coarse region dimension.
6. `device_enrichment` - infer device category.
7. `campaign_join` - map campaign to serving tier.
8. `ctr_feature_builder` - compute click-through-rate features.
9. `rolling_aggregation` - maintain running engagement score.
10. `window_ranker` - rank campaigns in current window.
11. `report_formatter` - prepare windowed analytics record.
12. `report_sink` - emit conversion/impression label.

Why this supports web analytics:
- It matches the clickstream objective: clean events, enrich context, aggregate windows, and output reporting metrics.

Run:
```bash
python3 scripts/run_individual_applications.py --datasets web-analytics --samples 3
```

#### C) Network Intrusion Detection + UNSW-NB15 (7 operators)

Kafka ingestion:
- Topic: `tenant.<tenant_id>.input`
- Source records: `datasets/network-intrusion/UNSW_NB15_training-set.csv` + `UNSW_NB15_testing-set.csv`

Operators (exactly 7):
1. `packet_parser` - parse flow bytes/duration/protocol.
2. `flow_normalizer` - derive normalized flow volume and byte ratio.
3. `protocol_classifier` - classify protocol family.
4. `feature_extractor` - compute rates and entropy proxy features.
5. `windowed_detector` - calculate window-level attack score.
6. `anomaly_classifier` - classify attack/benign.
7. `alert_sink` - emit severity-tagged alert output.

Why this supports intrusion detection:
- It follows practical IDS flow processing from packet features to scored alerts.

Run:
```bash
python3 scripts/run_individual_applications.py --datasets network-intrusion --samples 3
```

#### D) IoT Sensor Analytics + Intel Berkeley Lab (11 operators)

Kafka ingestion:
- Topic: `tenant.<tenant_id>.input`
- Source records: `datasets/iot-sensors/data.txt`

Operators (exactly 11):
1. `raw_reader` - decode sensor reading.
2. `schema_validator` - validate required fields.
3. `null_cleaner` - fill/clean missing or invalid values.
4. `outlier_filter` - clamp extreme temperature outliers.
5. `temperature_enrichment` - compute thermal stress.
6. `humidity_enrichment` - compute comfort index.
7. `light_transform` - transform light intensity feature.
8. `windowed_aggregator` - set analysis window.
9. `anomaly_scoring` - combine thermal/humidity/voltage signals.
10. `threshold_detector` - classify anomaly threshold crossing.
11. `notification_sink` - emit anomaly notification output.

Why this supports IoT analytics:
- It mirrors real sensor analytics: robust cleaning, feature engineering, windowing, and anomaly alerting.

Run:
```bash
python3 scripts/run_individual_applications.py --datasets iot-sensors --samples 3
```

#### Run all 4 applications sequentially

```bash
python3 scripts/run_individual_applications.py --samples 3
```

## 6) Export Full Metrics To CSV

```bash
python3 evaluation/export_prometheus_csv.py \
  --duration-sec 120 \
  --interval-sec 1 \
  --tenants tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot \
  --out-dir evaluation/results/csv
```

Output file pattern:
- `evaluation/results/csv/prometheus_metrics_YYYYMMDD_HHMMSS.csv`

## 7) What Is Inside The CSV

The CSV contains timestamped time-series metrics, including:
- Cluster KPIs: `rue_cluster`, `tlvr_cluster`, `eei`, `fpp`, `mis`
- Cluster throughput and traffic rates:
  - `system_throughput_msgs_per_sec`
  - `msg_in_rate_total`, `msg_out_rate_total`
  - `bytes_in_rate_total`, `bytes_out_rate_total`
- Cluster checkpoint utilization:
  - `checkpoint_cpu_cluster`, `checkpoint_memory_cluster`, `checkpoint_network_cluster`
- Per-tenant throughput:
  - `throughput_<tenant>_in`, `throughput_<tenant>_out`, `throughput_<tenant>_total`
- Per-tenant latency percentiles:
  - `latency_<tenant>_p50_ms`, `p90`, `p95`, `p99`, `p999`
- Per-tenant migration metrics:
  - downtime/transfer instant and accumulated totals
- Per-tenant checkpoint utilization metrics

## 8) Visualize Metrics

### 8.1 Prometheus UI

Open:
- `http://localhost:19090`

Example queries:

```promql
streambazaar_resource_utilization_efficiency{scope="cluster",tenant_id="all"}
streambazaar_tail_latency_violation_rate{scope="cluster",tenant_id="all"}
streambazaar_economic_efficiency_index
streambazaar_fairness_performance_product
streambazaar_migration_impact_score
sum(rate(streambazaar_messages_in_total[1m]))
streambazaar_latency_p99_ms{tenant_id="tenant-iot"}
```

### 8.2 Grafana Dashboard

Open:
- `http://localhost:13000`
- Login: `admin/admin` (if unchanged)
- Select: `StreamBazaar Comprehensive Metrics Dashboard`

### 8.3 Publication-ready figures from CSV

```bash
python3 evaluation/analysis-scripts/plot_publication_metrics.py \
  --csv evaluation/results/csv/prometheus_metrics_YYYYMMDD_HHMMSS.csv \
  --fig-dir evaluation/results/figures_publication \
  --tenants tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot
```

## 9) Run Full Experiment Pipeline

```bash
python3 evaluation/run_paper_experiments.py \
  --runs 2 \
  --warmup-sec 15 \
  --steady-sec 30 \
  --records-per-tenant 150 \
  --csv-monitor-sec 30
```

Generated artifacts:
- `evaluation/results/raw/exp_YYYYMMDD_HHMMSS/run_XX_evaluation_report_*.json`
- `evaluation/results/raw/exp_YYYYMMDD_HHMMSS/summary.json`
- `evaluation/results/raw/exp_YYYYMMDD_HHMMSS/figures/*.png`
- `evaluation/results/raw/exp_YYYYMMDD_HHMMSS/figures_publication/*.png`
- `evaluation/results/raw/exp_YYYYMMDD_HHMMSS/csv/prometheus_metrics_*.csv`

## 10) Run And Compare Baselines

### 10.1 Option A: True Measured Baselines (Recommended)

Run actual baseline implementations with real measurements:

```bash
python3 evaluation/run_true_baseline_measurements.py \
  --duration-sec 60 \
  --input-rate 80000 \
  --records-per-tenant 30000 \
  --dataset iot-sensors \
  --tenant-id tenant-iot
```

This will:
1. Execute all 5 scheduler modes (streambazaar, talos, ds2, capsys, flink_default)
2. Restart Docker containers with mode-specific env vars
3. Run real workloads for each mode
4. Collect actual Prometheus metrics
5. Generate corrected comparison reports

Output files:
- `evaluation/results/true_baseline_runs/run_YYYYMMDD_HHMMSS/true_measured_improvement_report_FIXED.txt`
- `evaluation/results/true_baseline_runs/run_YYYYMMDD_HHMMSS/mode_kpis_CORRECTED.json`
- `evaluation/results/true_baseline_runs/run_YYYYMMDD_HHMMSS/csv/{mode}/prometheus_metrics_*.csv`

**Note:** Use the `*_FIXED.txt` report for publication (latency unit conversion applied).

### 10.2 Option B: Synthesized Baseline Comparison (Faster, Not Measured)

Run synthesized comparison (projects baseline metrics from StreamBazaar data):

```bash
python3 evaluation/run_baseline_comparison.py \
  --prom-url http://localhost:19090 \
  --tenant-ids tenant-fraud,tenant-clickstream,tenant-ml
```

Result file:
- `evaluation/baseline_comparison_results.json`

This file contains:
- `decisions`: scheduler decisions for `TALOS`, `DS2`, `CAPSys`, and `FlinkDefault`
- `profiles`: performance profile per scheduler (synthesized)
- `improvements_vs_flink_default`: KPI deltas
- `metrics_by_scheduler`: full metric map for each scheduler

**Note:** These are synthesized baselines, not truly measured. Use Option A for publication.

### 10.3 Comparison Summary

| Approach | Method | Measured? | Speed | Use Case |
|----------|--------|-----------|-------|----------|
| **Option A** | Run each mode separately | ✅ YES | Slow (4+ min) | Publication, validation |
| **Option B** | Synthesize from StreamBazaar data | ❌ NO | Fast (1 min) | Quick comparison, development |

## 11) Switch Scheduler Modes At Runtime

The `stream-coordinator` supports multiple scheduler modes that can be switched via environment variables.

### 11.1 Available Modes

- `streambazaar`: Auction-driven allocation with pricing (default)
- `talos`: Lag-based reactive autoscaling with cooldown
- `ds2`: Capacity-model-based 3-step scaling
- `capsys`: Contention-aware placement and rebalancing baseline
- `flink_default`: Static fixed parallelism allocation

### 11.2 Switch Mode (Manual)

```bash
# Restart with specific mode
SCHEDULER_MODE=talos docker compose up -d --build stream-coordinator
./scripts/wait-for-services.sh

# Verify mode switched
curl -fsS http://localhost:18085/health | jq '.scheduler_mode'
```

### 11.3 Configuration Parameters

When switching modes, you can configure:

```bash
# Set multiple baseline config options
docker compose up -d --build -e SCHEDULER_MODE=talos \
  -e TALOS_COOLDOWN_SEC=90 \
  -e TALOS_IDLE_THRESHOLD_MS=500
```

Available environment variables:

| Variable | Mode | Default | Description |
|----------|------|---------|-------------|
| `SCHEDULER_MODE` | all | `streambazaar` | Which scheduler to use |
| `FIXED_PARALLELISM_PER_TENANT` | flink_default | 2 | Slots per tenant |
| `TALOS_COOLDOWN_SEC` | talos | 90 | Min seconds between scale actions |
| `TALOS_IDLE_THRESHOLD_MS` | talos | 500 | Idle time before scale-down (ms) |
| `DS2_MAX_SCALING_STEPS` | ds2 | 3 | Max parallelism change per cycle |
| `DS2_STABILITY_SEC` | ds2 | 120 | Stability period before scale-down (sec) |
| `CAPSYS_REBALANCE_SEC` | capsys | 30 | Minimum seconds between contention-driven rebalances |
| `CAPSYS_CONTENTION_THRESHOLD` | capsys | 0.75 | Threshold above which CAPSys scales up |

### 11.4 Automated Mode Switching

Use the true baseline measurement script (Option 10.1) which automatically handles mode switching across all 4 schedulers.

## 12) How To Change Configuration

Common places:
- `docker-compose.yml`
- `configs/cluster-config.yml`
- `configs/tenant-configs/*.yml`
- `configs/benchmark-configs/*.yml`

Typical knobs:
- Tenant list:
  - `TENANT_IDS=...`
- SLA target:
  - `DEFAULT_SLA_TARGET_MS=...`
- Cluster capacity (slots/resources)
- Workload rate and payload:
  - `--input-rate`, `--input-rates`, `--payload-bytes`, `--payload-bytes-map`

Apply changes:

```bash
docker compose restart stream-coordinator pricing-engine resource-allocator
```

Then re-run workload and export metrics again for before/after comparison.

## 12) How To Change Configuration

Common places:
- `docker-compose.yml`
- `configs/cluster-config.yml`
- `configs/tenant-configs/*.yml`
- `configs/benchmark-configs/*.yml`

Typical knobs:
- Tenant list:
  - `TENANT_IDS=...`
- SLA target:
  - `DEFAULT_SLA_TARGET_MS=...`
- Cluster capacity (slots/resources)
- Workload rate and payload:
  - `--input-rate`, `--input-rates`, `--payload-bytes`, `--payload-bytes-map`

Apply changes:

```bash
docker compose restart stream-coordinator pricing-engine resource-allocator
```

Then re-run workload and export metrics again for before/after comparison.

## 13) Quick Troubleshooting

- Topic command fails:
  - Use `kafka-topics` (not `kafka-topics.sh`) in this container image.
- No data in Prometheus:
  - Check `http://localhost:19090/api/v1/targets` and service logs.
- Want strict real data only:
  - Add `--disable-synthetic-fallback --skip-download`.
- Scheduler mode not switching:
  - Verify health endpoint: `curl -fsS http://localhost:18085/health | jq '.scheduler_mode'`
  - Check logs: `docker compose logs stream-coordinator | tail -20`
- Continuous jobs still running:

```bash
pkill -f "continuous_collector.py" || true
pkill -f "run_workloads.py" || true
pkill -f "run_true_baseline_measurements.py" || true
```

## 14) Stop Everything

```bash
docker compose down
```

Optional cleanup of experiment outputs:

```bash
rm -rf evaluation/results/csv/prometheus_metrics_*.csv
rm -rf evaluation/results/raw/exp_*
rm -rf evaluation/results/true_baseline_runs/run_*
```
