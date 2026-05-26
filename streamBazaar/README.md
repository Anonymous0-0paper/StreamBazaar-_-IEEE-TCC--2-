# StreamBazaar Initial Implementation

This folder contains an initial, paper-aligned StreamBazaar scaffold with containerized core services, telemetry storage, and evaluation scripts.

## 1) What Is Included

- Control plane services:
	- `services/auction-orchestrator`
	- `services/pricing-engine`
	- `services/tenant-manager`
	- `services/resource-allocator`
	- `services/migration-coordinator`
- Runtime dependencies:
	- Apache Flink (`jobmanager`, `taskmanager`)
	- Kafka + Zookeeper
	- Redis
	- PostgreSQL
	- InfluxDB
	- Prometheus + Grafana
- Evaluation and workload skeletons:
	- `evaluation/metrics-collector`
	- `evaluation/latency-tracker`
	- `evaluation/run_evaluation.py`
	- `workloads/fraud-detection`
	- `workloads/clickstream-analytics`
	- `workloads/ml-inference`

Paper mapping details are in `PAPER_ALIGNMENT.md`.

## 1.1) Implemented Core Algorithms

- **Auction clearing** (`services/auction-orchestrator/app/main.py`):
	- Bid registration per tenant (`/bid`)
	- Score-based winner selection with tenant priority + SLA urgency (`/auction/clear`)
	- Clearing-price computation and revenue tracking (`/auction/last`)
- **Dynamic pricing** (`services/pricing-engine/pricing_engine/pricing_server.py`):
	- SLA-aware bid floor with utilization, queue pressure, and credit-balance pressure
	- Temporal smoothing to avoid oscillation in bid floor
- **Resource allocation** (`services/resource-allocator/app/main.py`):
	- Weighted fair allocation for multi-tenant batches (water-filling refinement)
	- Backward-compatible single-tenant allocation path
- **Migration/preemption policy** (`services/migration-coordinator/app/main.py`):
	- Triggering based on load-pressure and SLA breach ratio
	- Per-tenant cooldown to reduce migration thrashing
- **Native Flink streaming control loop** (`flink-integration/`): 📌 NEW
	- Consumes multi-tenant input events from Kafka (`tenant.*.input` topics)
	- Applies pricing logic by extracting SLA target, priority, and workload context from event payload
	- Performs batch auction clearing with SLA-weighted scoring
	- Computes weighted fair allocations with water-filling to handle SLA gaps
	- Manages per-tenant latency tracking and fairness metrics
	- Emits allocation decisions and preemption signals to `streamBazaar.allocations`
	- Replaces REST-based `stream-coordinator` for paper evaluation; demonstrates native streaming scheduler on Flink
	- Built with Maven, deployed as JAR to Flink `/lib/`, auto-submitted on cluster startup
- **Baseline comparison metrics** (`evaluation/baseline_comparison.py`):
	- Resource, latency, throughput, and Jain fairness comparisons vs YARN baseline

## 2) Prerequisites & Full Setup Guide

Before running any experiment, complete every step in this section in order.

---

### Step 1 — Install Docker Engine & Docker Compose

**Ubuntu / Debian:**

```bash
# Remove old versions if any
sudo apt-get remove -y docker docker-engine docker.io containerd runc 2>/dev/null || true

# Install dependencies
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg lsb-release

# Add Docker's official GPG key and repo
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
    sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker Engine + Compose plugin
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Allow running Docker without sudo (log out and back in after this)
sudo usermod -aG docker $USER
```

**macOS** — install [Docker Desktop](https://www.docker.com/products/docker-desktop/) which includes the Compose plugin.

**Windows** — install [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/) (WSL 2 backend recommended).

Verify:

```bash
docker --version          # e.g. Docker version 25.x
docker compose version    # e.g. Docker Compose version v2.x
```

---

### Step 2 — Install Python 3.10+

**Ubuntu / Debian:**

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-pip python3-venv
python3 --version   # must be 3.10 or higher
```

**macOS (via Homebrew):**

```bash
brew install python@3.11
python3 --version
```

**Windows** — download the installer from [python.org](https://www.python.org/downloads/) and check "Add Python to PATH" during install.

---

### Step 3 — Install curl & git

```bash
# Ubuntu / Debian
sudo apt-get install -y curl git

# macOS (curl and git are pre-installed; update via Homebrew if needed)
brew install curl git
```

---

### Step 4 — Clone the Repository

```bash
git clone <your-repo-url>
cd streamBazaar
```

---

### Step 5 — Create & Activate a Python Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate          # Linux / macOS
# .venv\Scripts\activate           # Windows PowerShell
```

---

### Step 6 — Install Python Dependencies

```bash
pip install --upgrade pip
pip install -r requirements-dev.txt -r evaluation/requirements.txt
```

Key packages installed include `kafka-python`, `requests`, `influxdb-client`, `prometheus-api-client`, `matplotlib`, `numpy`, and `scipy`.

---

### Step 7 — Verify Docker Resources (Recommended)

StreamBazaar runs ~12 containers. Ensure Docker has sufficient resources:

- **CPU**: 4+ cores
- **RAM**: 8 GB minimum (16 GB recommended for paper-grade runs)
- **Disk**: 10 GB free

On Docker Desktop, adjust limits under **Settings → Resources**.

---

After completing all steps above, proceed to Section 3 (Start The Stack).

The local scripts use `kafka-python` to publish workload events to Kafka topics.

## 2.1) Quick Start (First Time Setup)

If you want to run with real paper datasets, set credentials now:

```bash
# Option 1: Kaggle credentials (for fraud detection & web analytics datasets)
export KAGGLE_USERNAME=your_username
export KAGGLE_KEY=your_api_key

# Option 2: Or use credentials file (see section 5.3 for setup)

# Option 3: UNSW-NB15 (network intrusion)
# Manually download & place files in datasets/network-intrusion/ OR
export UNSW_NB15_BASE_URL=https://your-mirror-url

# Verify dataset readiness:
python3 scripts/prepare_datasets.py --json
```

If you skip credential setup, experiments will use synthetic fallback data automatically.

## 3) Start The Stack

From `streamBazaar/`:

```bash
docker compose up -d --build
./scripts/wait-for-services.sh
```

Or run the helper:

```bash
./deploy.sh
```

## 4) Initialize Tenant Metadata (PostgreSQL)

Run after `postgres` is up:

```bash
python scripts/init-tenants.py
```

This applies `scripts/init-tenants.sql` and loads `configs/tenant-configs/sample-tenants.yml`.

## 5) Run Evaluation

Short smoke run (5 minutes):

```bash
python evaluation/run_evaluation.py --duration 5
```

Quick validation run (0 minutes, seeds and computes one metric batch):

```bash
python evaluation/run_evaluation.py --duration 0
```

Paper-style longer run (example 120 minutes):

```bash
python evaluation/run_evaluation.py --duration 120
```

The script writes `evaluation_report_YYYYMMDD_HHMMSS.json` in `streamBazaar/`.

Note: Earlier versions of this scaffold wrote placeholder `null` metrics in smoke reports.
The current version collects/writes/query metrics before report generation, so fields are populated.

## 5.1) Run Scalability Experiment

The scalability experiment measures StreamBazaar performance (latency, throughput, resource utilization) across increasing node counts, reproducing the paper's scalability evaluation.

### Prerequisites

Make sure the stack is running and Python dependencies are installed:

```bash
# From streamBazaar/
docker compose up -d --build
./scripts/wait-for-services.sh

# Install evaluation dependencies if not already done
pip install -r evaluation/requirements.txt
```

### Step-by-Step Guide

**Step 1 — Navigate to the project root (`streamBazaar/`):**

```bash
cd streamBazaar
```

**Step 2 — (Optional) Activate your virtual environment:**

```bash
source .venv/bin/activate
```

**Step 3 — Run the scalability experiment:**

```bash
python3 evaluation/run_scalability_experiment.py \
    --node-counts 1 2 4 \
    --duration-sec 60 \
    --warmup-sec 5
```

| Argument | Description |
|----------|-------------|
| `--node-counts 1 2 4` | Node counts to benchmark (runs one experiment per count) |
| `--duration-sec 60` | Steady-state measurement window per node count (seconds) |
| `--warmup-sec 5` | Warm-up period before metrics are collected (seconds) |

**Step 4 — Wait for results.** The script runs three sequential experiments (1 node → 2 nodes → 4 nodes). Each takes `warmup-sec + duration-sec` seconds, so the full run above takes approximately **3 × 65 = ~195 seconds**.

**Step 5 — Find the output.** Results are written to:

```
evaluation/results/scalability/
├── scalability_report_YYYYMMDD_HHMMSS.json   ← aggregated metrics per node count
└── figures/
    ├── scalability_latency.png
    ├── scalability_throughput.png
    └── scalability_resource_util.png
```

### Customizing the Run

Longer paper-grade run (matches Section 5 of the paper):

```bash
python3 evaluation/run_scalability_experiment.py \
    --node-counts 1 2 4 8 16 \
    --duration-sec 120 \
    --warmup-sec 30
```

Quick smoke test (fast sanity check):

```bash
python3 evaluation/run_scalability_experiment.py \
    --node-counts 1 2 \
    --duration-sec 15 \
    --warmup-sec 5
```

### Troubleshooting

- **Services not ready**: run `./scripts/wait-for-services.sh` before the experiment.
- **Missing dependencies**: run `pip install -r evaluation/requirements.txt`.
- **Port conflicts**: verify no other process occupies ports `18080–18088`, `19090`, `19092`.
- **Permission denied on script**: run `chmod +x evaluation/run_scalability_experiment.py`.

---

## 5.3) Run Reproducible Paper Experiments

This runs multi-round experiments with warmup + steady-state windows, stores raw reports,
and automatically generates aggregate statistics and figures.

Install analysis dependencies if needed:

```bash
pip install -r evaluation/requirements.txt
```

Run a compact experiment (example):

```bash
python3 evaluation/run_paper_experiments.py --runs 2 --warmup-sec 15 --steady-sec 30 --records-per-tenant 200
```

Outputs (each experiment run creates its own folder):

- Experiment folder: `evaluation/results/raw/exp_YYYYMMDD_HHMMSS/`
- Raw per-run reports: `evaluation/results/raw/exp_.../run_*_evaluation_report_*.json`
- Run manifest: `evaluation/results/raw/exp_.../run_manifest.json`
- Statistical summary (mean/std/95% CI): `evaluation/results/raw/exp_.../summary.json`
- Figures:
  - `evaluation/results/raw/exp_.../figures/latency_p99.png`
  - `evaluation/results/raw/exp_.../figures/throughput_avg.png`
  - `evaluation/results/raw/exp_.../figures/cpu_avg.png`
  - `evaluation/results/raw/exp_.../figures/memory_avg.png`

## 5.4) Flink Streaming Scheduler Integration

The `flink-integration/` module implements a native Apache Flink job that replaces the REST-based stream-coordinator with a true streaming control loop. This is the paper's intended architecture: a single unified Flink job orchestrating pricing → auction → allocation → migration natively on the streaming cluster.

**Build & Deployment** (automatic with `docker compose up --build`):
- Maven builds `flink-integration/pom.xml` → `target/flink-integration.jar`
- Flink service (`services/flink-cluster/Dockerfile`) multi-stage builds: copies built JAR to Flink `/lib/`
- Cluster startup: `flink-jobmanager` + `flink-taskmanager` + `flink-job-submitter` service (auto-submits job after jobmanager is healthy)

**Job Behavior** (`flink-integration/src/main/java/streambazaar/flink/`):
- `StreamBazaarJob.java`: Main entry point; configures Kafka source (pattern `tenant.*.input`), applies `AuctionOrchestrator` processing, sinks allocations to `streamBazaar.allocations`
- `AuctionOrchestrator.java`: Keyed processing function (per-tenant state); computes pricing → bidding → allocation → migration in a single streaming pipeline
- `RecordingMetadata.java`: Per-operator latency recording for evaluation metrics

**Monitoring**:
- Flink UI: `http://localhost:18088` → Running jobs, task parallelism, latency metrics
- Prometheus: `http://localhost:19090` → Flink operator metrics (published on port 9249)

**Expected Behavior**:
- On cluster startup, the job-submitter polls jobmanager health and submits the job
- Job consumes Kafka `tenant.fraud.input`, `tenant.clickstream.input`, `tenant.ml.input`
- Each event updates tenant state (queue backlog, SLA latencies, credit balance)
- Every 2 seconds or when queue exceeds 100 events: trigger auction, compute allocation, emit decision
- Decisions published to `streamBazaar.allocations` and per-tenant `tenant.*.output` topics

To inspect job logs or manually resubmit:
```bash
# View Flink jobmanager logs
docker logs streamBazaar-flink-jobmanager-1

# Manually submit job (if auto-submission failed)
./scripts/submit-flink-job.sh flink-jobmanager 8081

# Cancel running job (replace JOB_ID with actual ID from Flink UI)
curl -X PATCH http://localhost:18088/v1/jobs/JOB_ID?mode=cancel
```

## 5.5) Dataset Setup For Real-Data Evaluation

For automatic downloads of real evaluation datasets, configure credentials before your first experiment run.

**Kaggle (IEEE-CIS Fraud Detection & Criteo Click Logs):**

Get your API token:
1. Go to https://www.kaggle.com/settings/account
2. Click "Create New API Token" (downloads `kaggle.json`)

Then choose one method:

- **Via credentials file** (recommended for local development):
  ```bash
  mkdir -p ~/.kaggle
  cp ~/Downloads/kaggle.json ~/.kaggle/
  chmod 600 ~/.kaggle/kaggle.json
  ```

- **Via environment variables** (recommended for CI/scripts):
  ```bash
  export KAGGLE_USERNAME=your_username
  export KAGGLE_KEY=your_api_key
  ```

**UNSW-NB15 (Network Intrusion Detection):**

Choose one method:

- **Manual file placement** (simpler):
  1. Download from https://research.unsw.edu.au/projects/unsw-nb15-dataset
  2. Download `UNSW_NB15_training-set.csv` and `UNSW_NB15_testing-set.csv`
  3. Place in: `streamBazaar/datasets/network-intrusion/`

- **Via mirror URL** (if you have access to a mirror):
  ```bash
  export UNSW_NB15_BASE_URL=https://your-mirror-url
  ```

**Verify setup:**

```bash
python3 scripts/prepare_datasets.py --json
```

Expected output for full real-data setup:
- `iot-sensors`: `validated: true` (downloads automatically from Berkeley)
- `fraud`: `validated: true` (when Kaggle credentials are set)
- `web-analytics`: `validated: true` (when Kaggle credentials are set)
- `network-intrusion`: `validated: true` (when manual files placed or mirror URL set)

If validation fails, synthetic fallback data is used automatically during experiments.

## 6) Run Kafka Workload Streaming (E2E)

Create topics:

```bash
./scripts/create-kafka-topics.sh
```

Publish synthetic workload events into Kafka (`streamBazaar.bids` and per-tenant input topics):

```bash
python3 scripts/run_workloads.py --duration-sec 30 --records-per-tenant 50
```

### Real Dataset Management (Paper-Aligned)

`scripts/run_workloads.py` now uses `datasets/download_manager.py` and dataset-specific loaders.
For each requested dataset, it follows this flow:

1. Check local files under `streamBazaar/datasets/<dataset-dir>/`
2. Validate integrity (required files, minimum size, minimum row count)
3. Download missing datasets when allowed
4. If unavailable, switch to synthetic fallback (unless disabled)

Implemented structure:

```text
datasets/
├── download_manager.py
├── dataset_loaders/
│   ├── fraud_loader.py
│   ├── criteo_loader.py
│   ├── unsw_loader.py
│   └── berkeley_loader.py
├── synthetic_fallback/
│   └── generators.py
└── workload_generators/
		├── fraud_workload.py
		├── web_analytics_workload.py
		├── network_intrusion_workload.py
		└── iot_sensor_workload.py
```

Default paper dataset set:

- `fraud` (priority: `high`, 3 operators)
- `web-analytics` (priority: `low`, 12 operators)
- `network-intrusion` (priority: `high`, 7 operators)
- `iot-sensors` (priority: `medium`, 11 operators)

State sizes for windowed operators are attached to each event and are configurable with:

- `--state-size-min-gb` (default `0.1`)
- `--state-size-max-gb` (default `10.0`)
- `--state-size-avg-gb` (default `1.0`)

Replay compression is configurable via `--compress-time-window` (default `10.0`) to increase backpressure while preserving data ordering/characteristics.

### Dataset Sources And Required Files

- IEEE-CIS Fraud Detection (Kaggle):
	- URL: `https://www.kaggle.com/c/ieee-fraud-detection/data`
	- Files: `train_transaction.csv`, `train_identity.csv`
	- Directory: `datasets/fraud-detection/`

- Criteo Click Logs (Kaggle challenge or Criteo Labs):
	- URL: `https://www.kaggle.com/c/criteo-display-ad-challenge/data`
	- Alt URL: `http://labs.criteo.com/2013/12/download-terabyte-click-logs/`
	- File: `train.txt` (subset supported for practical execution)
	- Directory: `datasets/web-analytics/`

- UNSW-NB15:
	- URL: `https://research.unsw.edu.au/projects/unsw-nb15-dataset`
	- Files: `UNSW_NB15_training-set.csv`, `UNSW_NB15_testing-set.csv`
	- Directory: `datasets/network-intrusion/`

- Intel Berkeley Lab Sensor Data:
	- URL: `http://db.csail.mit.edu/labdata/labdata.html`
	- File: `data.txt`
	- Directory: `datasets/iot-sensors/`

### Kaggle Authentication

For Kaggle datasets, configure one of:

- Environment variables: `KAGGLE_USERNAME` and `KAGGLE_KEY`
- File: `~/.kaggle/kaggle.json`

Install CLI if needed:

```bash
pip install kaggle
```

### Example: Real-First Run With Fallback

```bash
python3 scripts/run_workloads.py \
	--datasets fraud,web-analytics,network-intrusion,iot-sensors \
	--records-per-dataset fraud=50000,web-analytics=200000,network-intrusion=60000,iot-sensors=60000 \
	--input-rates fraud=120000,web-analytics=500000,network-intrusion=100000,iot-sensors=80000 \
	--compress-time-window 12 \
	--criteo-subset-lines 500000 \
	--duration-sec 120
```

### Example: Strict Real-Data Mode (No Synthetic)

```bash
python3 scripts/run_workloads.py \
	--datasets fraud,web-analytics,network-intrusion,iot-sensors \
	--disable-synthetic-fallback \
	--duration-sec 60
```

### One-Command Dataset Preflight/Download

Use `scripts/prepare_datasets.py` to check, validate, and optionally download datasets before experiments.

```bash
python3 scripts/prepare_datasets.py
```

Strict mode (fail if real datasets are unavailable):

```bash
python3 scripts/prepare_datasets.py --disable-synthetic-fallback --skip-download
```

Machine-readable summary:

```bash
python3 scripts/prepare_datasets.py --json
```

### Dataset-Only Dry Run (No Kafka Required)

You can validate dataset readiness and workload wiring without connecting to Kafka:

```bash
python3 scripts/run_workloads.py \
	--datasets fraud,web-analytics,network-intrusion,iot-sensors \
	--disable-synthetic-fallback \
	--skip-download \
	--dry-run
```

### Dataset Access Troubleshooting

- Kaggle auth failures:
	- Verify `KAGGLE_USERNAME`/`KAGGLE_KEY` or `~/.kaggle/kaggle.json`
	- Confirm competition terms are accepted in your Kaggle account

- Network/download failures:
	- Retry with stable connectivity
	- Use `--skip-download` when datasets are already staged locally

- UNSW direct access restrictions:
	- Stage files manually in `datasets/network-intrusion/` or set `UNSW_NB15_BASE_URL` to a valid mirror

- Criteo storage pressure:
	- Use `--criteo-subset-lines` to constrain local footprint
	- Ensure sufficient free disk before large downloads

### Synthetic Fallback Behavior

- Fallback activates when required files are missing or fail integrity checks.
- Synthetic records follow the same normalized schema fields used by real dataset loaders.
- Disable fallback with `--disable-synthetic-fallback` for strict reproducibility checks.

### Licensing And Citation Notes

Dataset licensing and usage terms are controlled by the original providers. Before use, verify each dataset's current terms and citation requirements at the source links above.

Recommended citations (as used in the paper context):

- IEEE-CIS Fraud Detection (`ieee_cis_fraud_2019`)
- Criteo CTR logs (`criteo_ctr_2013`)
- UNSW-NB15 (`moustafa2015unsw`)
- Intel Berkeley Lab data (`madden2004intel`)

Run end-to-end stream test (health checks + topic creation + publishing + topic verification):

```bash
./scripts/e2e_stream_test.sh
```

Run full smoke test (API + DB + Kafka stream + evaluation report):

```bash
./scripts/smoke_test.sh
```

## 6.1) Configure Tenants, Topics, Message Rate, And Message Bytes

The main control point is `scripts/run_workloads.py` plus topic creation via `scripts/create-kafka-topics.sh`.

### A) Change number of tenants and tenant names

Use any tenant IDs you want by aligning `--datasets` and `--tenant-ids`:

```bash
python3 scripts/run_workloads.py \
	--datasets fraud,clickstream,ml \
	--tenant-ids tenant-a,tenant-b,tenant-c \
	--duration-sec 30 \
	--records-per-tenant 100
```

Create matching Kafka topics:

```bash
TENANT_IDS=tenant-a,tenant-b,tenant-c \
INPUT_TOPIC_TEMPLATE='tenant.{tenant_id}.input' \
OUTPUT_TOPIC_TEMPLATE='tenant.{tenant_id}.output' \
bash ./scripts/create-kafka-topics.sh
```

### B) Change number of messages per dataset

```bash
python3 scripts/run_workloads.py \
	--datasets fraud,clickstream,ml \
	--records-per-dataset fraud=300,clickstream=1000,ml=200 \
	--duration-sec 60
```

### C) Change frequency (messages/sec)

```bash
python3 scripts/run_workloads.py \
	--input-rates fraud=20,clickstream=60,ml=15 \
	--duration-sec 60
```

### D) Change bytes per message

`payload` padding lets you control message size:

```bash
python3 scripts/run_workloads.py \
	--payload-bytes-map fraud=256,clickstream=1024,ml=512 \
	--duration-sec 60
```

### E) Change Kafka topic names/templates

```bash
python3 scripts/run_workloads.py \
	--bids-topic custom.bids \
	--input-topic-template 'ingest.{tenant_id}.input' \
	--duration-sec 30
```

Topic names used by the stream coordinator are configurable with environment variables in `docker-compose.yml` or shell:

- `TENANT_IDS`
- `INPUT_TOPIC_TEMPLATE`
- `OUTPUT_TOPIC_TEMPLATE`
- `ALLOC_TOPIC`
- `PREEMPT_TOPIC`
- `METRICS_TOPIC`

## 7) Monitoring Endpoints

- Flink UI: `http://localhost:18088`
- Auction Orchestrator: `http://localhost:18080/health`
- Pricing Engine: `http://localhost:18081/health`
- Tenant Manager: `http://localhost:18082/health`
- Resource Allocator: `http://localhost:18083/health`
- Migration Coordinator: `http://localhost:18084/health`
- Stream Coordinator: `http://localhost:18085/health`
- Prometheus: `http://localhost:19090`
- Grafana: `http://localhost:13000` (default admin/admin unless changed)
- InfluxDB: `http://localhost:18086`
- Kafka external bootstrap: `localhost:19092`

### Message State / Telemetry Metrics

The stream coordinator exposes message state and flow metrics on `http://localhost:18085/metrics`:

- `streambazaar_messages_in_total`
- `streambazaar_messages_out_total`
- `streambazaar_message_bytes_in_total`
- `streambazaar_message_bytes_out_total`
- `streambazaar_message_last_bytes`
- `streambazaar_tenant_backlog`
- `streambazaar_tenant_p99_latency_ms`
- `streambazaar_tenant_last_bid`

These let you track message frequency (`rate(...)`), bytes throughput (`rate(..._bytes...)`), and current state (backlog/latency/bid).

All runtime metrics are now measured from live traffic and real service timings (no random/synthetic metric generation).

- Latency percentiles come from observed event timestamps at consume time.
- Throughput comes from actual consumed/published message deltas.
- Migration transfer/downtime use measured request/interval timings.
- Checkpoint CPU/memory/network utilization is measured from process/system stats and observed network byte flow.

## 7.1) New KPI Metrics (Prometheus)

Implemented and exported by `stream-coordinator`:

- `streambazaar_resource_utilization_efficiency` (RUE)
- `streambazaar_tail_latency_violation_rate` (TLVR)
- `streambazaar_economic_efficiency_index` (EEI)
- `streambazaar_fairness_performance_product` (FPP)
- `streambazaar_migration_impact_score` (MIS)

### Prometheus Queries

RUE (cluster):

```promql
streambazaar_resource_utilization_efficiency{scope="cluster",tenant_id="all"}
```

RUE per tenant:

```promql
streambazaar_resource_utilization_efficiency{scope="tenant"}
```

TLVR (cluster):

```promql
streambazaar_tail_latency_violation_rate{scope="cluster",tenant_id="all"}
```

TLVR per high-priority tenant:

```promql
streambazaar_tail_latency_violation_rate{scope="tenant"}
```

EEI:

```promql
streambazaar_economic_efficiency_index
```

FPP:

```promql
streambazaar_fairness_performance_product
```

MIS:

```promql
streambazaar_migration_impact_score
```

Message frequency (in/out msg/sec):

```promql
sum(rate(streambazaar_messages_in_total[1m]))
sum(rate(streambazaar_messages_out_total[1m]))
```

Byte throughput (bytes/sec):

```promql
sum(rate(streambazaar_message_bytes_in_total[1m]))
sum(rate(streambazaar_message_bytes_out_total[1m]))
```

Per-topic ingest rate:

```promql
sum by (topic) (rate(streambazaar_messages_in_total[1m]))
```

Backlog by tenant:

```promql
streambazaar_tenant_backlog
```

### Grafana

Use datasource `Prometheus` and paste any query above into panels. Recommended dashboard panels:

- KPI Row: RUE, TLVR, EEI, FPP, MIS (single stat)
- Traffic Row: in/out msg rate, in/out byte rate (time series)
- Tenant Row: backlog, p99 latency, last bid by tenant

## 7.2) Save Metrics To CSV (1-second intervals)

Export selected Prometheus metrics to timestamped CSV:

```bash
python3 evaluation/export_prometheus_csv.py \
	--prom-url http://localhost:19090 \
	--duration-sec 120 \
	--interval-sec 1 \
	--out-dir evaluation/results/csv
```

Output file pattern:

- `evaluation/results/csv/prometheus_metrics_YYYYMMDD_HHMMSS.csv`

The CSV now includes:

- Throughput (system + per-tenant in/out/total)
- Latency percentiles (`p50`, `p90`, `p95`, `p99`, `p99.9`) per tenant
- Migration downtime and transfer time (current and accumulated totals)
- Checkpoint utilization (`cpu`, `memory`, `network`) per tenant and cluster
- KPI metrics (`RUE`, `TLVR`, `EEI`, `FPP`, `MIS`)

## 7.3) Include KPIs In Evaluation Reports

`evaluation/run_evaluation.py` now snapshots KPI values from Prometheus and stores them under `advanced_kpis` in each report JSON.

## 11) End-To-End Run (Main Flow, No Flink Required)

```bash
# 1) Start stack
docker compose up -d --build
./scripts/wait-for-services.sh

# 2) Initialize tenant metadata
python3 scripts/init-tenants.py

# 3) Create topics for chosen tenants
TENANT_IDS=tenant-fraud,tenant-clickstream,tenant-ml bash ./scripts/create-kafka-topics.sh

# 4) Run workload with custom message rates/sizes/counts
python3 scripts/run_workloads.py \
	--datasets fraud,clickstream,ml \
	--tenant-ids tenant-fraud,tenant-clickstream,tenant-ml \
	--records-per-dataset fraud=300,clickstream=900,ml=200 \
	--input-rates fraud=20,clickstream=60,ml=15 \
	--payload-bytes-map fraud=256,clickstream=1024,ml=512 \
	--duration-sec 90

# 5) Export Prometheus KPIs to CSV every second
python3 evaluation/export_prometheus_csv.py --duration-sec 90 --interval-sec 1

# 6) Generate evaluation report (includes advanced_kpis)
python3 evaluation/run_evaluation.py --duration 1
```

Where monitoring is available:

- Prometheus UI: `http://localhost:19090`
- Grafana UI: `http://localhost:13000`
- Stream coordinator metrics endpoint: `http://localhost:18085/metrics`
- CSV outputs: `evaluation/results/csv/`

## 12) Step-By-Step Browser Validation Guide

See `howtorun.md` for a complete runbook including:

- end-to-end commands,
- Prometheus query checks in browser,
- Grafana panel setup,
- publication-ready plotting commands.

## 8) Kafka Topic Plan (Paper)

Create topics as needed:

- `streamBazaar.bids`
- `streamBazaar.allocations`
- `streamBazaar.preemptions`
- `streamBazaar.metrics`
- `tenant.{id}.input`
- `tenant.{id}.output`

## 9) Important Notes For Paper-Grade Experiments

- **Flink streaming scheduler** (`flink-integration/`): ✅ IMPLEMENTED - Native Flink job orchestrates pricing/auction/allocation/migration natively. Auto-builds and auto-submits on cluster startup.
- **Workload generation**: Dual-mode support - synthetic Kafka publishers for E2E validation, plus paper-ready experiment harness with warmup/steady phases and statistical aggregation.
- **Baseline comparison**: Currently uses deterministic profile model; can be extended with measured runs from real scheduler backends (YARN, Kubernetes, Mesos).
- **Flink metrics**: Operator-level latencies recorded and can be emitted to InfluxDB via Prometheus bridge.

See `PAPER_ALIGNMENT.md` for complete algorithm mapping and remaining gaps.

## 10) Stop And Clean

```bash
docker compose down
```

Remove volumes as well:

```bash
docker compose down -v
```


## 7A) Comprehensive Monitoring & Metrics Guide

StreamBazaar exposes **real-time metrics** capturing all performance dimensions mentioned in the paper evaluation.

### 📊 Quick Access to Monitoring Documentation

- **[METRICS_QUICKSTART.md](METRICS_QUICKSTART.md)** — 5-minute guide to access and interpret metrics live
- **[MONITORING.md](MONITORING.md)** — Complete reference covering all 50+ metrics (RUE, TLVR, EEI, FPP, MIS)
- **[PARAMETER_TUNING.md](PARAMETER_TUNING.md)** — How to adjust tenants, workload rates, and observe effects on metrics

### Paper-Specified Metrics Exposed

All metrics mentioned in paper Section 5 (Performance Evaluation) are exposed via Prometheus:

| Metric | Query | Interpretation | Paper Target |
|--------|-------|-----------------|---------------|
| **RUE** | `streambazaar_resource_utilization_efficiency{scope="cluster"}` | Resource packing efficiency (0-100%) | 38% improvement vs Flink-Default |
| **TLVR** | `streambazaar_tail_latency_violation_rate{scope="cluster"}` | SLA violation rate (0-1) | 6.6× fewer violations vs baselines |
| **EEI** | `streambazaar_economic_efficiency_index` | Auction allocation quality (0-1) | Target >0.85 for near-optimal |
| **FPP** | `streambazaar_fairness_performance_product` | Fairness × throughput (0-1) | Target >0.7 balanced fairness |
| **MIS** | `streambazaar_migration_impact_score` | Migration overhead (0+) | 3.3× lower than baselines |

Plus 40+ supporting metrics for per-tenant latency (p50/p90/p99/p99.9), throughput, backlog, bids, and resource utilization.

### Access Monitoring Interfaces

**Prometheus** (Raw metrics & queries): 
```
http://localhost:19090
```
- Graph tab for live metric visualization
- Query examples: `streambazaar_resource_utilization_efficiency{scope="cluster"}`
- See [MONITORING.md](MONITORING.md) Section 8 for advanced queries

**Grafana** (Visual dashboards):
```
http://localhost:13000
Login: admin / admin
```
- Dashboard: **StreamBazaar Comprehensive Metrics Dashboard** (auto-provisioned)
- Shows all 5 paper metrics + supporting panels
- Auto-refreshes every 5 seconds

**Flink UI** (Streaming job details):
```
http://localhost:18088
```
- Job topology, operator state, checkpoint progress
- Per-operator metrics and backpressure indicators

### Quick Experiment: Monitor Metrics While Running Workload

Terminal 1 - Start the stack:
```bash
docker compose up -d --build
./scripts/wait-for-services.sh
```

Terminal 2 - Start a workload:
```bash
python3 scripts/run_workloads.py \
  --datasets fraud-detection \
  --tenants tenant-fraud \
  --duration-sec 120 \
  --records-per-tenant 500
```

Terminal 3 - Monitor in Grafana:
```
Open http://localhost:13000 in browser
Watch RUE, TLVR, EEI, FPP, MIS panels update in real-time
```

Or in Prometheus:
```
curl "http://localhost:19090/api/v1/query?query=streambazaar_resource_utilization_efficiency{scope=%22cluster%22}"
```

### Changing Parameters to Observe Metric Effects

See [PARAMETER_TUNING.md](PARAMETER_TUNING.md) for detailed experiments:

**Example 1: Add More Tenants**
```yaml
# Edit docker-compose.yml
environment:
  - TENANT_IDS=tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot
# Restart and observe FPP (fairness), RUE changes
```

**Example 2: Increase Workload Rate**
```bash
# Start high-rate workload (stress test
python3 scripts/run_workloads.py \
  --datasets web-analytics \
  --rate-per-sec 250000  # 250K msgs/sec
# Watch TLVR spike, RUE increase, bids escalate
```

**Example 3: Adjust SLA Target**
```yaml
# Edit docker-compose.yml - stricter SLA
environment:
  - DEFAULT_SLA_TARGET_MS=50  # vs default 200ms
# Watch TLVR increase significantly
```

See Section 2-5 of [PARAMETER_TUNING.md](PARAMETER_TUNING.md) for 10+ more experiments with expected outcomes.

