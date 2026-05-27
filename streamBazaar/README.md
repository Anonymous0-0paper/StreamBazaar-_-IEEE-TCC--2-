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

## 3.1) Run Scalability Experiment

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
    --node-counts 1 2 4 8 16 \
    --duration-sec 60 \
    --warmup-sec 5
```
```bash
python3 evaluation/plot_scalability.py 
```

| Argument | Description |
|----------|-------------|
| `--node-counts 1 2 4` | Node counts to benchmark (runs one experiment per count) |
| `--duration-sec 60` | Steady-state measurement window per node count (seconds) |
| `--warmup-sec 5` | Warm-up period before metrics are collected (seconds) |

**Step 4 — Wait for results.** The script runs three sequential experiments.

**Step 5 — Find the output.** Results are written to:

```
evaluation/results/scalability/
├── scalability_report_YYYYMMDD_HHMMSS.json   ← aggregated metrics per node count
└── figures/
    ├── scalability_latency.png
    ├── scalability_throughput.png
    └── scalability_resource_util.png
```

### Troubleshooting

- **Services not ready**: run `./scripts/wait-for-services.sh` before the experiment.
- **Missing dependencies**: run `pip install -r evaluation/requirements.txt`.
- **Port conflicts**: verify no other process occupies ports `18080–18088`, `19090`, `19092`.
- **Permission denied on script**: run `chmod +x evaluation/run_scalability_experiment.py`.


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




## 4) Stop And Clean

```bash
docker compose down
```

Remove volumes as well:

```bash
docker compose down -v
```

