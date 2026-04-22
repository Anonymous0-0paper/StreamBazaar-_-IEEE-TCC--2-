# StreamBazaar — 1-Node Deployment

Single machine, all services in one Docker Compose stack.
This is the default local development and quick-evaluation mode.

---

## Architecture

```
┌────────────────────────────────────────────────────────┐
│                      node-0 (localhost)                 │
│                                                        │
│  Kafka · Zookeeper · Redis · Postgres                  │
│  auction-orchestrator  pricing-engine                  │
│  resource-allocator    migration-coordinator           │
│  stream-coordinator    Prometheus · Grafana            │
│                                                        │
│  Tenants: tenant-fraud, tenant-clickstream,            │
│           tenant-ml, tenant-iot                        │
└────────────────────────────────────────────────────────┘
```

All services share a single Docker network. Redis stores shared state
(virtual balances, bid history, price history). The stream-coordinator
consumes all Kafka partitions because it is the only consumer in its group.

---

## Prerequisites

- Docker >= 24 and Docker Compose V2 installed
- Python 3.9+ (for workload and evaluation scripts)
- Port availability: 18080–18086, 19090, 13000, 19092, 15432, 6379

```bash
# Verify Docker
docker --version
docker compose version

# Verify Python
python3 --version
```

---

## Step 1 — Clone / enter the project directory

```bash
cd "/home/user/Downloads/StreamBazaar _ IEEE TCC (2)/streamBazaar"
```

All commands below must be run from this directory.

---

## Step 2 — Build and start all services

```bash
docker compose up -d --build
docker compose up -d kafka migration-coordinator
```

Expected output: all containers transition to `Up` or `Up (healthy)`.

---

## Step 3 — Wait for services to be ready

```bash
./scripts/wait-for-services.sh
```

All six health endpoints (18080–18085) must return `Ready`.

---

## Step 4 — Initialize tenants and Kafka topics

```bash
python3 scripts/init-tenants.py

TENANT_IDS=tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot \
  bash ./scripts/create-kafka-topics.sh
```

Expected: 12 topics created (`topic ready:` lines).

---

## Step 5 — Verify all services are healthy

```bash
docker compose ps
```

All services should show `Up`. Then check the scheduler mode:

```bash
curl -fsS http://localhost:18085/health | python3 -c \
  "import json,sys; d=json.load(sys.stdin); print('mode:', d['scheduler_mode'], '| running:', d['running'])"
```

Expected: `mode: streambazaar | running: True`

---

## Step 6 — Send a workload

```bash
python3 scripts/run_workloads.py \
  --datasets iot-sensors \
  --tenant-ids tenant-iot \
  --records-per-tenant 500 \
  --input-rate 100 \
  --duration-sec 180 \
  --disable-synthetic-fallback \
  --skip-download
```

Watch consumption in real time:

```bash
# Messages flowing into Kafka
docker compose exec -T kafka kafka-run-class \
  kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 \
  --topic tenant.tenant-iot.input --time -1
```

---

## Step 7 — Export metrics to CSV

```bash
python3 evaluation/export_prometheus_csv.py \
  --duration-sec 120 \
  --interval-sec 1 \
  --tenants tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot \
  --out-dir evaluation/results/csv
```

Output: `evaluation/results/csv/prometheus_metrics_YYYYMMDD_HHMMSS.csv`

---

## Step 8 — Run the full paper experiment pipeline

```bash
python3 evaluation/run_paper_experiments.py \
  --runs 2 \
  --warmup-sec 15 \
  --steady-sec 30 \
  --records-per-tenant 150 \
  --csv-monitor-sec 30
```

Artifacts saved to: `evaluation/results/raw/exp_YYYYMMDD_HHMMSS/`

---

## Step 9 — Run true baseline measurements

```bash
python3 evaluation/run_true_baseline_measurements.py \
  --duration-sec 60 \
  --input-rate 800 \
  --records-per-tenant 3000 \
  --dataset iot-sensors \
  --tenant-id tenant-iot
```

Runs all five modes (streambazaar, talos, ds2, capsys, flink_default) and
writes the comparison report to:
`evaluation/results/true_baseline_runs/run_YYYYMMDD_HHMMSS/true_measured_improvement_report.txt`

---

## Dashboards

| Dashboard | URL | Login |
|-----------|-----|-------|
| Prometheus | http://localhost:19090 | — |
| Grafana | http://localhost:13000 | admin / admin |

---

## Switch scheduler mode at runtime

```bash
SCHEDULER_MODE=talos docker compose up -d --build stream-coordinator
./scripts/wait-for-services.sh
curl -fsS http://localhost:18085/health | python3 -c \
  "import json,sys; d=json.load(sys.stdin); print(d['scheduler_mode'])"
```

Available modes: `streambazaar` · `talos` · `ds2` · `capsys` · `flink_default`

---

## Verify Redis shared state

```bash
# Check virtual currency balances (updated each auction cycle)
docker compose exec -T redis redis-cli HGETALL streambazaar:virtual_balance

# Check latest bids submitted to the auction
docker compose exec -T redis redis-cli HGETALL streambazaar:bids

# Check price smoothing history
docker compose exec -T redis redis-cli HGETALL streambazaar:prices
```

---

## Stop everything

```bash
docker compose down

# Optional: remove all experiment outputs
rm -rf evaluation/results/csv/prometheus_metrics_*.csv
rm -rf evaluation/results/raw/exp_*
rm -rf evaluation/results/true_baseline_runs/run_*
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Kafka topic command fails | Use `kafka-topics` not `kafka-topics.sh` inside the container |
| `wait-for-services.sh` hangs | Run `docker compose logs <service>` to find the error |
| Prometheus shows no data | Check `http://localhost:19090/api/v1/targets` for `health: "up"` |
| Redis not storing balances | Run `docker compose logs stream-coordinator \| tail -20` |
| Scheduler mode not switching | Restart only `stream-coordinator` with the new env var |
