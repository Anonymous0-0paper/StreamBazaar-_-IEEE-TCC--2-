# StreamBazaar — 4-Node Deployment

Four machines (or simulated containers on one machine).
Kafka partitions: **4**. Each node owns **2 tenants**.

---

## Architecture

```
┌─────────────────────────┐
│  node-0 (infra + shard) │   Kafka(4p) · Redis · Postgres
│  tenant-fraud           │   Prometheus · Grafana
│  tenant-clickstream     │   auction-orchestrator
└────────────┬────────────┘
             │ shared Redis + Kafka
   ┌─────────┼─────────┬──────────┐
   ▼         ▼         ▼          ▼
node-0     node-1    node-2     node-3
fraud      web       intrusion  iot
clickstr.  fraud-2   ml-3       iot-2
```

**Kafka partition assignment (automatic via consumer group):**
Each stream-coordinator has a unique `COORDINATOR_GROUP_ID` so Kafka assigns
it exactly the partitions whose keys hash to its shard.

---

## Prerequisites (all 4 machines)

- Docker >= 24 + Docker Compose V2
- Python 3.9+ on node-0
- All machines reachable from each other
- Open on node-0: 19092, 6379, 18080, 19090, 13000

---

## Quick start — single machine simulation

```bash
cd "/home/user/Downloads/StreamBazaar _ IEEE TCC (2)/streamBazaar"
NODE_COUNT=4 bash scripts/run-distributed.sh start

# Check all 4 shards
NODE_COUNT=4 bash scripts/run-distributed.sh status
```

Skip to [Step 7 — Send workload](#step-7--send-workload).

---

## Real 4-machine deployment

### Step 1 — Set environment variables (all machines)

```bash
export NODE0_IP=192.168.1.10   # change to real IP
export INFRA_HOST=$NODE0_IP
export KAFKA_BOOTSTRAP="${NODE0_IP}:19092"
export REDIS_URL="redis://${NODE0_IP}:6379/0"
export SCHEDULER_MODE=streambazaar
```

---

### Step 2 — Start infrastructure on node-0

```bash
# On node-0
cd "/home/user/Downloads/StreamBazaar _ IEEE TCC (2)/streamBazaar"

docker compose up -d --build \
  zookeeper kafka redis postgres \
  auction-orchestrator tenant-manager \
  prometheus grafana

until docker compose exec -T kafka \
  kafka-topics --bootstrap-server kafka:9092 --list &>/dev/null; do
  echo "waiting for kafka..."; sleep 3
done
echo "Kafka ready"
```

---

### Step 3 — Create Kafka topics with 4 partitions

```bash
# On node-0
TENANT_IDS=tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot,tenant-fraud-2,tenant-web,tenant-intrusion,tenant-iot-2 \
PARTITIONS=4 \
  bash ./scripts/create-kafka-topics.sh
```

---

### Step 4 — Start each worker node

Use the run-distributed script to generate and launch each node's override
automatically, OR manually deploy using the pattern below.

**Automated (single machine):**

```bash
NODE_COUNT=4 bash scripts/run-distributed.sh start
```

**Manual (separate machines) — run on each respective machine:**

```bash
# node-0: tenant-fraud, tenant-web
TENANT_SHARD="tenant-fraud,tenant-web"
NODE_ID=0
SC_PORT=18085; PE_PORT=18081

# node-1: tenant-clickstream, tenant-fraud-2
TENANT_SHARD="tenant-clickstream,tenant-fraud-2"
NODE_ID=1
SC_PORT=18095; PE_PORT=18091

# node-2: tenant-ml, tenant-intrusion
TENANT_SHARD="tenant-ml,tenant-intrusion"
NODE_ID=2
SC_PORT=18105; PE_PORT=18101

# node-3: tenant-iot, tenant-iot-2
TENANT_SHARD="tenant-iot,tenant-iot-2"
NODE_ID=3
SC_PORT=18115; PE_PORT=18111
```

On each machine, create `deployment/node-overrides/node-${NODE_ID}.yml`:

```yaml
services:
  stream-coordinator:
    container_name: sb-stream-coordinator-node${NODE_ID}
    ports:
      - "${SC_PORT}:8085"
    environment:
      KAFKA_BOOTSTRAP_SERVERS: ${NODE0_IP}:19092
      REDIS_URL: redis://${NODE0_IP}:6379/0
      TENANT_IDS: ${TENANT_SHARD}
      SCHEDULER_MODE: streambazaar
      COORDINATOR_GROUP_ID: stream-coordinator-node${NODE_ID}
      NODE_ID: "${NODE_ID}"
      CLUSTER_SLOTS: "30"
      PRICING_URL: http://pricing-engine-node${NODE_ID}:8081/price
      BID_URL: http://${NODE0_IP}:18080/bid
      CLEAR_URL: http://${NODE0_IP}:18080/auction/clear
      ALLOCATE_URL: http://resource-allocator-node${NODE_ID}:8083/allocate
      MIGRATE_URL: http://migration-coordinator-node${NODE_ID}:8084/migrate
  pricing-engine:
    container_name: sb-pricing-engine-node${NODE_ID}
    hostname: pricing-engine-node${NODE_ID}
    environment:
      REDIS_URL: redis://${NODE0_IP}:6379/0
  resource-allocator:
    container_name: sb-resource-allocator-node${NODE_ID}
    hostname: resource-allocator-node${NODE_ID}
  migration-coordinator:
    container_name: sb-migration-coordinator-node${NODE_ID}
    hostname: migration-coordinator-node${NODE_ID}
```

Then start:

```bash
docker compose \
  -f docker-compose.distributed.yml \
  -f deployment/node-overrides/node-${NODE_ID}.yml \
  up -d --build \
  pricing-engine resource-allocator migration-coordinator stream-coordinator
```

---

### Step 5 — Initialize tenants

```bash
# On node-0
python3 scripts/init-tenants.py
```

---

### Step 6 — Verify all 4 nodes

```bash
for port in 18085 18095 18105 18115; do
  echo -n "port ${port}: "
  curl -fsS http://localhost:${port}/health 2>/dev/null | \
    python3 -c "import json,sys; d=json.load(sys.stdin); \
    print('mode='+d['scheduler_mode'], 'tenants='+str(d['tenants']), 'running='+str(d['running']))" \
    2>/dev/null || echo "unreachable"
done
```

Verify Redis has all 8 tenant balances:

```bash
docker compose exec -T redis redis-cli HGETALL streambazaar:virtual_balance | wc -l
# Expected: 16 lines (8 keys × 2 lines each: key + value)
```

---

### Step 7 — Send workload

```bash
python3 scripts/run_workloads.py \
  --datasets fraud,web-analytics,network-intrusion,iot-sensors \
  --tenant-ids tenant-fraud,tenant-web,tenant-intrusion,tenant-iot \
  --records-per-dataset fraud=50000,web-analytics=200000,network-intrusion=60000,iot-sensors=60000 \
  --input-rates fraud=120000,web-analytics=500000,network-intrusion=100000,iot-sensors=80000 \
  --duration-sec 120 \
  --disable-synthetic-fallback \
  --skip-download
```

---

### Step 8 — Export metrics

```bash
python3 evaluation/export_prometheus_csv.py \
  --duration-sec 120 \
  --interval-sec 1 \
  --tenants tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot,tenant-fraud-2,tenant-web,tenant-intrusion,tenant-iot-2 \
  --out-dir evaluation/results/csv
```

---

### Step 9 — Run paper experiments

```bash
python3 evaluation/run_paper_experiments.py \
  --runs 3 \
  --warmup-sec 20 \
  --steady-sec 60 \
  --records-per-tenant 500 \
  --csv-monitor-sec 60
```

---

## Tenant assignment summary

| Node | Tenants | Coordinator port | Pricing port |
|------|---------|-----------------|--------------|
| node-0 | tenant-fraud, tenant-web | 18085 | 18081 |
| node-1 | tenant-clickstream, tenant-fraud-2 | 18095 | 18091 |
| node-2 | tenant-ml, tenant-intrusion | 18105 | 18101 |
| node-3 | tenant-iot, tenant-iot-2 | 18115 | 18111 |

---

## Dashboards

| Dashboard | URL |
|-----------|-----|
| Prometheus | http://\<NODE0_IP\>:19090 |
| Grafana | http://\<NODE0_IP\>:13000 (admin/admin) |

---

## Stop

```bash
NODE_COUNT=4 bash scripts/run-distributed.sh stop
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| A node shows 0 consumed events | Check `TENANT_IDS` env var; confirm Kafka partition count is 4 |
| Auction clearing seen on multiple nodes | Redis lock race; check Redis connectivity from all nodes |
| Tenant balance not updating | Verify `REDIS_URL` points to node-0; check `docker logs sb-stream-coordinator-nodeN` |
| Port conflict on single machine | All ports are offset by 10 per node (18085, 18095, 18105, 18115) |
