# StreamBazaar — 8-Node Deployment

Eight machines (or simulated containers on one machine).
Kafka partitions: **8**. Each node owns **2 tenants** (16 tenants total).

---

## Architecture

```
┌─────────────────────────┐
│  node-0 (infra + shard) │   Kafka(8p) · Redis · Postgres
│  tenant-fraud           │   Prometheus · Grafana
│  tenant-fraud-2         │   auction-orchestrator
└────────────┬────────────┘
             │ shared Redis + Kafka
   ┌─────────┼──── … ────────────────────────┐
  node-0   node-1  node-2  node-3  node-4  node-5  node-6  node-7
```

**Tenant assignment (round-robin across 16 tenants):**

| Node | Tenants |
|------|---------|
| node-0 | tenant-fraud, tenant-fraud-2 |
| node-1 | tenant-clickstream, tenant-clickstream-3 |
| node-2 | tenant-ml, tenant-ml-3 |
| node-3 | tenant-iot, tenant-iot-3 |
| node-4 | tenant-fraud-3, tenant-fraud-4 |
| node-5 | tenant-clickstream-4, tenant-ml-4 |
| node-6 | tenant-web, tenant-intrusion |
| node-7 | tenant-iot-2, tenant-iot-4 |

---

## Prerequisites (all 8 machines)

- Docker >= 24 + Docker Compose V2
- Python 3.9+ on node-0
- All machines reachable from each other over TCP
- Open on node-0: 19092 (Kafka), 6379 (Redis), 18080 (auction), 19090 (Prometheus), 13000 (Grafana)
- Open on each worker node: stream-coordinator port (see table below)

```bash
# Verify connectivity from each worker node to node-0
nc -zv <NODE0_IP> 19092   # Kafka
nc -zv <NODE0_IP> 6379    # Redis
nc -zv <NODE0_IP> 18080   # auction-orchestrator
```

---

## Quick start — single machine simulation

```bash
cd "/home/user/Downloads/StreamBazaar _ IEEE TCC (2)/streamBazaar"
NODE_COUNT=8 bash scripts/run-distributed.sh start

# Check all 8 shards
NODE_COUNT=8 bash scripts/run-distributed.sh status
```

Skip to [Step 7 — Send workload](#step-7--send-workload).

---

## Real 8-machine deployment

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

### Step 3 — Create Kafka topics with 8 partitions

```bash
# On node-0
TENANT_IDS=tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot,tenant-fraud-2,tenant-clickstream-3,tenant-ml-3,tenant-iot-3,tenant-fraud-3,tenant-fraud-4,tenant-clickstream-4,tenant-ml-4,tenant-web,tenant-intrusion,tenant-iot-2,tenant-iot-4 \
PARTITIONS=8 \
  bash ./scripts/create-kafka-topics.sh
```

---

### Step 4 — Start each worker node

**Automated (single machine):**

```bash
NODE_COUNT=8 bash scripts/run-distributed.sh start
```

**Manual (separate machines):**

For each node, write `deployment/node-overrides/node-${NODE_ID}.yml` following the pattern below, then start:

```bash
docker compose \
  -f docker-compose.distributed.yml \
  -f deployment/node-overrides/node-${NODE_ID}.yml \
  up -d --build \
  pricing-engine resource-allocator migration-coordinator stream-coordinator
```

**Override template (substitute NODE_ID, SC_PORT, PE_PORT, TENANT_SHARD):**

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

**Per-node values:**

| NODE_ID | SC_PORT | PE_PORT | TENANT_SHARD |
|---------|---------|---------|--------------|
| 0 | 18085 | 18081 | tenant-fraud,tenant-fraud-2 |
| 1 | 18095 | 18091 | tenant-clickstream,tenant-clickstream-3 |
| 2 | 18105 | 18101 | tenant-ml,tenant-ml-3 |
| 3 | 18115 | 18111 | tenant-iot,tenant-iot-3 |
| 4 | 18125 | 18121 | tenant-fraud-3,tenant-fraud-4 |
| 5 | 18135 | 18131 | tenant-clickstream-4,tenant-ml-4 |
| 6 | 18145 | 18141 | tenant-web,tenant-intrusion |
| 7 | 18155 | 18151 | tenant-iot-2,tenant-iot-4 |

> **node-0 special case**: Use `kafka:9092` and `redis://redis:6379/0` (Docker internal hostnames) instead of `${NODE0_IP}:19092` / `redis://${NODE0_IP}:6379/0`, since node-0 runs the infra services.

---

### Step 5 — Initialize tenants

```bash
# On node-0
python3 scripts/init-tenants.py
```

---

### Step 6 — Verify all 8 nodes

```bash
for port in 18085 18095 18105 18115 18125 18135 18145 18155; do
  echo -n "port ${port}: "
  curl -fsS http://localhost:${port}/health 2>/dev/null | \
    python3 -c "import json,sys; d=json.load(sys.stdin); \
    print('mode='+d['scheduler_mode'], 'tenants='+str(d['tenants']), 'running='+str(d['running']))" \
    2>/dev/null || echo "unreachable"
done
```

Verify Redis has all 16 tenant balances:

```bash
docker compose exec -T redis redis-cli HGETALL streambazaar:virtual_balance | wc -l
# Expected: 32 lines (16 keys × 2 lines each)
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
  --tenants tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot,tenant-fraud-2,tenant-clickstream-3,tenant-ml-3,tenant-iot-3,tenant-fraud-3,tenant-fraud-4,tenant-clickstream-4,tenant-ml-4,tenant-web,tenant-intrusion,tenant-iot-2,tenant-iot-4 \
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
| node-0 | tenant-fraud, tenant-fraud-2 | 18085 | 18081 |
| node-1 | tenant-clickstream, tenant-clickstream-3 | 18095 | 18091 |
| node-2 | tenant-ml, tenant-ml-3 | 18105 | 18101 |
| node-3 | tenant-iot, tenant-iot-3 | 18115 | 18111 |
| node-4 | tenant-fraud-3, tenant-fraud-4 | 18125 | 18121 |
| node-5 | tenant-clickstream-4, tenant-ml-4 | 18135 | 18131 |
| node-6 | tenant-web, tenant-intrusion | 18145 | 18141 |
| node-7 | tenant-iot-2, tenant-iot-4 | 18155 | 18151 |

---

## Dashboards

| Dashboard | URL |
|-----------|-----|
| Prometheus | http://\<NODE0_IP\>:19090 |
| Grafana | http://\<NODE0_IP\>:13000 (admin/admin) |

---

## Stop

```bash
NODE_COUNT=8 bash scripts/run-distributed.sh stop
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| A node shows 0 consumed events | Check `TENANT_IDS` env var; confirm Kafka partition count is 8 |
| Auction clearing seen on multiple nodes | Redis lock race; verify Redis connectivity from all nodes |
| Tenant balance not updating | Verify `REDIS_URL` points to node-0; check `docker logs sb-stream-coordinator-nodeN` |
| Port conflict on single machine | All ports offset by 10 per node (18085 … 18155) |
| node-N cannot reach node-0 | Run `nc -zv <NODE0_IP> 19092` from that node |
