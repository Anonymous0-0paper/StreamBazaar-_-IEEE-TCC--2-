# StreamBazaar — 12-Node Deployment

Twelve machines (or simulated containers on one machine).
Kafka partitions: **12**. 16 tenants across 12 nodes (4 nodes get 2 tenants, 8 nodes get 1 tenant).

---

## Architecture

```
┌─────────────────────────┐
│  node-0 (infra + shard) │   Kafka(12p) · Redis · Postgres
│  tenant-fraud           │   Prometheus · Grafana
│  tenant-ml-4            │   auction-orchestrator
└────────────┬────────────┘
             │ shared Redis + Kafka
  node-0 … node-11  (12 total worker shards)
```

**Tenant assignment (round-robin over 16 tenants):**

| Node | Tenants |
|------|---------|
| node-0 | tenant-fraud, tenant-ml-4 |
| node-1 | tenant-clickstream, tenant-iot-4 |
| node-2 | tenant-ml, tenant-fraud-4 |
| node-3 | tenant-iot, tenant-clickstream-4 |
| node-4 | tenant-fraud-2 |
| node-5 | tenant-web |
| node-6 | tenant-intrusion |
| node-7 | tenant-iot-2 |
| node-8 | tenant-fraud-3 |
| node-9 | tenant-clickstream-3 |
| node-10 | tenant-ml-3 |
| node-11 | tenant-iot-3 |

---

## Prerequisites (all 12 machines)

- Docker >= 24 + Docker Compose V2
- Python 3.9+ on node-0
- All machines reachable from each other over TCP
- Open on node-0: 19092 (Kafka), 6379 (Redis), 18080 (auction), 19090 (Prometheus), 13000 (Grafana)
- Open on each worker node: stream-coordinator port (see table below)

```bash
# Verify connectivity from worker nodes to node-0
nc -zv <NODE0_IP> 19092   # Kafka
nc -zv <NODE0_IP> 6379    # Redis
nc -zv <NODE0_IP> 18080   # auction-orchestrator
```

---

## Quick start — single machine simulation

```bash
cd "/home/user/Downloads/StreamBazaar _ IEEE TCC (2)/streamBazaar"
NODE_COUNT=12 bash scripts/run-distributed.sh start

# Check all 12 shards
NODE_COUNT=12 bash scripts/run-distributed.sh status
```

Skip to [Step 7 — Send workload](#step-7--send-workload).

---

## Real 12-machine deployment

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

### Step 3 — Create Kafka topics with 12 partitions

```bash
# On node-0
TENANT_IDS=tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot,tenant-fraud-2,tenant-web,tenant-intrusion,tenant-iot-2,tenant-fraud-3,tenant-clickstream-3,tenant-ml-3,tenant-iot-3,tenant-fraud-4,tenant-clickstream-4,tenant-ml-4,tenant-iot-4 \
PARTITIONS=12 \
  bash ./scripts/create-kafka-topics.sh
```

---

### Step 4 — Start each worker node

**Automated (single machine):**

```bash
NODE_COUNT=12 bash scripts/run-distributed.sh start
```

**Manual (separate machines):**

Write `deployment/node-overrides/node-${NODE_ID}.yml` for each node and start with:

```bash
docker compose \
  -f docker-compose.distributed.yml \
  -f deployment/node-overrides/node-${NODE_ID}.yml \
  up -d --build \
  pricing-engine resource-allocator migration-coordinator stream-coordinator
```

**Override template:**

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
| 0 | 18085 | 18081 | tenant-fraud,tenant-ml-4 |
| 1 | 18095 | 18091 | tenant-clickstream,tenant-iot-4 |
| 2 | 18105 | 18101 | tenant-ml,tenant-fraud-4 |
| 3 | 18115 | 18111 | tenant-iot,tenant-clickstream-4 |
| 4 | 18125 | 18121 | tenant-fraud-2 |
| 5 | 18135 | 18131 | tenant-web |
| 6 | 18145 | 18141 | tenant-intrusion |
| 7 | 18155 | 18151 | tenant-iot-2 |
| 8 | 18165 | 18161 | tenant-fraud-3 |
| 9 | 18175 | 18171 | tenant-clickstream-3 |
| 10 | 18185 | 18181 | tenant-ml-3 |
| 11 | 18195 | 18191 | tenant-iot-3 |

> **node-0**: Use `kafka:9092` / `redis://redis:6379/0` (Docker internal hostnames) instead of external IPs.

---

### Step 5 — Initialize tenants

```bash
# On node-0
python3 scripts/init-tenants.py
```

---

### Step 6 — Verify all 12 nodes

```bash
for port in 18085 18095 18105 18115 18125 18135 18145 18155 18165 18175 18185 18195; do
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
  --tenants tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot,tenant-fraud-2,tenant-web,tenant-intrusion,tenant-iot-2,tenant-fraud-3,tenant-clickstream-3,tenant-ml-3,tenant-iot-3,tenant-fraud-4,tenant-clickstream-4,tenant-ml-4,tenant-iot-4 \
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
| node-0 | tenant-fraud, tenant-ml-4 | 18085 | 18081 |
| node-1 | tenant-clickstream, tenant-iot-4 | 18095 | 18091 |
| node-2 | tenant-ml, tenant-fraud-4 | 18105 | 18101 |
| node-3 | tenant-iot, tenant-clickstream-4 | 18115 | 18111 |
| node-4 | tenant-fraud-2 | 18125 | 18121 |
| node-5 | tenant-web | 18135 | 18131 |
| node-6 | tenant-intrusion | 18145 | 18141 |
| node-7 | tenant-iot-2 | 18155 | 18151 |
| node-8 | tenant-fraud-3 | 18165 | 18161 |
| node-9 | tenant-clickstream-3 | 18175 | 18171 |
| node-10 | tenant-ml-3 | 18185 | 18181 |
| node-11 | tenant-iot-3 | 18195 | 18191 |

---

## Dashboards

| Dashboard | URL |
|-----------|-----|
| Prometheus | http://\<NODE0_IP\>:19090 |
| Grafana | http://\<NODE0_IP\>:13000 (admin/admin) |

---

## Stop

```bash
NODE_COUNT=12 bash scripts/run-distributed.sh stop
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| A node shows 0 consumed events | Check `TENANT_IDS` env var; confirm Kafka partition count is 12 |
| Auction clearing seen on multiple nodes | Redis lock race; verify Redis connectivity from all nodes |
| Tenant balance not updating | Verify `REDIS_URL` points to node-0; check `docker logs sb-stream-coordinator-nodeN` |
| Port conflict on single machine | All ports offset by 10 per node (18085 … 18195) |
| node-N cannot reach node-0 | Run `nc -zv <NODE0_IP> 19092` and `nc -zv <NODE0_IP> 6379` from that node |
