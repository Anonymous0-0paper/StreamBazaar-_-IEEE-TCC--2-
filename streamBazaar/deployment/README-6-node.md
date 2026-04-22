# StreamBazaar — 6-Node Deployment

Six machines (or simulated containers on one machine).
Kafka partitions: **6**. Each node owns **2 tenants** (12 tenants total).

---

## Architecture

```
┌─────────────────────────┐
│  node-0 (infra + shard) │   Kafka(6p) · Redis · Postgres
│  tenant-fraud           │   Prometheus · Grafana
│  tenant-fraud-2         │   auction-orchestrator
└────────────┬────────────┘
             │ shared Redis + Kafka
   ┌─────────┼──────────────────────────────┐
   ▼         ▼         ▼         ▼    ▼    ▼
node-0     node-1    node-2   node-3  node-4  node-5
fraud      click     ml       iot     fraud-2  web
fraud-2    click-3   ml-3     iot-3   intrus.  iot-2
```

**Tenant assignment:**

| Node | Tenants |
|------|---------|
| node-0 | tenant-fraud, tenant-fraud-2 |
| node-1 | tenant-clickstream, tenant-clickstream-3 |
| node-2 | tenant-ml, tenant-ml-3 |
| node-3 | tenant-iot, tenant-iot-3 |
| node-4 | tenant-web, tenant-intrusion |
| node-5 | tenant-iot-2, tenant-fraud-3 |

---

## Prerequisites (all 6 machines)

- Docker >= 24 + Docker Compose V2
- Python 3.9+ on node-0
- All machines reachable from each other over TCP
- Open on node-0: 19092 (Kafka), 6379 (Redis), 18080 (auction), 19090 (Prometheus), 13000 (Grafana)
- Open on each node: stream-coordinator port (see table below)

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
NODE_COUNT=6 bash scripts/run-distributed.sh start

# Check all 6 shards
NODE_COUNT=6 bash scripts/run-distributed.sh status
```

Skip to [Step 7 — Send workload](#step-7--send-workload).

---

## Real 6-machine deployment

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

### Step 3 — Create Kafka topics with 6 partitions

```bash
# On node-0
TENANT_IDS=tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot,tenant-fraud-2,tenant-clickstream-3,tenant-ml-3,tenant-iot-3,tenant-web,tenant-intrusion,tenant-iot-2,tenant-fraud-3 \
PARTITIONS=6 \
  bash ./scripts/create-kafka-topics.sh
```

---

### Step 4 — Start each worker node

**Automated (single machine):**

```bash
NODE_COUNT=6 bash scripts/run-distributed.sh start
```

**Manual (separate machines) — run on each respective machine:**

For each node, create `deployment/node-overrides/node-${NODE_ID}.yml` and start the workers.

#### node-0 (tenant-fraud, tenant-fraud-2)

```bash
NODE_ID=0; SC_PORT=18085; PE_PORT=18081
TENANT_SHARD="tenant-fraud,tenant-fraud-2"
```

```yaml
# deployment/node-overrides/node-0.yml
services:
  stream-coordinator:
    container_name: sb-stream-coordinator-node0
    ports:
      - "18085:8085"
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      REDIS_URL: redis://redis:6379/0
      TENANT_IDS: tenant-fraud,tenant-fraud-2
      SCHEDULER_MODE: streambazaar
      COORDINATOR_GROUP_ID: stream-coordinator-node0
      NODE_ID: "0"
      CLUSTER_SLOTS: "30"
      PRICING_URL: http://pricing-engine-node0:8081/price
      BID_URL: http://auction-orchestrator:8080/bid
      CLEAR_URL: http://auction-orchestrator:8080/auction/clear
      ALLOCATE_URL: http://resource-allocator-node0:8083/allocate
      MIGRATE_URL: http://migration-coordinator-node0:8084/migrate
  pricing-engine:
    container_name: sb-pricing-engine-node0
    hostname: pricing-engine-node0
    environment:
      REDIS_URL: redis://redis:6379/0
  resource-allocator:
    container_name: sb-resource-allocator-node0
    hostname: resource-allocator-node0
  migration-coordinator:
    container_name: sb-migration-coordinator-node0
    hostname: migration-coordinator-node0
```

#### node-1 through node-5 (on separate machines)

Replace `${NODE0_IP}` with the actual IP of node-0.

```yaml
# deployment/node-overrides/node-1.yml
services:
  stream-coordinator:
    container_name: sb-stream-coordinator-node1
    ports:
      - "18095:8085"
    environment:
      KAFKA_BOOTSTRAP_SERVERS: ${NODE0_IP}:19092
      REDIS_URL: redis://${NODE0_IP}:6379/0
      TENANT_IDS: tenant-clickstream,tenant-clickstream-3
      SCHEDULER_MODE: streambazaar
      COORDINATOR_GROUP_ID: stream-coordinator-node1
      NODE_ID: "1"
      CLUSTER_SLOTS: "30"
      PRICING_URL: http://pricing-engine-node1:8081/price
      BID_URL: http://${NODE0_IP}:18080/bid
      CLEAR_URL: http://${NODE0_IP}:18080/auction/clear
      ALLOCATE_URL: http://resource-allocator-node1:8083/allocate
      MIGRATE_URL: http://migration-coordinator-node1:8084/migrate
  pricing-engine:
    container_name: sb-pricing-engine-node1
    hostname: pricing-engine-node1
    environment:
      REDIS_URL: redis://${NODE0_IP}:6379/0
  resource-allocator:
    container_name: sb-resource-allocator-node1
    hostname: resource-allocator-node1
  migration-coordinator:
    container_name: sb-migration-coordinator-node1
    hostname: migration-coordinator-node1
```

Repeat for nodes 2–5, incrementing `NODE_ID`, container names, ports (`18105`, `18115`, `18125`, `18135`), and `TENANT_IDS` per the table above.

**Start command (each machine):**

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

### Step 6 — Verify all 6 nodes

```bash
for port in 18085 18095 18105 18115 18125 18135; do
  echo -n "port ${port}: "
  curl -fsS http://localhost:${port}/health 2>/dev/null | \
    python3 -c "import json,sys; d=json.load(sys.stdin); \
    print('mode='+d['scheduler_mode'], 'tenants='+str(d['tenants']), 'running='+str(d['running']))" \
    2>/dev/null || echo "unreachable"
done
```

Verify Redis has all 12 tenant balances:

```bash
docker compose exec -T redis redis-cli HGETALL streambazaar:virtual_balance | wc -l
# Expected: 24 lines (12 keys × 2 lines each)
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
  --tenants tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot,tenant-fraud-2,tenant-clickstream-3,tenant-ml-3,tenant-iot-3,tenant-web,tenant-intrusion,tenant-iot-2,tenant-fraud-3 \
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
| node-4 | tenant-web, tenant-intrusion | 18125 | 18121 |
| node-5 | tenant-iot-2, tenant-fraud-3 | 18135 | 18131 |

---

## Dashboards

| Dashboard | URL |
|-----------|-----|
| Prometheus | http://\<NODE0_IP\>:19090 |
| Grafana | http://\<NODE0_IP\>:13000 (admin/admin) |

---

## Stop

```bash
NODE_COUNT=6 bash scripts/run-distributed.sh stop
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| A node shows 0 consumed events | Check `TENANT_IDS` env var; confirm Kafka partition count is 6 |
| Auction clearing seen on multiple nodes | Redis lock race; check Redis connectivity from all nodes |
| Tenant balance not updating | Verify `REDIS_URL` points to node-0; check `docker logs sb-stream-coordinator-nodeN` |
| Port conflict on single machine | All ports are offset by 10 per node (18085 … 18135) |
| node-N cannot reach node-0 | Run `nc -zv <NODE0_IP> 19092` and `nc -zv <NODE0_IP> 6379` from that node |
