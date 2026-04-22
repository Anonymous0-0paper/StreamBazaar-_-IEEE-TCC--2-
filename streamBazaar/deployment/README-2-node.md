# StreamBazaar — 2-Node Deployment

Two machines (or two simulated nodes on one machine).
Kafka partitions: **2**. Each node owns **2 tenants**.

---

## Architecture

```
┌──────────────────────────────┐   ┌──────────────────────────────┐
│         node-0 (infra)       │   │         node-1 (worker)      │
│                              │   │                              │
│  Kafka (2 partitions)        │   │  stream-coordinator          │
│  Zookeeper · Redis           │◄──┤  pricing-engine              │
│  Postgres · Prometheus       │   │  resource-allocator          │
│  Grafana                     │   │  migration-coordinator       │
│  auction-orchestrator        │   │                              │
│  tenant-manager              │   │  Tenants:                    │
│                              │   │  tenant-ml, tenant-iot       │
│  stream-coordinator          │   └──────────────────────────────┘
│  pricing-engine              │
│  resource-allocator          │
│  migration-coordinator       │
│                              │
│  Tenants:                    │
│  tenant-fraud,               │
│  tenant-clickstream          │
└──────────────────────────────┘
```

Shared state (Redis): virtual balances, bid history, price smoothing history.
The single auction-orchestrator on node-0 clears bids from both nodes using
a Redis distributed lock to prevent duplicate clearing.

---

## Prerequisites (both machines)

- Docker >= 24 and Docker Compose V2
- Python 3.9+ (on node-0 for scripts)
- Machines must be able to reach each other over TCP
- Ports open on node-0: 19092 (Kafka), 6379 (Redis), 18080 (auction), 19090 (Prometheus), 13000 (Grafana)
- Ports open on node-1: 18085+10=18095 (stream-coordinator), 18081+10=18091 (pricing-engine)

```bash
# Check connectivity from node-1 to node-0
ping <NODE0_IP>
nc -zv <NODE0_IP> 19092   # Kafka
nc -zv <NODE0_IP> 6379    # Redis
```

---

## Single-machine simulation

If you only have one machine, this command runs both nodes as separate containers:

```bash
cd "/home/user/Downloads/StreamBazaar _ IEEE TCC (2)/streamBazaar"
NODE_COUNT=2 bash scripts/run-distributed.sh start
```

Then skip to [Step 5 — Verify](#step-5--verify-all-nodes-are-running).

---

## Real two-machine deployment

### Step 1 — Set environment variables (both machines)

```bash
# Replace with your actual node-0 IP
export NODE0_IP=192.168.1.10
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
```

Wait for Kafka:

```bash
until docker compose exec -T kafka \
  kafka-topics --bootstrap-server kafka:9092 --list &>/dev/null; do
  echo "waiting for kafka..."; sleep 3
done
echo "Kafka ready"
```

---

### Step 3 — Create Kafka topics with 2 partitions

```bash
# On node-0
TENANT_IDS=tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot \
PARTITIONS=2 \
  bash ./scripts/create-kafka-topics.sh
```

Verify:

```bash
docker compose exec -T kafka kafka-topics \
  --bootstrap-server kafka:9092 --describe --topic tenant.tenant-fraud.input
```

Expected: `PartitionCount: 2`

---

### Step 4 — Start workers

**node-0 (tenants: tenant-fraud, tenant-clickstream):**

```bash
# On node-0 — generate override then start
mkdir -p deployment/node-overrides
cat > deployment/node-overrides/node-0.yml <<'EOF'
services:
  stream-coordinator:
    container_name: sb-stream-coordinator-node0
    ports:
      - "18085:8085"
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      REDIS_URL: redis://redis:6379/0
      TENANT_IDS: tenant-fraud,tenant-clickstream
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
EOF

docker compose \
  -f docker-compose.distributed.yml \
  -f deployment/node-overrides/node-0.yml \
  up -d --build \
  pricing-engine resource-allocator migration-coordinator stream-coordinator
```

**node-1 (tenants: tenant-ml, tenant-iot):**

```bash
# On node-1 — copy the project directory, then:
cd "/home/user/Downloads/StreamBazaar _ IEEE TCC (2)/streamBazaar"
mkdir -p deployment/node-overrides

cat > deployment/node-overrides/node-1.yml <<EOF
services:
  stream-coordinator:
    container_name: sb-stream-coordinator-node1
    ports:
      - "18095:8085"
    environment:
      KAFKA_BOOTSTRAP_SERVERS: ${NODE0_IP}:19092
      REDIS_URL: redis://${NODE0_IP}:6379/0
      TENANT_IDS: tenant-ml,tenant-iot
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
EOF

docker compose \
  -f docker-compose.distributed.yml \
  -f deployment/node-overrides/node-1.yml \
  up -d --build \
  pricing-engine resource-allocator migration-coordinator stream-coordinator
```

---

### Step 5 — Verify all nodes are running

```bash
# node-0 shard
curl -fsS http://localhost:18085/health | python3 -c \
  "import json,sys; d=json.load(sys.stdin); \
   print('node-0 | mode:', d['scheduler_mode'], '| tenants:', d['tenants'], '| running:', d['running'])"

# node-1 shard (from node-0, replace with node-1 IP if on separate machines)
curl -fsS http://localhost:18095/health | python3 -c \
  "import json,sys; d=json.load(sys.stdin); \
   print('node-1 | mode:', d['scheduler_mode'], '| tenants:', d['tenants'], '| running:', d['running'])"
```

Check Redis has balances for all 4 tenants:

```bash
docker compose exec -T redis redis-cli HGETALL streambazaar:virtual_balance
```

Expected: 4 entries (one per tenant, updated each auction cycle).

---

### Step 6 — Initialize tenants

```bash
# On node-0
python3 scripts/init-tenants.py
```

---

### Step 7 — Send workload

```bash
# Send to both shards simultaneously
python3 scripts/run_workloads.py \
  --datasets fraud,iot-sensors \
  --tenant-ids tenant-fraud,tenant-iot \
  --records-per-dataset fraud=30000,iot-sensors=30000 \
  --input-rates fraud=100000,iot-sensors=80000 \
  --duration-sec 120 \
  --disable-synthetic-fallback \
  --skip-download
```

---

### Step 8 — Export metrics and generate results

```bash
python3 evaluation/export_prometheus_csv.py \
  --duration-sec 120 \
  --interval-sec 1 \
  --tenants tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot \
  --out-dir evaluation/results/csv
```

---

### Step 9 — Run paper experiments

```bash
python3 evaluation/run_paper_experiments.py \
  --runs 2 \
  --warmup-sec 15 \
  --steady-sec 60 \
  --records-per-tenant 300 \
  --csv-monitor-sec 60
```

---

## Tenant assignment summary

| Node | Tenants | Port |
|------|---------|------|
| node-0 | tenant-fraud, tenant-clickstream | 18085 |
| node-1 | tenant-ml, tenant-iot | 18095 |

---

## Dashboards

| Dashboard | URL |
|-----------|-----|
| Prometheus | http://\<NODE0_IP\>:19090 |
| Grafana | http://\<NODE0_IP\>:13000 (admin/admin) |

---

## Stop everything

```bash
# On node-0
NODE_COUNT=2 bash scripts/run-distributed.sh stop
docker compose down

# On node-1
docker compose -f docker-compose.distributed.yml \
  -f deployment/node-overrides/node-1.yml down
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| node-1 cannot reach Kafka | Check `nc -zv <NODE0_IP> 19092`; ensure port 19092 is open |
| node-1 cannot reach Redis | Check `nc -zv <NODE0_IP> 6379`; ensure port 6379 is open |
| Only node-0 tenants appear in Prometheus | node-1 coordinator not running; check `docker logs sb-stream-coordinator-node1` |
| Duplicate auction clearing | Redis lock working correctly; second node skips and reuses last result |
| Wrong tenant on wrong node | Check `TENANT_IDS` env var; run `curl http://localhost:<port>/health` |
