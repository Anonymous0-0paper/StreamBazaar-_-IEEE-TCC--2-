# Services

## Aim
Control-plane microservices for pricing, auction, allocation, migration, tenant management, and stream coordination.

## What You Can Change
- Business logic: each service `app/main.py`.
- Metrics: add Prometheus counters/gauges in service files.
- Ports and service wiring: `docker-compose.yml`.

## Service Folders
- `auction-orchestrator/`: bid registration + clearing.
- `pricing-engine/`: dynamic bid floor.
- `resource-allocator/`: weighted fair allocation.
- `migration-coordinator/`: migration decisions.
- `tenant-manager/`: tenant config lifecycle.
- `stream-coordinator/`: live loop + Kafka integration + advanced KPIs.
- `monitoring/`: Prometheus container config.
- `flink-cluster/`: optional Flink runtime image.

## Run
```bash
docker compose up -d --build
curl -fsS http://localhost:18085/health
curl -fsS http://localhost:18085/metrics | head
```

## Impact
Changes in these services alter SLA behavior, fairness, allocation outcomes, and observability data used in experiments.
