# Scripts

## Aim
Operational scripts to initialize topics/tenants and run end-to-end smoke checks.

## What You Can Change
- Tenant count/topic names: edit env vars for `create-kafka-topics.sh`.
- Workload shape/rate/bytes: flags in `run_workloads.py`.
- Health checks: endpoints in `wait-for-services.sh`.

## Files
- `create-kafka-topics.sh`: Creates Kafka topics for configured tenants.
- `run_workloads.py`: Publishes synthetic events with configurable dataset/tenant/rate/payload bytes.
- `init-tenants.py` / `init-tenants.sql`: Tenant metadata bootstrap in PostgreSQL.
- `e2e_stream_test.sh`: End-to-end pipeline validation.
- `smoke_test.sh`: Full smoke checks.
- `wait-for-services.sh`: Polls service health.

## Run
```bash
./scripts/create-kafka-topics.sh
python3 scripts/run_workloads.py --duration-sec 30 --datasets fraud,clickstream,ml --records-per-tenant 100
./scripts/e2e_stream_test.sh
```

## Impact
Changing these scripts directly changes input traffic, benchmark pressure, and topic topology used by all downstream services.
