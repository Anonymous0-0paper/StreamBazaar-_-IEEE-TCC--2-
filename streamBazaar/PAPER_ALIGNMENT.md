# StreamBazaar Paper Alignment Notes

This scaffold is designed to map directly to the paper's experimental requirements.

## Scheduler & Control Plane
- `services/auction-orchestrator`: receives and tracks tenant bids.
- `services/pricing-engine`: calculates dynamic bid floor using utilization and SLA pressure.
- `services/resource-allocator`: exposes allocation decision endpoint.
- `services/migration-coordinator`: exposes preemption/migration endpoint.
- `flink-integration/`: **NEW** Native Flink job operators implementing streaming control loop natively (replaces REST-based stream-coordinator for paper evaluation).

## Data & Storage
- PostgreSQL: tenant and operator metadata (`scripts/init-tenants.sql`).
- Kafka: stream transport and event bus (`streamBazaar.bids`, `streamBazaar.allocations`, etc.).
- InfluxDB: high-frequency latency/throughput/resource telemetry.

## Evaluation Metrics in Scope
- Latency distribution (`p50`, `p90`, `p95`, `p99`, `p99.9`).
- Throughput stability and variance.
- Tenant-level resource utilization.
- Auction/preemption event volume.

## Known Initial-Phase Gaps
- ~~Flink custom scheduler and auction client are placeholders~~ **DONE**: Native Flink job (`flink-integration/`) now implements StreamBazaarJob that consumes Kafka tenant inputs, applies pricing/auction/allocation/migration logic natively, and emits allocation decisions back to Kafka.
- Workload publishers have dual support: synthetic Kafka generators for E2E validation, and paper-ready experiment harness (`evaluation/run_paper_experiments.py`) with warmup/steady phases and statistical aggregation.
- Baseline scheduler switching (YARN/Kubernetes/Mesos) still requires real deployment-level integration; currently uses deterministic profile models.

## Implemented Algorithmic Core
- Score-based auction clearing with tenant priority and SLA urgency.
- Dynamic pricing with utilization/SLA/backlog/balance terms and smoothing.
- Weighted fair resource allocation for batch tenant requests.
- Migration/preemption policy with pressure + SLA-breach triggers and cooldown.
- Streaming coordinator loop that consumes Kafka tenant-input topics and drives pricing, bidding, auction clearing, allocation, and migration services.
