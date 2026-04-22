# Stream Coordinator Service

## Aim
Consumes tenant input topics and runs `price -> bid -> clear -> allocate -> migrate` continuously.

## What You Can Change
- Tenant list: `TENANT_IDS` env var.
- Topic templates: `INPUT_TOPIC_TEMPLATE`, `OUTPUT_TOPIC_TEMPLATE`, `ALLOC_TOPIC`, `PREEMPT_TOPIC`, `METRICS_TOPIC`.
- SLA and control tuning: `DEFAULT_SLA_TARGET_MS`, `CLEAR_INTERVAL_SEC`, `CLUSTER_SLOTS`, `HIGH_PRIORITY_THRESHOLD`.
- Throughput and network normalization for KPIs: `THROUGHPUT_PEAK_MSG_PER_SEC`, `NETWORK_CAPACITY_MBPS`.

## Metrics Exposed (`/metrics`)
- Message flow and bytes: `streambazaar_messages_in_total`, `streambazaar_messages_out_total`, `streambazaar_message_bytes_in_total`, `streambazaar_message_bytes_out_total`.
- Tenant state: `streambazaar_tenant_backlog`, `streambazaar_tenant_p99_latency_ms`, `streambazaar_tenant_last_bid`.
- Requested KPIs: `streambazaar_resource_utilization_efficiency`, `streambazaar_tail_latency_violation_rate`, `streambazaar_economic_efficiency_index`, `streambazaar_fairness_performance_product`, `streambazaar_migration_impact_score`.

## Run
```bash
curl -fsS http://localhost:18085/health
curl -fsS http://localhost:18085/metrics | grep streambazaar_ | head -40
```

## Impact
This service is the main runtime source for system-level KPIs and message telemetry.
