# Pricing Engine Service

## Aim
Computes dynamic bid floor from utilization, SLA pressure, backlog, and balance.

## What You Can Change
- Weights and smoothing constants in pricing logic.
- Input validation and output schema.

## Run
```bash
curl -fsS http://localhost:18081/health
curl -fsS http://localhost:18081/metrics
```

## Impact
Changes here alter bid pressure and therefore auction outcomes and fairness/latency trade-offs.
