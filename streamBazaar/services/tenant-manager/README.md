# Tenant Manager Service

## Aim
Manages tenant metadata used by schedulers and experiments.

## What You Can Change
- Tenant schema and lifecycle endpoints.
- Default tenant loading flow.

## Run
```bash
curl -fsS http://localhost:18082/health
curl -fsS http://localhost:18082/metrics
```

## Impact
Tenant metadata changes affect priority handling and experiment reproducibility.
