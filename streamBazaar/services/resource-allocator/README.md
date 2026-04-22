# Resource Allocator Service

## Aim
Applies weighted-fair allocation and water-filling for multi-tenant slot assignment.

## What You Can Change
- Effective weight function.
- Water-filling behavior and slot caps.

## Run
```bash
curl -fsS http://localhost:18083/health
curl -fsS http://localhost:18083/metrics
```

## Impact
Allocation changes impact throughput, fairness, and SLA violation rates.
