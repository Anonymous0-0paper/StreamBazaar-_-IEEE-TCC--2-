# Migration Coordinator Service

## Aim
Schedules migration/preemption when pressure or SLA-breach thresholds are exceeded.

## What You Can Change
- Trigger thresholds.
- Cooldown parameters.

## Run
```bash
curl -fsS http://localhost:18084/health
curl -fsS http://localhost:18084/metrics
```

## Impact
Migration decisions directly influence tail latency spikes and MIS.
