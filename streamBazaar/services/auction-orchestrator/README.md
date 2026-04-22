# Auction Orchestrator Service

## Aim
Registers bids and computes clearing results (winners, clearing price, revenue).

## What You Can Change
- Scoring and winner logic: `app/main.py`.
- Priority weighting and SLA urgency influence.

## Run
```bash
curl -fsS http://localhost:18080/health
curl -fsS http://localhost:18080/metrics
```

## Impact
Directly affects welfare, clearing price, and upstream/downstream allocation outcomes.
