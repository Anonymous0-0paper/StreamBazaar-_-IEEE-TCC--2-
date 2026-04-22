# Fraud Detection Workload

## Aim
Generate fraud-like transaction events with optional fraud flags and variable rate.

## Change Points
- Event schema and feature distribution: `fraud_pipeline.py`.
- Emission rate via `input_rate` from `scripts/run_workloads.py`.

## Impact
Affects SLA urgency and priority behavior for fraud tenant paths.
