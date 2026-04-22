# Clickstream Workload

## Aim
Generate web interaction events (page/action/session).

## Change Points
- Event fields and page/action mix: `clickstream_pipeline.py`.
- Input rate via workload CLI flags.

## Impact
High-volume clickstream traffic mainly stresses throughput and queue pressure.
