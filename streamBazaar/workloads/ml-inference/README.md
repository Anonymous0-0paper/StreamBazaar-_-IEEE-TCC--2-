# ML Inference Workload

## Aim
Generate inference-request style events with feature vectors.

## Change Points
- Feature dimensions/model labels: `ml_inference_pipeline.py`.
- Input rate and payload bytes through workload CLI flags.

## Impact
Affects latency-sensitive and compute-heavy scheduling behavior.
