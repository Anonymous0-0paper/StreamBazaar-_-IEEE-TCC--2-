# Baselines

This package implements three comparison baselines against StreamBazaar:

- `TALOS` (Ntouni & Petrakis, 2024)
- `DS2` (Kalavri et al., OSDI 2018)
- `Flink Default` static scheduler

## Structure

- `talos/talos_scheduler.py`: task-level autoscaling decisions with 90s cooldown.
- `talos/buffer_metrics.py`: Equation 1-8 metric calculations.
- `talos/bottleneck_detector.py`: TALOS bottleneck/backpressure conditions.
- `ds2/ds2_scheduler.py`: DS2 three-step scaling with max 3-step increase.
- `ds2/processing_estimator.py`: true processing time and capacity model.
- `ds2/throughput_optimizer.py`: throughput-oriented target tuning.
- `flink_default/static_scheduler.py`: fixed parallelism scheduler.
- `flink_default/slot_allocator.py`: static slot sharing allocation.
- `comparison_system.py`: unified comparison entrypoint.

## Run

From repository root:

```bash
python3 evaluation/run_baseline_comparison.py
```

Results are written to:

- `evaluation/baseline_comparison_results.json`

## Notes

- TALOS formulas are implemented exactly as documented in the paper equations (Eq.1-8).
- DS2 scaling caps upward changes to 3 steps per decision and applies conservative scale-down.
- Flink Default performs no runtime scaling and only uses static slot allocation.
