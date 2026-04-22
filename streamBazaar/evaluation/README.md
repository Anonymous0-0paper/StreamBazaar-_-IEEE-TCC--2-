# Evaluation

## Aim
Run timed experiments, compute report metrics, aggregate multiple runs, and export monitoring data.

## What You Can Change
- Evaluation duration/windowing: `run_evaluation.py` flags.
- Experiment orchestration: `run_paper_experiments.py` flags.
- CSV exports from Prometheus: `export_prometheus_csv.py` query list and interval.
- Plot/aggregation logic: `analysis-scripts/`.

## Files
- `run_evaluation.py`: creates `evaluation_report_*.json` with latency/throughput/resource + advanced KPIs.
- `run_paper_experiments.py`: multi-run warmup/steady automation.
- `export_prometheus_csv.py`: 1-second full metric exporter to CSV (throughput, latency percentiles, migration, checkpoint utilization, KPI metrics).
- `metrics-collector/`: Influx metric writers.
- `latency-tracker/`: percentile/stat calculators.
- `analysis-scripts/`: summary + plotting.

## Run
```bash
python3 evaluation/run_evaluation.py --duration 0.2
python3 evaluation/export_prometheus_csv.py --duration-sec 60 --interval-sec 1
python3 evaluation/run_paper_experiments.py --runs 2 --warmup-sec 15 --steady-sec 30 --csv-monitor-sec 30
python3 evaluation/analysis-scripts/plot_publication_metrics.py --csv evaluation/results/csv/prometheus_metrics_YYYYMMDD_HHMMSS.csv --fig-dir evaluation/results/figures_publication
```

## Impact
This folder defines how performance claims are measured, reported, and exported for analysis.
