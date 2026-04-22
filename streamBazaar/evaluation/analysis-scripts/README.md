# Analysis Scripts

## Aim
Aggregate multi-run reports and generate figures.

## Files
- `aggregate_reports.py`: computes mean/std/ci95 per metric (now includes advanced_kpis).
- `plot_results.py`: writes PNG charts from `summary.json`.

## Run
```bash
python3 evaluation/analysis-scripts/aggregate_reports.py --reports-dir evaluation/results/raw/exp_YYYYMMDD_HHMMSS --out evaluation/results/raw/exp_YYYYMMDD_HHMMSS/summary.json
python3 evaluation/analysis-scripts/plot_results.py --summary evaluation/results/raw/exp_YYYYMMDD_HHMMSS/summary.json --fig-dir evaluation/results/raw/exp_YYYYMMDD_HHMMSS/figures
```
