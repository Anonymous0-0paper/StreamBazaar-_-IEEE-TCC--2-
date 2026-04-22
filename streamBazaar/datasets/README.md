# Dataset Management

This folder contains StreamBazaar's dataset management system for paper-aligned evaluation.

## Components

- `download_manager.py`: checks local availability, validates integrity, attempts downloads, handles fallback decisions.
- `dataset_loaders/`: per-dataset normalization into a consistent event schema.
- `synthetic_fallback/`: synthetic generators used when real datasets are not available.
- `workload_generators/`: streaming workload pipelines with operator counts, priorities, and state-size metadata.

## Dataset Locations

- `datasets/fraud-detection/`: `train_transaction.csv`, `train_identity.csv`
- `datasets/web-analytics/`: `train.txt` or `train.csv` or `random_submission.csv`
- `datasets/network-intrusion/`: `UNSW_NB15_training-set.csv`, `UNSW_NB15_testing-set.csv`
- `datasets/iot-sensors/`: `data.txt`

## Notes

- Criteo can be subsetted with `--criteo-subset-lines`.
- UNSW auto-download may require a mirror URL via `UNSW_NB15_BASE_URL`.
- Use `--disable-synthetic-fallback` to enforce strict real-data-only execution.
