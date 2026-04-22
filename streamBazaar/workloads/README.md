# Workloads

## Aim
Streaming workload generation for paper datasets with real-data-first loading and synthetic fallback.

## What You Can Change
- Dataset list and priorities: `scripts/run_workloads.py --datasets ... --priorities ...`
- Replay rates: `--input-rates` (supports accelerated replay)
- Backpressure pressure profile: `--compress-time-window`
- State sizes for migration tests: `--state-size-min-gb --state-size-max-gb --state-size-avg-gb`
- Real vs synthetic policy: `--skip-download`, `--disable-synthetic-fallback`

## Folders
- `fraud-detection/`: transaction-like events.
- `clickstream-analytics/`: page/action events.
- `ml-inference/`: feature-vector request events.

Paper-aligned dataset-aware implementations are under `datasets/workload_generators/`.

## Run
```bash
python3 scripts/run_workloads.py \
  --datasets fraud,web-analytics,network-intrusion,iot-sensors \
  --input-rates fraud=120000,web-analytics=500000,network-intrusion=100000,iot-sensors=80000 \
  --records-per-dataset fraud=50000,web-analytics=200000,network-intrusion=60000,iot-sensors=60000 \
  --compress-time-window 12 \
  --criteo-subset-lines 500000 \
  --payload-bytes-map fraud=256,web-analytics=1024,network-intrusion=512,iot-sensors=512
```

## Impact
Workload changes directly affect queue backlog, pricing pressure, migration behavior, and all observed KPIs.
