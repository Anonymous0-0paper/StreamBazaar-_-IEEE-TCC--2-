# Metrics Collector

## Aim
Collect and store synthetic telemetry in InfluxDB for evaluation windows.

## Change Points
- Collection intervals and distributions in `collector.py`.
- Resource sampling logic in `resource_monitor.py`.

## Impact
Controls evaluation signal quality for latency/throughput/resource report fields.
