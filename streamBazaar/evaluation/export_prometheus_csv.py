#!/usr/bin/env python3
import argparse
import csv
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import requests


BASE_QUERIES: Dict[str, str] = {
    "rue_cluster": 'streambazaar_resource_utilization_efficiency{scope="cluster",tenant_id="all"}',
    "tlvr_cluster": 'streambazaar_tail_latency_violation_rate{scope="cluster",tenant_id="all"}',
    "eei": "streambazaar_economic_efficiency_index",
    "fpp": "streambazaar_fairness_performance_product",
    "mis": "streambazaar_migration_impact_score",
    "system_throughput_msgs_per_sec": "streambazaar_system_throughput_msgs_per_sec",
    "system_throughput_in_msgs_per_sec": "streambazaar_system_throughput_in_msgs_per_sec",
    "system_throughput_out_msgs_per_sec": "streambazaar_system_throughput_out_msgs_per_sec",
    "system_goodput_msgs_per_sec": "streambazaar_system_goodput_msgs_per_sec",
    "system_drain_ratio": "streambazaar_system_drain_ratio",
    "system_backlog": "streambazaar_system_backlog",
    "system_backlog_slope_per_sec": "streambazaar_system_backlog_slope_per_sec",
    "consumed_rate_total_msgs_per_sec": "sum(rate(streambazaar_stream_events_consumed_total[30s]))",
    "msg_in_rate_total": "sum(rate(streambazaar_messages_in_total[1m]))",
    "msg_out_rate_total": "sum(rate(streambazaar_messages_out_total[1m]))",
    "bytes_in_rate_total": "sum(rate(streambazaar_message_bytes_in_total[1m]))",
    "bytes_out_rate_total": "sum(rate(streambazaar_message_bytes_out_total[1m]))",
    "clearing_cycles_rate": "rate(streambazaar_clearing_cycles_total[1m])",
    "checkpoint_cpu_cluster": 'streambazaar_checkpoint_cpu_utilization_percent{scope="cluster",tenant_id="all"}',
    "checkpoint_memory_cluster": 'streambazaar_checkpoint_memory_utilization_percent{scope="cluster",tenant_id="all"}',
    "checkpoint_network_cluster": 'streambazaar_checkpoint_network_utilization_percent{scope="cluster",tenant_id="all"}',
}


def build_tenant_queries(tenants: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for tenant in tenants:
        safe = tenant.replace("-", "_")
        out[f"throughput_{safe}_in"] = f'streambazaar_throughput_msgs_per_sec{{tenant_id="{tenant}",direction="in"}}'
        out[f"throughput_{safe}_out"] = f'streambazaar_throughput_msgs_per_sec{{tenant_id="{tenant}",direction="out"}}'
        out[f"throughput_{safe}_total"] = f'streambazaar_throughput_msgs_per_sec{{tenant_id="{tenant}",direction="total"}}'
        out[f"consumed_rate_{safe}_msgs_per_sec"] = f'rate(streambazaar_stream_events_consumed_total{{topic="tenant.{tenant}.input"}}[30s])'
        out[f"consumed_total_{safe}"] = f'streambazaar_stream_events_consumed_total{{topic="tenant.{tenant}.input"}}'
        # Use the Gauge metrics published directly by the coordinator rather than
        # histogram_quantile.  The histogram approach caps at the top bucket (5000 ms)
        # and returns NaN when no observations exist, neither of which is useful.
        # The Gauges are updated from the same latency_samples_ms deque and reflect
        # the true rolling percentile without any bucket-resolution distortion.
        out[f"latency_{safe}_p50_ms"] = f'streambazaar_latency_p50_ms{{tenant_id="{tenant}"}}'
        out[f"latency_{safe}_p90_ms"] = f'streambazaar_latency_p90_ms{{tenant_id="{tenant}"}}'
        out[f"latency_{safe}_p95_ms"] = f'streambazaar_latency_p95_ms{{tenant_id="{tenant}"}}'
        out[f"latency_{safe}_p99_ms"] = f'streambazaar_latency_p99_ms{{tenant_id="{tenant}"}}'
        out[f"latency_{safe}_p999_ms"] = f'streambazaar_latency_p999_ms{{tenant_id="{tenant}"}}'
        out[f"migration_{safe}_downtime_sec"] = f'streambazaar_migration_downtime_seconds{{tenant_id="{tenant}"}}'
        out[f"migration_{safe}_transfer_sec"] = f'streambazaar_migration_transfer_time_seconds{{tenant_id="{tenant}"}}'
        out[f"migration_{safe}_downtime_total_sec"] = f'streambazaar_migration_downtime_accumulated_seconds_total{{tenant_id="{tenant}"}}'
        out[f"migration_{safe}_transfer_total_sec"] = f'streambazaar_migration_transfer_time_accumulated_seconds_total{{tenant_id="{tenant}"}}'
        out[f"checkpoint_{safe}_cpu"] = f'streambazaar_checkpoint_cpu_utilization_percent{{scope="tenant",tenant_id="{tenant}"}}'
        out[f"checkpoint_{safe}_memory"] = f'streambazaar_checkpoint_memory_utilization_percent{{scope="tenant",tenant_id="{tenant}"}}'
        out[f"checkpoint_{safe}_network"] = f'streambazaar_checkpoint_network_utilization_percent{{scope="tenant",tenant_id="{tenant}"}}'
    return out


def parse_query_overrides(raw: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not raw:
        return out
    for pair in [x.strip() for x in raw.split(",") if x.strip()]:
        key, value = pair.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def instant_query(prom_url: str, expr: str) -> float:
    response = requests.get(
        f"{prom_url.rstrip('/')}/api/v1/query",
        params={"query": expr},
        timeout=5,
    )
    response.raise_for_status()
    data = response.json()
    if data.get("status") != "success":
        return 0.0
    result = data.get("data", {}).get("result", [])
    if not result:
        return 0.0
    try:
        return float(result[0]["value"][1])
    except Exception:
        return 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Export selected Prometheus metrics to CSV at 1-second intervals")
    parser.add_argument("--prom-url", default="http://localhost:19090")
    parser.add_argument("--duration-sec", type=int, default=60)
    parser.add_argument("--interval-sec", type=int, default=1)
    parser.add_argument("--out-dir", default="evaluation/results/csv")
    parser.add_argument("--tenants", default="tenant-fraud,tenant-clickstream,tenant-ml")
    parser.add_argument(
        "--queries",
        default="",
        help="Optional query overrides: name=promql,name2=promql2",
    )
    args = parser.parse_args()

    tenants = [t.strip() for t in args.tenants.split(",") if t.strip()]
    query_map = dict(BASE_QUERIES)
    query_map.update(build_tenant_queries(tenants))
    query_map.update(parse_query_overrides(args.queries))

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"prometheus_metrics_{ts}.csv"

    headers = ["timestamp_epoch", "timestamp_iso"] + list(query_map.keys())

    with out_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        end_time = time.time() + args.duration_sec
        while time.time() <= end_time:
            now_epoch = int(time.time())
            now_iso = datetime.fromtimestamp(now_epoch, tz=timezone.utc).isoformat()
            row = [now_epoch, now_iso]
            for metric_name in query_map.keys():
                row.append(instant_query(args.prom_url, query_map[metric_name]))
            writer.writerow(row)
            f.flush()
            time.sleep(max(args.interval_sec, 1))

    print(f"[csv-export] wrote {out_file}")
    print("[csv-export] queries:")
    for name, promql in query_map.items():
        print(f"  {name}={promql}")


if __name__ == "__main__":
    main()
