#!/usr/bin/env python3
"""
Export StreamBazaar metrics to CSV by scraping all coordinator nodes directly.

Each stream-coordinator node exposes /metrics on host port 18085 + node_id * 10.
This script scrapes all N nodes, aggregates cluster-wide metrics correctly, and
writes one CSV row per interval for the full measurement duration.

Aggregation rules:
  SUM  — throughput, backlog, goodput  (each node owns a disjoint tenant set)
  AVG  — ratios & utilization (RUE, EEI, FPP, MIS, CPU %)
  NODE — per-tenant metrics; value comes from whichever node owns that tenant
  PROM — rate/counter metrics that still need the shared Prometheus PromQL API
"""
from __future__ import annotations

import argparse
import csv
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

# ── Coordinator node port formula ────────────────────────────────────────────
_BASE_PORT = 18085
_PORT_STEP  = 10


def node_port(node_id: int) -> int:
    return _BASE_PORT + node_id * _PORT_STEP


# ── Prometheus text-format scraper ───────────────────────────────────────────

def scrape_node(port: int) -> Dict[str, float]:
    """
    Fetch /metrics from a coordinator and return a flat dict
    {  'metric_name{label="val",...}' : float_value  }.
    Returns {} if the endpoint is unreachable.
    """
    try:
        resp = requests.get(f"http://localhost:{port}/metrics", timeout=3.0)
        resp.raise_for_status()
    except Exception:
        return {}

    result: Dict[str, float] = {}
    for line in resp.text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Split off trailing timestamp (optional) then value
        parts = line.split()
        if len(parts) < 2:
            continue
        key = parts[0]
        try:
            val = float(parts[1])
        except ValueError:
            continue
        if val != val:   # NaN → treat as 0
            val = 0.0
        result[key] = val
    return result


def scrape_all_nodes(node_count: int) -> List[Dict[str, float]]:
    return [scrape_node(node_port(n)) for n in range(node_count)]


# ── Metric lookup helpers ─────────────────────────────────────────────────────

def _metric_key(name: str, labels: Optional[Dict[str, str]] = None) -> str:
    """Build the Prometheus text-format key for a metric + label set."""
    if not labels:
        return name
    lbl = ",".join(f'{k}="{v}"' for k, v in labels.items())
    return f"{name}{{{lbl}}}"


def _find_values(
    all_metrics: List[Dict[str, float]],
    metric_name: str,
    labels: Optional[Dict[str, str]] = None,
) -> List[float]:
    """
    Collect all values for metric_name (optionally filtered by labels)
    across every scraped node.  Uses substring match on labels so partial
    label sets work correctly.
    """
    values: List[float] = []
    for node_metrics in all_metrics:
        for key, val in node_metrics.items():
            base = key.split("{")[0]
            if base != metric_name:
                continue
            if labels:
                if not all(f'{k}="{v}"' in key for k, v in labels.items()):
                    continue
            values.append(val)
    return values


def _sum(all_metrics: List[Dict[str, float]], metric_name: str,
         labels: Optional[Dict[str, str]] = None) -> float:
    vals = _find_values(all_metrics, metric_name, labels)
    return sum(vals)


def _avg(all_metrics: List[Dict[str, float]], metric_name: str,
         labels: Optional[Dict[str, str]] = None) -> float:
    vals = [v for v in _find_values(all_metrics, metric_name, labels) if abs(v) > 1e-12]
    return sum(vals) / len(vals) if vals else 0.0


def _first_nonzero(all_metrics: List[Dict[str, float]], metric_name: str,
                   labels: Optional[Dict[str, str]] = None) -> float:
    """Return the first non-zero value found across nodes (for per-tenant metrics)."""
    for v in _find_values(all_metrics, metric_name, labels):
        if abs(v) > 1e-12:
            return v
    return 0.0


# ── PromQL fallback for rate/counter metrics ──────────────────────────────────

_PROMQL_METRICS: Dict[str, str] = {
    "consumed_rate_total_msgs_per_sec":
        "sum(rate(streambazaar_stream_events_consumed_total[30s]))",
    "msg_in_rate_total":
        "sum(rate(streambazaar_messages_in_total[1m]))",
    "msg_out_rate_total":
        "sum(rate(streambazaar_messages_out_total[1m]))",
    "bytes_in_rate_total":
        "sum(rate(streambazaar_message_bytes_in_total[1m]))",
    "bytes_out_rate_total":
        "sum(rate(streambazaar_message_bytes_out_total[1m]))",
    "clearing_cycles_rate":
        "rate(streambazaar_clearing_cycles_total[1m])",
    "system_backlog_slope_per_sec":
        "streambazaar_system_backlog_slope_per_sec",
}


def _promql(prom_url: str, expr: str) -> float:
    try:
        resp = requests.get(
            f"{prom_url.rstrip('/')}/api/v1/query",
            params={"query": expr}, timeout=5,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "success":
            return 0.0
        result = data.get("data", {}).get("result", [])
        if not result:
            return 0.0
        return float(result[0]["value"][1])
    except Exception:
        return 0.0


# ── Row builder ───────────────────────────────────────────────────────────────

def build_row(
    all_metrics: List[Dict[str, float]],
    tenants: List[str],
    prom_url: str,
) -> Dict[str, float]:
    row: Dict[str, float] = {}
    n_nodes = len(all_metrics)

    # ── SUM metrics: per-node columns + cluster total ────────────────────────
    _SUM_GAUGES = {
        "system_throughput_msgs_per_sec":     "streambazaar_system_throughput_msgs_per_sec",
        "system_throughput_in_msgs_per_sec":  "streambazaar_system_throughput_in_msgs_per_sec",
        "system_throughput_out_msgs_per_sec": "streambazaar_system_throughput_out_msgs_per_sec",
        "system_goodput_msgs_per_sec":        "streambazaar_system_goodput_msgs_per_sec",
        "system_backlog":                     "streambazaar_system_backlog",
    }
    for col, metric in _SUM_GAUGES.items():
        node_vals: List[float] = []
        for n, node_m in enumerate(all_metrics):
            v = next(
                (val for key, val in node_m.items() if key.split("{")[0] == metric),
                0.0,
            )
            row[f"node{n}_{col}"] = v   # per-node column
            node_vals.append(v)
        row[col] = sum(node_vals)       # cluster total (SUM)

    # ── AVG metrics: per-node columns + cluster average ──────────────────────
    _AVG_GAUGES = {
        "rue_cluster":              ("streambazaar_resource_utilization_efficiency",
                                     {"scope": "cluster", "tenant_id": "all"}),
        "tlvr_cluster":             ("streambazaar_tail_latency_violation_rate",
                                     {"scope": "cluster", "tenant_id": "all"}),
        "eei":                      ("streambazaar_economic_efficiency_index",     None),
        "fpp":                      ("streambazaar_fairness_performance_product",  None),
        "mis":                      ("streambazaar_migration_impact_score",        None),
        "system_drain_ratio":       ("streambazaar_system_drain_ratio",            None),
        "checkpoint_cpu_cluster":   ("streambazaar_checkpoint_cpu_utilization_percent",
                                     {"scope": "cluster", "tenant_id": "all"}),
        "checkpoint_memory_cluster":("streambazaar_checkpoint_memory_utilization_percent",
                                     {"scope": "cluster", "tenant_id": "all"}),
        "checkpoint_network_cluster":("streambazaar_checkpoint_network_utilization_percent",
                                      {"scope": "cluster", "tenant_id": "all"}),
    }
    for col, (metric, labels) in _AVG_GAUGES.items():
        node_vals = []
        for n, node_m in enumerate(all_metrics):
            vals = _find_values([node_m], metric, labels)
            v = vals[0] if vals else 0.0
            row[f"node{n}_{col}"] = v   # per-node column
            node_vals.append(v)
        active = [v for v in node_vals if abs(v) > 1e-12]
        row[col] = sum(active) / len(active) if active else 0.0  # cluster AVG

    # ── Per-tenant: from whichever node owns that tenant ─────────────────────
    for tenant in tenants:
        safe = tenant.replace("-", "_")
        tid = {"tenant_id": tenant}
        row[f"throughput_{safe}_in"] = _first_nonzero(
            all_metrics, "streambazaar_throughput_msgs_per_sec",
            {"tenant_id": tenant, "direction": "in"})
        row[f"throughput_{safe}_out"] = _first_nonzero(
            all_metrics, "streambazaar_throughput_msgs_per_sec",
            {"tenant_id": tenant, "direction": "out"})
        row[f"throughput_{safe}_total"] = _first_nonzero(
            all_metrics, "streambazaar_throughput_msgs_per_sec",
            {"tenant_id": tenant, "direction": "total"})
        row[f"latency_{safe}_p50_ms"]  = _first_nonzero(
            all_metrics, "streambazaar_latency_p50_ms", tid)
        row[f"latency_{safe}_p90_ms"]  = _first_nonzero(
            all_metrics, "streambazaar_latency_p90_ms", tid)
        row[f"latency_{safe}_p95_ms"]  = _first_nonzero(
            all_metrics, "streambazaar_latency_p95_ms", tid)
        row[f"latency_{safe}_p99_ms"]  = _first_nonzero(
            all_metrics, "streambazaar_latency_p99_ms", tid)
        row[f"latency_{safe}_p999_ms"] = _first_nonzero(
            all_metrics, "streambazaar_latency_p999_ms", tid)
        row[f"migration_{safe}_downtime_sec"] = _first_nonzero(
            all_metrics, "streambazaar_migration_downtime_seconds", tid)
        row[f"migration_{safe}_transfer_sec"] = _first_nonzero(
            all_metrics, "streambazaar_migration_transfer_time_seconds", tid)
        row[f"migration_{safe}_downtime_total_sec"] = _first_nonzero(
            all_metrics,
            "streambazaar_migration_downtime_accumulated_seconds_total", tid)
        row[f"migration_{safe}_transfer_total_sec"] = _first_nonzero(
            all_metrics,
            "streambazaar_migration_transfer_time_accumulated_seconds_total", tid)
        row[f"checkpoint_{safe}_cpu"] = _first_nonzero(
            all_metrics, "streambazaar_checkpoint_cpu_utilization_percent",
            {"scope": "tenant", "tenant_id": tenant})
        row[f"checkpoint_{safe}_memory"] = _first_nonzero(
            all_metrics, "streambazaar_checkpoint_memory_utilization_percent",
            {"scope": "tenant", "tenant_id": tenant})
        row[f"checkpoint_{safe}_network"] = _first_nonzero(
            all_metrics, "streambazaar_checkpoint_network_utilization_percent",
            {"scope": "tenant", "tenant_id": tenant})

    # ── Rate/counter metrics via Prometheus PromQL ───────────────────────────
    for col, expr in _PROMQL_METRICS.items():
        row[col] = _promql(prom_url, expr)

    return row


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export StreamBazaar metrics from all coordinator nodes to CSV"
    )
    parser.add_argument("--prom-url",      default="http://localhost:19090",
                        help="Prometheus base URL (used only for rate/counter metrics)")
    parser.add_argument("--node-count",    type=int, default=1,
                        help="Number of coordinator nodes (ports 18085, 18095, …)")
    parser.add_argument("--duration-sec",  type=int, default=60)
    parser.add_argument("--interval-sec",  type=int, default=1)
    parser.add_argument("--out-dir",       default="evaluation/results/csv")
    parser.add_argument("--tenants",       default="tenant-fraud,tenant-clickstream,tenant-ml")
    args = parser.parse_args()

    tenants = [t.strip() for t in args.tenants.split(",") if t.strip()]

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"prometheus_metrics_{ts}.csv"

    # Build header from a dummy row
    dummy_all = scrape_all_nodes(args.node_count)
    sample_row = build_row(dummy_all, tenants, args.prom_url)
    col_names = list(sample_row.keys())
    headers = ["timestamp_epoch", "timestamp_iso"] + col_names

    print(f"[csv-export] nodes={args.node_count}  "
          f"ports={[node_port(n) for n in range(args.node_count)]}")
    print(f"[csv-export] tenants={len(tenants)}  duration={args.duration_sec}s")
    print(f"[csv-export] writing → {out_file}")

    with out_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        end_time = time.time() + args.duration_sec
        while time.time() <= end_time:
            now_epoch = int(time.time())
            now_iso   = datetime.fromtimestamp(now_epoch, tz=timezone.utc).isoformat()

            all_metrics = scrape_all_nodes(args.node_count)
            row_data    = build_row(all_metrics, tenants, args.prom_url)

            writer.writerow([now_epoch, now_iso] + [row_data.get(c, 0.0) for c in col_names])
            f.flush()
            time.sleep(max(args.interval_sec, 1))

    print(f"[csv-export] done → {out_file}")


if __name__ == "__main__":
    main()
