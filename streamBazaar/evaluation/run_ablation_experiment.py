#!/usr/bin/env python3
"""
StreamBazaar ablation study experiment runner.

Runs StreamBazaar with each component disabled in turn, collects KPIs, and
saves results for plot_ablation.py.

Ablation variants
-----------------
  full                   Full StreamBazaar (all components active)
  no_backpressure_urgency  SLA-pressure signal fixed at baseline constant 0.2
  no_currency_decay      Virtual-currency balances never decay (unbounded growth)
  no_latency_sensitivity SLA gap hidden from allocator (latency violations ignored)
  no_priority            All tenants get uniform priority weight = 1.0
  no_auction             VCG auction replaced by proportional backlog allocation

Usage
-----
    cd streamBazaar
    python3 evaluation/run_ablation_experiment.py \
        --node-count 4 \
        --duration-sec 120 \
        --warmup-sec 15

Output
------
    evaluation/results/ablation_runs/ablation_TIMESTAMP/
        ablation_<variant>_kpis.json   (one per variant)
        ablation_comparison.json       {variant: {metric: value}}
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests

# ── Constants ------------------------------------------------------------------

ABLATION_VARIANTS = [
    "full",
    "no_backpressure_urgency",
    "no_currency_decay",
    "no_latency_sensitivity",
    "no_priority",
    "no_auction",
]

VARIANT_LABELS = {
    "full":                    r"Full \texttt{StreamBazaar}",
    "no_backpressure_urgency": r"w/o Backpressure Urgency",
    "no_currency_decay":       r"w/o Currency Decay",
    "no_latency_sensitivity":  r"w/o Latency Sensitivity",
    "no_priority":             r"w/o Priority Weighting",
    "no_auction":              r"w/o Auction (Proportional)",
}

# Tenants for a 4-node run — covers all 4 required dataset types from the first 4
_ABLATION_TENANTS = [
    "tenant-fraud", "tenant-web", "tenant-intrusion", "tenant-iot",
    "tenant-fraud-2", "tenant-clickstream", "tenant-intrusion-2", "tenant-iot-2",
    "tenant-fraud-3", "tenant-clickstream-3", "tenant-intrusion-3", "tenant-iot-3",
    "tenant-fraud-4", "tenant-clickstream-4", "tenant-intrusion-4", "tenant-iot-4",
]

_TENANT_DATASET = {
    **{f"tenant-fraud{'' if i == 0 else f'-{i}'}": "fraud"
       for i in [0] + list(range(2, 17))},
    **{f"tenant-iot{'' if i == 0 else f'-{i}'}": "iot-sensors"
       for i in [0] + list(range(2, 17))},
    **{f"tenant-clickstream{'' if i == 0 else f'-{i}'}": "web-analytics"
       for i in [0] + list(range(3, 17))},
    "tenant-web":       "web-analytics",
    "tenant-intrusion": "network-intrusion",
    **{f"tenant-ml{'' if i == 0 else f'-{i}'}": "network-intrusion"
       for i in [0] + list(range(3, 17))},
}


def sc_port(n: int) -> int:
    return 18085 + n * 10


def node_health(node_id: int, expected_mode: str = "streambazaar") -> bool:
    try:
        r = requests.get(f"http://localhost:{sc_port(node_id)}/health", timeout=5.0)
        r.raise_for_status()
        h = r.json()
        return (str(h.get("scheduler_mode", "")).lower() == expected_mode
                and bool(h.get("running")))
    except Exception:
        return False


def wait_healthy(node_count: int, timeout: int = 240) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = sum(1 for n in range(node_count) if node_health(n))
        if ok == node_count:
            print(f"    [health] all {node_count} nodes healthy ✓")
            return True
        print(f"    [health] {ok}/{node_count} healthy — waiting …")
        time.sleep(5)
    print(f"    [health] WARNING: timed out, continuing anyway")
    return False


# ── Metric loading (same helpers as run_scalability_experiment.py) -------------

def _mean_nonzero(values: List[float]) -> float:
    nz = [v for v in values if abs(v) > 1e-12]
    return sum(nz) / len(nz) if nz else 0.0


def _percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    pos = (q / 100.0) * (len(s) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] * (1.0 - pos + lo) + s[hi] * (pos - lo)


def load_kpis_from_csv(csv_path: Path, warmup_sec: int = 15) -> Dict[str, float]:
    with csv_path.open("r", encoding="utf-8") as fp:
        rows = list(csv.DictReader(fp))
    if not rows:
        return _zero_kpis()

    first_ts = 0
    try:
        first_ts = int(float(rows[0].get("timestamp_epoch", "0") or 0))
    except Exception:
        pass
    if first_ts > 0 and warmup_sec > 0:
        cutoff = first_ts + warmup_sec
        filtered = [r for r in rows
                    if int(float(r.get("timestamp_epoch", "0") or 0)) >= cutoff]
        if filtered:
            rows = filtered

    def series(name: str) -> List[float]:
        out = []
        for r in rows:
            try:
                out.append(float(r.get(name, "0") or 0.0))
            except Exception:
                out.append(0.0)
        return out

    latency_keys = {
        "latency_p50":  [k for k in rows[0].keys() if k.startswith("latency_") and k.endswith("_p50_ms")],
        "latency_p99":  [k for k in rows[0].keys() if k.startswith("latency_") and k.endswith("_p99_ms")],
        "latency_p999": [k for k in rows[0].keys() if k.startswith("latency_") and k.endswith("_p999_ms")],
    }
    latency: Dict[str, float] = {}
    for metric, keys in latency_keys.items():
        vals: List[float] = []
        for key in keys:
            vals.extend(series(key))
        latency[metric] = _mean_nonzero(vals)

    out_series = series("system_throughput_out_msgs_per_sec")
    if not any(abs(v) > 1e-12 for v in out_series):
        out_series = series("system_throughput_msgs_per_sec")

    return {
        **latency,
        "throughput_out": _mean_nonzero(out_series),
        "throughput_in":  _mean_nonzero(series("system_throughput_in_msgs_per_sec")),
        "rue":      _mean_nonzero(series("rue_cluster")),
        "eei":      _mean_nonzero(series("eei")),
        "fpp":      _mean_nonzero(series("fpp")),
        "mis":      _mean_nonzero(series("mis")),
        "cpu_util": _mean_nonzero(series("checkpoint_cpu_cluster")),
    }


def _zero_kpis() -> Dict[str, float]:
    return {k: 0.0 for k in [
        "throughput_out", "throughput_in", "rue", "eei", "fpp", "mis",
        "cpu_util", "latency_p50", "latency_p99", "latency_p999",
    ]}


# ── Docker helpers -------------------------------------------------------------

def stop_all(root: Path, max_n: int = 8) -> None:
    for n in range(max_n):
        override = root / "deployment" / "node-overrides" / f"node-{n}.yml"
        if override.exists():
            subprocess.run(
                ["docker", "compose", "--project-directory", str(root),
                 "-p", f"sb-node-{n}", "-f", str(override), "down", "--remove-orphans"],
                cwd=root, capture_output=True,
            )


def start_cluster(root: Path, node_count: int, ablation_mode: str) -> None:
    env = {
        **os.environ,
        "NODE_COUNT":    str(node_count),
        "SCHEDULER_MODE": "streambazaar",
        "ABLATION_MODE": ablation_mode,
    }
    result = subprocess.run(
        ["bash", "scripts/run-distributed.sh", "start"],
        cwd=root, env=env, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Cluster start failed for ablation={ablation_mode}")


def start_workload(root: Path, tenants: List[str], duration_sec: int,
                   records: int, rate: int) -> subprocess.Popen:
    pairs = [(t, _TENANT_DATASET[t]) for t in tenants if t in _TENANT_DATASET]
    if not pairs:
        pairs = [("tenant-iot", "iot-sensors")]
    return subprocess.Popen(
        [
            sys.executable, str(root / "scripts" / "run_workloads.py"),
            "--datasets",            ",".join(d for _, d in pairs),
            "--tenant-ids",          ",".join(t for t, _ in pairs),
            "--records-per-tenant",  str(records),
            "--input-rate",          str(rate),
            "--duration-sec",        str(duration_sec),
            "--disable-synthetic-fallback",
            "--skip-download",
        ],
        cwd=root, text=True,
    )


def start_exporter(root: Path, csv_dir: Path, tenants: List[str],
                   duration_sec: int, warmup_sec: int,
                   node_count: int) -> subprocess.Popen:
    csv_dir.mkdir(parents=True, exist_ok=True)
    return subprocess.Popen(
        [
            sys.executable, str(root / "evaluation" / "export_prometheus_csv.py"),
            "--duration-sec", str(duration_sec),
            "--interval-sec", "1",
            "--node-count",   str(node_count),
            "--tenants",      ",".join(tenants),
            "--out-dir",      str(csv_dir),
        ],
        cwd=root, text=True,
    )


# ── Main ----------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="StreamBazaar ablation study")
    parser.add_argument("--node-count",          type=int, default=4)
    parser.add_argument("--variants",            nargs="+", default=ABLATION_VARIANTS)
    parser.add_argument("--duration-sec",        type=int, default=120)
    parser.add_argument("--warmup-sec",          type=int, default=15)
    parser.add_argument("--records-per-tenant",  type=int, default=200_000)
    parser.add_argument("--input-rate",          type=int, default=10_000)
    parser.add_argument("--out-dir",             default="evaluation/results/ablation_runs")
    args = parser.parse_args()

    root    = Path(__file__).resolve().parents[1]
    run_dir = root / args.out_dir / datetime.now().strftime("ablation_%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    tenants = _ABLATION_TENANTS[: args.node_count * 4]

    results: Dict[str, Dict[str, float]] = {}
    total = len(args.variants)

    for idx, variant in enumerate(args.variants, 1):
        print(f"\n{'='*64}")
        print(f"  [{idx}/{total}]  ablation_mode={variant}")
        print(f"{'='*64}")

        stop_all(root, max_n=args.node_count + 1)
        time.sleep(3)

        start_cluster(root, args.node_count, variant)
        wait_healthy(args.node_count, timeout=240)
        time.sleep(5 * args.node_count)

        print(f"    [workload] {len(tenants)} tenants")
        csv_dir  = run_dir / f"csv_{variant}"
        workload = start_workload(root, tenants, args.duration_sec,
                                  args.records_per_tenant, args.input_rate)
        exporter = start_exporter(root, csv_dir, tenants, args.duration_sec,
                                  args.warmup_sec, args.node_count)

        print(f"    [metrics]  collecting {args.duration_sec}s …")
        exporter.wait()

        if workload.poll() is None:
            workload.terminate()
            try:
                workload.wait(timeout=10)
            except subprocess.TimeoutExpired:
                workload.kill()

        csvs = sorted(csv_dir.glob("prometheus_metrics_*.csv"))
        if csvs:
            kpis = load_kpis_from_csv(csvs[-1], warmup_sec=args.warmup_sec)
        else:
            print("    [warn] no CSV found — zeroing KPIs")
            kpis = _zero_kpis()

        results[variant] = kpis
        (run_dir / f"ablation_{variant}_kpis.json").write_text(
            json.dumps(kpis, indent=2), encoding="utf-8"
        )
        print(f"    [done]  tput={kpis['throughput_out']:.1f}  "
              f"p99={kpis['latency_p99']:.0f}ms  "
              f"rue={kpis['rue']:.3f}  eei={kpis['eei']:.4f}")

    stop_all(root, max_n=args.node_count + 1)

    out_path = run_dir / "ablation_comparison.json"
    out_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\n[ablation] saved → {out_path}")
    print(f"[ablation] plot  → python3 evaluation/plot_ablation.py --results-json {out_path}")

    # Quick summary table
    metrics = [
        ("throughput_out", "Throughput (msgs/s)", False),
        ("latency_p99",    "Latency p99 (ms)",    True),
        ("rue",            "RUE",                  False),
        ("eei",            "EEI",                  False),
        ("mis",            "MIS",                  True),
    ]
    print("\n" + "═" * 72)
    print("  ABLATION SUMMARY")
    print("═" * 72)
    for metric, label, lib in metrics:
        direction = "↓ lower is better" if lib else "↑ higher is better"
        print(f"\n── {label}  ({direction}) ──")
        full_val = results.get("full", {}).get(metric, 0.0)
        for v in args.variants:
            val = results.get(v, {}).get(metric, 0.0)
            delta = ""
            if v != "full" and abs(full_val) > 1e-9:
                pct = (val - full_val) / abs(full_val) * 100.0
                delta = f"  ({pct:+.1f}% vs full)"
            print(f"  {VARIANT_LABELS[v]:<40}  {val:>10.2f}{delta}")


if __name__ == "__main__":
    main()
