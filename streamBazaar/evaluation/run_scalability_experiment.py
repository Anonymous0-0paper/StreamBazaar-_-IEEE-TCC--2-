#!/usr/bin/env python3
"""
StreamBazaar scalability comparison experiment.

Runs ALL scheduler modes (streambazaar, talos, ds2, capsys, flink_default)
at N = 1, 2, 4 nodes on a single machine, then saves results for plotting.

Experiment matrix:
  node_counts × modes  →  KPIs per cell

Usage:
    cd streamBazaar
    python3 evaluation/run_scalability_experiment.py \
        --node-counts 1 2 4 \
        --duration-sec 120 \
        --warmup-sec 15

Output:
    evaluation/results/scalability_runs/scalability_TIMESTAMP/
        nodes_1_streambazaar_kpis.json
        nodes_1_talos_kpis.json  ...
        scalability_comparison.json   ← {node_count: {mode: {metrics}}}

Then plot:
    python3 evaluation/plot_scalability.py
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
MODES = ["streambazaar", "talos", "ds2", "capsys", "flink_default"]

ALL_TENANTS = [
    # First 4 cover all 4 required dataset types at every node count:
    # fraud → Credit Card Fraud, web → Web Analytics,
    # intrusion → Network Intrusion, iot → IoT Sensors
    "tenant-fraud",    "tenant-clickstream",            "tenant-ml", "tenant-iot",
    "tenant-fraud-2",  "tenant-clickstream-2",    "tenant-ml-2",        "tenant-iot-2",
    "tenant-fraud-3",  "tenant-clickstream-3",  "tenant-ml-3",  "tenant-iot-3",
    "tenant-fraud-4",  "tenant-clickstream-4",  "tenant-ml-4",  "tenant-iot-4",
    "tenant-fraud-5",  "tenant-clickstream-5",  "tenant-ml-5",  "tenant-iot-5",
    "tenant-fraud-6",  "tenant-clickstream-6",  "tenant-ml-6",  "tenant-iot-6",
    "tenant-fraud-7",  "tenant-clickstream-7",  "tenant-ml-7",  "tenant-iot-7",
    "tenant-fraud-8",  "tenant-clickstream-8",  "tenant-ml-8",  "tenant-iot-8",
    "tenant-fraud-9",  "tenant-clickstream-9",  "tenant-ml-9",  "tenant-iot-9",
    "tenant-fraud-10", "tenant-clickstream-10", "tenant-ml-10", "tenant-iot-10",
    "tenant-fraud-11", "tenant-clickstream-11", "tenant-ml-11", "tenant-iot-11",
    "tenant-fraud-12", "tenant-clickstream-12", "tenant-ml-12", "tenant-iot-12",
    "tenant-fraud-13", "tenant-clickstream-13", "tenant-ml-13", "tenant-iot-13",
    "tenant-fraud-14", "tenant-clickstream-14", "tenant-ml-14", "tenant-iot-14",
    "tenant-fraud-15", "tenant-clickstream-15", "tenant-ml-15", "tenant-iot-15",
    "tenant-fraud-16", "tenant-clickstream-16", "tenant-ml-16", "tenant-iot-16",
]


def sc_port(n: int) -> int:
    return 18085 + n * 10


# ── Scraping / health ----------------------------------------------------------

def node_health(node_id: int) -> Optional[Dict]:
    try:
        r = requests.get(f"http://localhost:{sc_port(node_id)}/health", timeout=5.0)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def wait_all_healthy(node_count: int, mode: str, timeout: int = 240) -> bool:
    deadline = time.time() + timeout
    ok = 0
    while time.time() < deadline:
        ok = sum(
            1 for n in range(node_count)
            if (h := node_health(n)) and str(h.get("scheduler_mode", "")).lower() == mode
            and h.get("running")
        )
        if ok == node_count:
            print(f"    [health] all {node_count} node(s) healthy ✓")
            return True
        print(f"    [health] {ok}/{node_count} healthy — waiting …")
        time.sleep(5)
    print(f"    [health] WARNING: only {ok}/{node_count} healthy after timeout, continuing")
    return False


# ── CSV-based metric collection (same approach as run_true_baseline_measurements) ──

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
    return float(s[lo] * (1.0 - pos + lo) + s[hi] * (pos - lo))


def load_kpis_from_csv(csv_path: Path, warmup_sec: int = 15) -> Dict[str, float]:
    """Parse a prometheus_metrics CSV produced by export_prometheus_csv.py."""
    with csv_path.open("r", encoding="utf-8") as fp:
        rows = list(csv.DictReader(fp))
    if not rows:
        return {k: 0.0 for k in [
            "throughput_out", "throughput_in", "rue", "eei", "fpp", "mis",
            "cpu_util", "latency_p50", "latency_p99", "latency_p999",
        ]}

    first_ts = 0
    try:
        first_ts = int(float(rows[0].get("timestamp_epoch", "0") or 0))
    except Exception:
        pass
    steady_rows = rows
    if first_ts > 0 and warmup_sec > 0:
        cutoff = first_ts + warmup_sec
        filtered = []
        for r in rows:
            try:
                ts = int(float(r.get("timestamp_epoch", "0") or 0))
                if ts >= cutoff:
                    filtered.append(r)
            except Exception:
                continue
        if filtered:
            steady_rows = filtered

    def series(name: str) -> List[float]:
        out = []
        for r in steady_rows:
            try:
                out.append(float(r.get(name, "0") or 0.0))
            except Exception:
                out.append(0.0)
        return out

    # Latency: average per-tenant percentile columns
    latency_keys = {
        "latency_p50":  [k for k in steady_rows[0].keys() if k.startswith("latency_") and k.endswith("_p50_ms")],
        "latency_p99":  [k for k in steady_rows[0].keys() if k.startswith("latency_") and k.endswith("_p99_ms")],
        "latency_p999": [k for k in steady_rows[0].keys() if k.startswith("latency_") and k.endswith("_p999_ms")],
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
    in_series = series("system_throughput_in_msgs_per_sec")

    cpu_series = series("checkpoint_cpu_cluster")

    return {
        **latency,
        "throughput_out": _mean_nonzero(out_series),
        "throughput_in":  _mean_nonzero(in_series),
        "rue":      _mean_nonzero(series("rue_cluster")),
        "eei":      _mean_nonzero(series("eei")),
        "fpp":      _mean_nonzero(series("fpp")),
        "mis":      _mean_nonzero(series("mis")),
        "cpu_util": _mean_nonzero(cpu_series),
    }


def collect_metrics_via_csv(
    root: Path, csv_dir: Path, tenants: List[str],
    duration_sec: int, warmup_sec: int,
    node_count: int = 1,
) -> subprocess.Popen:
    """Start export_prometheus_csv.py and return the process (caller waits on it)."""
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


# ── Workload ------------------------------------------------------------------

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


def tenant_list(node_count: int) -> List[str]:
    total = min(node_count * 4, len(ALL_TENANTS))
    return ALL_TENANTS[:total]


def start_workload(root: Path, tenants: List[str], duration_sec: int,
                   records_per_tenant: int, input_rate: int) -> subprocess.Popen:
    pairs = [(t, _TENANT_DATASET[t]) for t in tenants if t in _TENANT_DATASET]
    if not pairs:
        pairs = [("tenant-iot", "iot-sensors")]
    tenant_ids_csv = ",".join(t for t, _ in pairs)
    dataset_csv    = ",".join(d for _, d in pairs)
    return subprocess.Popen(
        [
            sys.executable, str(root / "scripts" / "run_workloads.py"),
            "--datasets",            dataset_csv,
            "--tenant-ids",          tenant_ids_csv,
            "--records-per-tenant",  str(records_per_tenant),
            "--input-rate",          str(input_rate),
            "--duration-sec",        str(duration_sec),
            "--disable-synthetic-fallback",
            "--skip-download",
        ],
        cwd=root, text=True,
    )


# ── Docker helpers ------------------------------------------------------------

def run_distributed(root: Path, node_count: int, action: str, mode: str) -> None:
    env = {**os.environ, "NODE_COUNT": str(node_count), "SCHEDULER_MODE": mode}
    result = subprocess.run(
        ["bash", "scripts/run-distributed.sh", action],
        cwd=root, env=env, text=True,
    )
    if result.returncode != 0 and action != "stop":
        raise RuntimeError(f"run-distributed.sh {action} failed (code {result.returncode})")


def stop_all(root: Path, max_n: int = 8) -> None:
    for n in range(max_n):
        override = root / "deployment" / "node-overrides" / f"node-{n}.yml"
        if override.exists():
            subprocess.run(
                ["docker", "compose", "--project-directory", str(root),
                 "-p", f"sb-node-{n}", "-f", str(override), "down", "--remove-orphans"],
                cwd=root, capture_output=True,
            )


# ── Main ----------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare all scheduler modes across 1/2/4 nodes"
    )
    parser.add_argument("--node-counts",        nargs="+", type=int, default=[1, 2, 4])
    parser.add_argument("--modes",              nargs="+", default=MODES)
    parser.add_argument("--duration-sec",        type=int,  default=120)
    parser.add_argument("--warmup-sec",          type=int,  default=15)
    parser.add_argument("--records-per-tenant",  type=int,  default=500000)
    parser.add_argument("--input-rate",          type=int,  default=50000)
    parser.add_argument("--out-dir",             default="evaluation/results/scalability_runs")
    args = parser.parse_args()

    root    = Path(__file__).resolve().parents[1]
    run_dir = root / args.out_dir / datetime.now().strftime("scalability_%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    results: Dict[int, Dict[str, Dict[str, float]]] = {}

    total_cells = len(args.node_counts) * len(args.modes)
    cell = 0

    for node_count in args.node_counts:
        results[node_count] = {}
        for mode in args.modes:
            cell += 1
            print(f"\n{'='*64}")
            print(f"  [{cell}/{total_cells}]  nodes={node_count}  mode={mode}")
            print(f"{'='*64}")

            stop_all(root, max_n=max(args.node_counts) + 1)
            time.sleep(3)

            run_distributed(root, node_count, "start", mode)

            if not wait_all_healthy(node_count, mode, timeout=240):
                pass

            time.sleep(5 * node_count)  # Kafka consumer group rebalance scales with node count

            tenants = tenant_list(node_count)
            print(f"    [workload] {len(tenants)} tenants → {tenants}")

            # Start workload and CSV exporter in parallel (same pattern as
            # run_true_baseline_measurements.py)
            workload = start_workload(
                root, tenants, args.duration_sec,
                args.records_per_tenant, args.input_rate,
            )
            csv_dir = run_dir / f"csv_n{node_count}_{mode}"
            exporter = collect_metrics_via_csv(
                root, csv_dir, tenants, args.duration_sec, args.warmup_sec,
                node_count=node_count,
            )

            print(f"    [metrics]  collecting {args.duration_sec}s via Prometheus CSV …")
            exporter.wait()

            if workload.poll() is None:
                workload.terminate()
                try:
                    workload.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    workload.kill()

            # Load KPIs from CSV
            csvs = sorted(csv_dir.glob("prometheus_metrics_*.csv"))
            if csvs:
                kpis = load_kpis_from_csv(csvs[-1], warmup_sec=args.warmup_sec)
            else:
                print("    [warn] no CSV found, zeroing KPIs")
                kpis = {k: 0.0 for k in [
                    "throughput_out", "throughput_in", "rue", "eei", "fpp", "mis",
                    "cpu_util", "latency_p50", "latency_p99", "latency_p999",
                ]}

            kpis["node_count"]    = float(node_count)
            kpis["total_slots"]   = float(node_count * 30)
            kpis["total_tenants"] = float(len(tenants))

            results[node_count][mode] = kpis
            (run_dir / f"nodes_{node_count}_{mode}_kpis.json").write_text(
                json.dumps(kpis, indent=2), encoding="utf-8"
            )
            print(f"    [done]  tput={kpis['throughput_out']:.2f}  "
                  f"p99={kpis['latency_p99']:.0f}ms  "
                  f"rue={kpis['rue']:.3f}  eei={kpis['eei']:.4f}")

        stop_all(root, max_n=node_count + 1)
        time.sleep(5)

    out_path = run_dir / "scalability_comparison.json"
    out_path.write_text(
        json.dumps({str(k): v for k, v in results.items()}, indent=2),
        encoding="utf-8",
    )
    print(f"\n[scalability] saved → {out_path}")
    print(f"[scalability] plot  → python3 evaluation/plot_scalability.py --results-json {out_path}")

    for metric, label, lib in [
        ("throughput_out", "Throughput (msgs/s)", False),
        ("latency_p99",    "Latency p99 (ms)",    True),
        ("rue",            "RUE",                  False),
        ("eei",            "EEI",                  False),
    ]:
        print(f"\n── {label} {'↓' if lib else '↑'} ──")
        header = f"{'Mode':<18}" + "".join(f"  {n}N".rjust(9) for n in args.node_counts)
        print(header)
        print("-" * len(header))
        for m in args.modes:
            row = f"{m:<18}"
            for nc in args.node_counts:
                v = results.get(nc, {}).get(m, {}).get(metric, 0.0)
                row += f"  {v:>8.2f}"
            print(row)


if __name__ == "__main__":
    main()
