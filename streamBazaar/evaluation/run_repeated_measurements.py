#!/usr/bin/env python3
"""
Run baseline measurements N times and aggregate mean ± std with 95% confidence interval.

Usage:
    python3 evaluation/run_repeated_measurements.py --repeats 5 --duration-sec 180 --warmup-sec 15

Output:
    evaluation/results/repeated_runs/<timestamp>/
        run_1/ ... run_N/      <- raw per-run KPI JSONs
        multi_run_stats.json   <- mean, std, ci95 per mode per metric
        summary_table.txt      <- human-readable comparison table
"""
from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

# t-distribution critical values for two-tailed 95% CI (df = n-1)
# scipy.stats.t.ppf(0.975, df)
_T95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
        6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228,
        15: 2.131, 20: 2.086, 30: 2.042, 60: 2.000, 120: 1.980}

MODES = ["streambazaar", "talos", "ds2", "capsys", "flink_default"]
LOWER_IS_BETTER = {
    "latency_p50", "latency_p90", "latency_p95", "latency_p99", "latency_p999",
    "mis", "tlvr", "backlog_slope_per_sec", "cpu_util", "mem_util", "net_util",
}
KPI_KEYS = [
    "latency_p50", "latency_p90", "latency_p95", "latency_p99", "latency_p999",
    "throughput_out_avg", "throughput_in_avg", "goodput_avg", "drain_ratio",
    "rue", "eei", "fpp", "mis", "tlvr",
    "cpu_util", "mem_util", "net_util",
]


def _t95(df: int) -> float:
    """Return t_{0.975, df} via the nearest entry in the lookup table."""
    if df <= 0:
        return float("inf")
    candidates = sorted(_T95.keys())
    for k in candidates:
        if df <= k:
            return _T95[k]
    return _T95[max(candidates)]


def _stats(values: List[float]) -> Dict[str, float]:
    n = len(values)
    if n == 0:
        return {"mean": 0.0, "std": 0.0, "ci95": 0.0, "n": 0}
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / max(n - 1, 1)
    std = math.sqrt(variance)
    ci95 = _t95(n - 1) * std / math.sqrt(n)
    return {"mean": mean, "std": std, "ci95": ci95, "n": n}


def run_single(root: Path, run_dir: Path, args: argparse.Namespace) -> None:
    """Invoke run_true_baseline_measurements.py for one trial, saving into run_dir."""
    cmd = [
        sys.executable,
        str(root / "evaluation" / "run_true_baseline_measurements.py"),
        "--duration-sec", str(args.duration_sec),
        "--warmup-sec",   str(args.warmup_sec),
        "--input-rate",   str(args.input_rate),
        "--records-per-tenant", str(args.records_per_tenant),
        "--dataset",      args.dataset,
        "--tenant-id",    args.tenant_id,
        "--out-dir",      str(run_dir),
    ]
    env = os.environ.copy()
    result = subprocess.run(cmd, cwd=root, env=env, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"run_true_baseline_measurements.py exited with code {result.returncode}")


def load_kpis_from_run_dir(run_dir: Path) -> Dict[str, Dict[str, float]]:
    """Load per-mode KPI JSONs produced by run_true_baseline_measurements.py."""
    result: Dict[str, Dict[str, float]] = {}
    for mode in MODES:
        kpi_file = run_dir / f"{mode}_kpis.json"
        if kpi_file.exists():
            data = json.loads(kpi_file.read_text(encoding="utf-8"))
            # Normalise key names (some old runs use long suffixes)
            normalised: Dict[str, float] = {}
            for k, v in data.items():
                nk = (k.replace("throughput_out_avg_msgs_per_sec", "throughput_out_avg")
                        .replace("throughput_in_avg_msgs_per_sec", "throughput_in_avg")
                        .replace("goodput_avg_msgs_per_sec", "goodput_avg"))
                normalised[nk] = float(v)
            result[mode] = normalised
    return result


def aggregate(all_runs: List[Dict[str, Dict[str, float]]]) -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    Returns: {mode: {metric: {mean, std, ci95, n}}}
    """
    agg: Dict[str, Dict[str, Dict[str, float]]] = {}
    for mode in MODES:
        agg[mode] = {}
        for key in KPI_KEYS:
            vals = [run[mode][key] for run in all_runs if mode in run and key in run[mode]]
            agg[mode][key] = _stats(vals)
    return agg


def improvement(sb: float, base: float, metric: str) -> float:
    if abs(base) < 1e-12:
        return 0.0
    if metric in LOWER_IS_BETTER:
        return (base - sb) / abs(base) * 100.0
    return (sb - base) / abs(base) * 100.0


def write_summary_table(agg: Dict[str, Dict[str, Dict[str, float]]], out_path: Path) -> None:
    lines: List[str] = [
        "StreamBazaar vs Baselines — Repeated Measurements Summary",
        f"Generated: {datetime.now().isoformat()}",
        f"Repeats: {list(agg['streambazaar'].values())[0]['n'] if agg.get('streambazaar') else '?'}",
        "",
        "Values shown as mean ± std  [95% CI half-width]",
        "",
    ]
    baselines = [m for m in MODES if m != "streambazaar"]
    col_w = 26
    for metric in KPI_KEYS:
        lines.append(f"--- {metric} ({'lower' if metric in LOWER_IS_BETTER else 'higher'} is better) ---")
        sb = agg.get("streambazaar", {}).get(metric, {})
        sb_mean = sb.get("mean", 0.0)
        sb_std  = sb.get("std", 0.0)
        sb_ci   = sb.get("ci95", 0.0)
        lines.append(f"  StreamBazaar : {sb_mean:.4f} ± {sb_std:.4f}  [±{sb_ci:.4f}]")
        for b in baselines:
            bst = agg.get(b, {}).get(metric, {})
            bm, bs, bc = bst.get("mean", 0.0), bst.get("std", 0.0), bst.get("ci95", 0.0)
            imp = improvement(sb_mean, bm, metric)
            sign = "+" if imp >= 0 else ""
            lines.append(f"  {b:<16}: {bm:.4f} ± {bs:.4f}  [±{bc:.4f}]   SB {sign}{imp:.1f}%")
        lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run baseline measurements N times and aggregate statistics"
    )
    parser.add_argument("--repeats",            type=int,   default=5)
    parser.add_argument("--duration-sec",       type=int,   default=180)
    parser.add_argument("--warmup-sec",         type=int,   default=15)
    parser.add_argument("--input-rate",         type=int,   default=100000)
    parser.add_argument("--records-per-tenant", type=int,   default=50000)
    parser.add_argument("--dataset",            default="iot-sensors")
    parser.add_argument("--tenant-id",          default="tenant-iot")
    parser.add_argument("--out-dir",            default="evaluation/results/repeated_runs")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    out_root = root / args.out_dir
    session_id = datetime.now().strftime("session_%Y%m%d_%H%M%S")
    session_dir = out_root / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    all_runs: List[Dict[str, Dict[str, float]]] = []

    for trial in range(1, args.repeats + 1):
        print(f"\n{'='*60}")
        print(f"[repeated] Trial {trial}/{args.repeats}  session={session_id}")
        print(f"{'='*60}")
        trial_dir = session_dir / f"run_{trial}"
        trial_dir.mkdir(parents=True, exist_ok=True)

        run_single(root, trial_dir, args)

        # run_true_baseline_measurements creates a timestamped sub-directory
        sub_dirs = sorted(trial_dir.glob("run_*"))
        if not sub_dirs:
            print(f"[repeated] WARNING: no sub-run found in {trial_dir}, skipping trial {trial}")
            continue
        trial_run_dir = sub_dirs[-1]

        kpis = load_kpis_from_run_dir(trial_run_dir)
        if not kpis:
            print(f"[repeated] WARNING: no KPI JSONs in {trial_run_dir}, skipping trial {trial}")
            continue

        all_runs.append(kpis)
        (trial_dir / "kpis.json").write_text(json.dumps(kpis, indent=2), encoding="utf-8")
        print(f"[repeated] Trial {trial} complete — modes with data: {list(kpis.keys())}")

    if not all_runs:
        print("[repeated] ERROR: no successful trials, aborting.")
        return

    print(f"\n[repeated] Aggregating {len(all_runs)} trials …")
    agg = aggregate(all_runs)

    stats_path = session_dir / "multi_run_stats.json"
    stats_path.write_text(json.dumps(agg, indent=2), encoding="utf-8")
    print(f"[repeated] Stats saved to {stats_path}")

    table_path = session_dir / "summary_table.txt"
    write_summary_table(agg, table_path)
    print(f"[repeated] Summary table saved to {table_path}")

    # Also save raw per-run KPIs list for inspection
    raw_path = session_dir / "all_runs_raw.json"
    raw_path.write_text(json.dumps(all_runs, indent=2), encoding="utf-8")

    print(f"\n[repeated] Done.  session_dir={session_dir}")
    print(f"[repeated] To plot: python3 evaluation/plot_ieee.py --stats-json {stats_path}")


if __name__ == "__main__":
    main()
