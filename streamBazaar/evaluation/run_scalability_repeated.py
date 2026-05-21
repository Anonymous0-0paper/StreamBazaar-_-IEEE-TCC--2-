#!/usr/bin/env python3
"""
Run the scalability experiment N times and aggregate results (mean ± stddev).

Usage:
    cd streamBazaar
    python3 evaluation/run_scalability_repeated.py \
        --runs 5 \
        --node-counts 4 8 16 \
        --duration-sec 120 \
        --warmup-sec 15

Output:
    evaluation/results/scalability_repeated/TIMESTAMP/
        run_1/  run_2/ ... run_N/   ← individual scalability_comparison.json per run
        aggregated.json             ← {node_count: {mode: {metric: {mean, std, runs}}}}
        summary_table.txt           ← human-readable mean ± std tables
"""
from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List


METRICS_TO_REPORT = ["rue", "throughput_out", "latency_p99", "eei", "fpp"]
MODES = ["streambazaar", "talos", "ds2", "capsys", "flink_default"]


# ── Stats helpers ──────────────────────────────────────────────────────────────

def mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def stddev(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    m = mean(values)
    return math.sqrt(sum((v - m) ** 2 for v in values) / (len(values) - 1))


# ── Aggregation ───────────────────────────────────────────────────────────────

def aggregate(run_results: List[Dict]) -> Dict:
    """Combine list of scalability_comparison dicts into mean ± std per cell."""
    # Collect all node counts and modes from the first run
    node_counts = sorted(int(k) for k in run_results[0].keys())
    modes = list(run_results[0][str(node_counts[0])].keys())
    metrics = list(run_results[0][str(node_counts[0])][modes[0]].keys())

    aggregated: Dict = {}
    for nc in node_counts:
        aggregated[nc] = {}
        for mode in modes:
            aggregated[nc][mode] = {}
            for metric in metrics:
                vals = []
                for run in run_results:
                    v = run.get(str(nc), {}).get(mode, {}).get(metric, None)
                    if v is not None:
                        try:
                            vals.append(float(v))
                        except (TypeError, ValueError):
                            pass
                aggregated[nc][mode][metric] = {
                    "mean": mean(vals),
                    "std":  stddev(vals),
                    "runs": vals,
                }
    return aggregated


def format_tables(aggregated: Dict, node_counts: List[int], modes: List[str]) -> str:
    lines = []
    for metric in METRICS_TO_REPORT:
        higher_is_better = metric not in ("latency_p99",)
        arrow = "↑" if higher_is_better else "↓"
        lines.append(f"\n── {metric} {arrow} (mean ± std) ──")
        header = f"{'Mode':<18}" + "".join(f"  {n}N".rjust(14) for n in node_counts)
        lines.append(header)
        lines.append("-" * len(header))
        for mode in modes:
            row = f"{mode:<18}"
            for nc in node_counts:
                cell = aggregated.get(nc, {}).get(mode, {}).get(metric, {})
                m = cell.get("mean", 0.0)
                s = cell.get("std",  0.0)
                row += f"  {m:6.2f}±{s:5.2f}"
            lines.append(row)
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run scalability experiment multiple times and aggregate"
    )
    parser.add_argument("--runs",               type=int, default=5)
    parser.add_argument("--node-counts",        nargs="+", type=int, default=[4, 8, 16])
    parser.add_argument("--modes",              nargs="+", default=MODES)
    parser.add_argument("--duration-sec",       type=int,  default=120)
    parser.add_argument("--warmup-sec",         type=int,  default=15)
    parser.add_argument("--records-per-tenant", type=int,  default=500000)
    parser.add_argument("--input-rate",         type=int,  default=50000)
    parser.add_argument("--out-dir",            default="evaluation/results/scalability_repeated")
    parser.add_argument("--pause-between-runs", type=int,  default=30,
                        help="Seconds to wait between runs to let containers settle")
    args = parser.parse_args()

    root    = Path(__file__).resolve().parents[1]
    out_dir = root / args.out_dir / datetime.now().strftime("repeated_%Y%m%d_%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)

    run_results: List[Dict] = []

    for run_idx in range(1, args.runs + 1):
        run_out = out_dir / f"run_{run_idx}"
        run_out.mkdir(parents=True, exist_ok=True)

        print(f"\n{'#'*64}")
        print(f"  RUN {run_idx}/{args.runs}")
        print(f"{'#'*64}")

        cmd = [
            sys.executable,
            str(root / "evaluation" / "run_scalability_experiment.py"),
            "--node-counts",        *[str(n) for n in args.node_counts],
            "--modes",              *args.modes,
            "--duration-sec",       str(args.duration_sec),
            "--warmup-sec",         str(args.warmup_sec),
            "--records-per-tenant", str(args.records_per_tenant),
            "--input-rate",         str(args.input_rate),
            "--out-dir",            str(run_out),
        ]

        result = subprocess.run(cmd, cwd=root, text=True)
        if result.returncode != 0:
            print(f"  [warn] run {run_idx} exited with code {result.returncode}, continuing")

        # Find the scalability_comparison.json produced by this run
        jsons = sorted(run_out.rglob("scalability_comparison.json"))
        if jsons:
            with jsons[-1].open() as fp:
                run_results.append(json.load(fp))
            print(f"  [run {run_idx}] loaded results from {jsons[-1]}")
        else:
            print(f"  [run {run_idx}] WARNING: no scalability_comparison.json found")

        if run_idx < args.runs:
            print(f"  [pause] waiting {args.pause_between_runs}s before next run …")
            time.sleep(args.pause_between_runs)

    if not run_results:
        print("[error] no run produced usable results — exiting")
        sys.exit(1)

    # Aggregate
    aggregated = aggregate(run_results)
    agg_path = out_dir / "aggregated.json"
    agg_path.write_text(
        json.dumps({str(k): v for k, v in aggregated.items()}, indent=2),
        encoding="utf-8",
    )
    print(f"\n[done] aggregated results → {agg_path}")

    # Print and save summary tables
    node_counts = sorted(aggregated.keys())
    modes_found = list(aggregated[node_counts[0]].keys()) if node_counts else MODES
    table_txt = format_tables(aggregated, node_counts, modes_found)
    print(table_txt)

    table_path = out_dir / "summary_table.txt"
    table_path.write_text(table_txt, encoding="utf-8")
    print(f"[done] summary table     → {table_path}")
    print(f"\nPlot with:")
    print(f"  python3 evaluation/plot_scalability.py --results-json {agg_path}")


if __name__ == "__main__":
    main()
