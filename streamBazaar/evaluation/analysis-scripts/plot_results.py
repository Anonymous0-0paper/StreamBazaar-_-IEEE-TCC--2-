#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt

TENANTS = ["tenant-fraud", "tenant-clickstream", "tenant-ml"]


def extract(summary: dict, section: str, metric: str):
    vals = []
    errs = []
    for tenant in TENANTS:
        node = summary.get("tenants", {}).get(tenant, {}).get(section, {}).get(metric, {})
        vals.append(float(node.get("mean", 0.0)))
        errs.append(float(node.get("ci95", 0.0)))
    return vals, errs


def bar_plot(summary: dict, section: str, metric: str, ylabel: str, out_path: Path):
    vals, errs = extract(summary, section, metric)
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.bar(TENANTS, vals, yerr=errs, capsize=5)
    ax.set_title(f"{metric} by tenant (mean +/- 95% CI)")
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    print(f"Wrote figure: {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot summary metrics from evaluation/results/summary.json")
    parser.add_argument("--summary", default="evaluation/results/summary.json")
    parser.add_argument("--fig-dir", default="evaluation/results/figures")
    args = parser.parse_args()

    summary = json.loads(Path(args.summary).read_text(encoding="utf-8"))
    fig_dir = Path(args.fig_dir)

    bar_plot(summary, "latency", "p99", "Latency (ms)", fig_dir / "latency_p99.png")
    bar_plot(summary, "throughput", "avg_throughput", "Records/sec", fig_dir / "throughput_avg.png")
    bar_plot(summary, "resource_usage", "cpu_percent", "CPU (%)", fig_dir / "cpu_avg.png")
    bar_plot(summary, "resource_usage", "memory_percent", "Memory (%)", fig_dir / "memory_avg.png")


if __name__ == "__main__":
    main()
