#!/usr/bin/env python3
"""
Generate publication-quality bar charts comparing StreamBazaar vs baseline schedulers.

Usage (auto-picks latest run):
    python3 evaluation/plot_baseline_comparison.py

Specify a run directory:
    python3 evaluation/plot_baseline_comparison.py --run-dir evaluation/results/true_baseline_runs/run_YYYYMMDD_HHMMSS

Specify a pre-computed KPI JSON (from run_true_baseline_measurements.py summary.json):
    python3 evaluation/plot_baseline_comparison.py --kpi-json evaluation/results/true_baseline_runs/run_.../summary.json
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


MODES = ["streambazaar", "talos", "ds2", "capsys", "flink_default"]
MODE_LABELS = {
    "streambazaar": "StreamBazaar",
    "talos": "TALOS",
    "ds2": "DS2",
    "capsys": "CAPSys",
    "flink_default": "Flink\nDefault",
}
MODE_COLORS = {
    "streambazaar": "#1f77b4",
    "talos": "#ff7f0e",
    "ds2": "#2ca02c",
    "capsys": "#d62728",
    "flink_default": "#9467bd",
}
MODE_HATCHES = {
    "streambazaar": "//",
    "talos": "xx",
    "ds2": "..",
    "capsys": "oo",
    "flink_default": "++",
}

LOWER_IS_BETTER = {"latency_p50", "latency_p90", "latency_p95", "latency_p99", "latency_p999",
                   "mis", "tlvr", "backlog_slope_per_sec"}


# ---------- KPI loading (mirrors run_true_baseline_measurements.py) ----------

def _mean_nonzero(values: List[float]) -> float:
    nz = [v for v in values if abs(v) > 1e-12]
    return sum(nz) / len(nz) if nz else 0.0


def _percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    if len(s) == 1:
        return float(s[0])
    pos = (q / 100.0) * (len(s) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(s) - 1)
    return float(s[lo] * (1.0 - (pos - lo)) + s[hi] * (pos - lo))


def load_kpis_from_csv(csv_path: Path, warmup_sec: int = 15) -> Dict[str, float]:
    with csv_path.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return {}

    first_ts = int(float(rows[0].get("timestamp_epoch", "0") or 0))
    cutoff = first_ts + warmup_sec
    steady = [r for r in rows if int(float(r.get("timestamp_epoch", "0") or 0)) >= cutoff] or rows

    def series(name: str) -> List[float]:
        out = []
        for r in steady:
            try:
                out.append(float(r.get(name, "0") or 0.0))
            except Exception:
                out.append(0.0)
        return out

    latency_keys = {
        p: [k for k in steady[0].keys() if k.startswith("latency_tenant_") and k.endswith(f"_{p}_ms")]
        for p in ("p50", "p90", "p95", "p99", "p999")
    }
    latency: Dict[str, float] = {}
    for p, keys in latency_keys.items():
        vals: List[float] = []
        for key in keys:
            vals.extend(series(key))
        latency[f"latency_{p}"] = _mean_nonzero(vals)

    out_series = series("system_throughput_out_msgs_per_sec")
    if not any(abs(v) > 1e-12 for v in out_series):
        out_series = series("system_throughput_msgs_per_sec")
    in_series = series("system_throughput_in_msgs_per_sec")
    if not any(abs(v) > 1e-12 for v in in_series):
        in_series = series("msg_in_rate_total")
    goodput_series = series("system_goodput_msgs_per_sec")
    if not any(abs(v) > 1e-12 for v in goodput_series):
        goodput_series = out_series

    out_avg = _mean_nonzero(out_series)
    in_avg = _mean_nonzero(in_series)

    return {
        **latency,
        "throughput_out_avg": out_avg,
        "throughput_out_p50": _percentile(out_series, 50.0),
        "throughput_out_p95": _percentile(out_series, 95.0),
        "throughput_in_avg": in_avg,
        "goodput_avg": _mean_nonzero(goodput_series),
        "drain_ratio": out_avg / max(in_avg, 1e-6),
        "backlog_slope_per_sec": _mean_nonzero(series("system_backlog_slope_per_sec")),
        "rue": _mean_nonzero(series("rue_cluster")),
        "eei": _mean_nonzero(series("eei")),
        "fpp": _mean_nonzero(series("fpp")),
        "mis": _mean_nonzero(series("mis")),
        "tlvr": _mean_nonzero(series("tlvr_cluster")),
        "cpu_util": _mean_nonzero(series("checkpoint_cpu_cluster")),
        "mem_util": _mean_nonzero(series("checkpoint_memory_cluster")),
        "net_util": _mean_nonzero(series("checkpoint_network_cluster")),
    }


def load_kpis_from_run(run_dir: Path, warmup_sec: int = 15) -> Dict[str, Dict[str, float]]:
    results: Dict[str, Dict[str, float]] = {}
    for mode in MODES:
        mode_csv_dir = run_dir / "csv" / mode
        if not mode_csv_dir.exists():
            continue
        csv_files = sorted(mode_csv_dir.glob("prometheus_metrics_*.csv"))
        if not csv_files:
            continue
        results[mode] = load_kpis_from_csv(csv_files[-1], warmup_sec)
    return results


def load_kpis_from_summary_json(json_path: Path) -> Dict[str, Dict[str, float]]:
    data = json.loads(json_path.read_text(encoding="utf-8"))
    mode_kpis = data.get("mode_kpis", {})
    # Normalise old key names
    out: Dict[str, Dict[str, float]] = {}
    for mode, kpis in mode_kpis.items():
        renamed: Dict[str, float] = {}
        for k, v in kpis.items():
            new_k = k.replace("throughput_out_avg_msgs_per_sec", "throughput_out_avg") \
                      .replace("throughput_out_p50_msgs_per_sec", "throughput_out_p50") \
                      .replace("throughput_out_p95_msgs_per_sec", "throughput_out_p95") \
                      .replace("throughput_in_avg_msgs_per_sec", "throughput_in_avg") \
                      .replace("goodput_avg_msgs_per_sec", "goodput_avg") \
                      .replace("throughput", "throughput_out_avg")
            renamed[new_k] = float(v)
        out[mode] = renamed
    return out


# ---------- plotting helpers ----------

BORDER_COLORS = {
    "streambazaar": "#0a3d6b",   # dark blue — always most prominent
    "talos":        "#b85c00",
    "ds2":          "#1a6b1a",
    "capsys":       "#8b0000",
    "flink_default":"#4b006b",
}


def _improvement_label(sb_val: float, baseline_val: float, lower_is_better: bool) -> str:
    """Return a signed % string: positive means SB is better."""
    if abs(baseline_val) < 1e-12:
        return ""
    if lower_is_better:
        pct = (baseline_val - sb_val) / abs(baseline_val) * 100.0
    else:
        pct = (sb_val - baseline_val) / abs(baseline_val) * 100.0
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.1f}%"


def bar_group(
    kpis: Dict[str, Dict[str, float]],
    metric: str,
    title: str,
    ylabel: str,
    out_path: Path,
    scale: float = 1.0,
    lower_is_better: bool = False,
) -> None:
    modes = [m for m in MODES if m in kpis]
    values = [kpis[m].get(metric, 0.0) * scale for m in modes]
    labels = [MODE_LABELS[m] for m in modes]
    colors  = [MODE_COLORS[m]  for m in modes]
    hatches = [MODE_HATCHES[m] for m in modes]
    borders = [BORDER_COLORS[m] for m in modes]

    sb_val = values[0] if values else 0.0
    best_idx = values.index(min(values)) if lower_is_better else values.index(max(values))

    fig, ax = plt.subplots(figsize=(10, 5.8))
    bars = ax.bar(
        labels, values,
        color=colors, hatch=hatches,
        width=0.55,
        edgecolor=borders,   # distinct border per algorithm
        linewidth=2.0,
    )

    # StreamBazaar bar gets an extra-thick border to make it stand out
    bars[0].set_linewidth(3.0)

    ylim_top = max(values) * 1.42 if max(values) > 0 else 1.0
    ax.set_ylim(0, ylim_top)

    # --- per-bar annotations ---
    for i, (bar, val, mode) in enumerate(zip(bars, values, modes)):
        bx = bar.get_x() + bar.get_width() / 2
        by = bar.get_height()

        # Raw value just above the bar
        ax.text(bx, by + ylim_top * 0.01, f"{val:.3g}",
                ha="center", va="bottom", fontsize=8.5, fontweight="bold",
                color=BORDER_COLORS[mode])

        # Improvement vs SB for every baseline bar
        if mode != "streambazaar":
            imp = _improvement_label(sb_val, val, lower_is_better)
            color_imp = "#27ae60" if imp.startswith("+") else "#e74c3c"
            ax.text(bx, by + ylim_top * 0.09, imp,
                    ha="center", va="bottom", fontsize=8, fontweight="bold",
                    color=color_imp,
                    bbox=dict(boxstyle="round,pad=0.2", facecolor="white",
                              edgecolor=color_imp, linewidth=0.8, alpha=0.85))

    # --- "Best ★" arrow to the winning bar ---
    best_bar = bars[best_idx]
    bx = best_bar.get_x() + best_bar.get_width() / 2
    by = best_bar.get_height()
    # place text to the right unless bar is near the right edge
    x_offset = best_bar.get_width() * 2.8
    if best_idx >= len(modes) - 2:
        x_offset = -best_bar.get_width() * 2.8
    ax.annotate(
        "Best ★",
        xy=(bx, by),
        xytext=(bx + x_offset, by + ylim_top * 0.20),
        fontsize=10, fontweight="bold", color="#c0392b",
        arrowprops=dict(arrowstyle="-|>", color="#c0392b", lw=2.0,
                        connectionstyle="arc3,rad=-0.30"),
        ha="center", va="bottom",
    )

    arrow_label = "lower is better ↓" if lower_is_better else "higher is better ↑"
    ax.set_title(f"{title}  ({arrow_label})", fontsize=12, pad=10)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.grid(axis="y", alpha=0.35, linestyle="--")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=200)
    plt.close(fig)
    print(f"  Wrote: {out_path}")


def grouped_latency_chart(kpis: Dict[str, Dict[str, float]], out_path: Path) -> None:
    percentiles = ["p50", "p90", "p95", "p99", "p999"]
    modes = [m for m in MODES if m in kpis]
    x = np.arange(len(percentiles))
    width = 0.15
    offsets = np.linspace(-(len(modes) - 1) / 2 * width, (len(modes) - 1) / 2 * width, len(modes))

    sb_p99 = kpis.get("streambazaar", {}).get("latency_p99", 0.0)

    fig, ax = plt.subplots(figsize=(13, 6.5))
    for i, mode in enumerate(modes):
        vals = [kpis[mode].get(f"latency_{p}", 0.0) for p in percentiles]
        lw = 2.5 if mode == "streambazaar" else 1.6
        bars = ax.bar(x + offsets[i], vals, width, label=MODE_LABELS[mode],
                      color=MODE_COLORS[mode], hatch=MODE_HATCHES[mode],
                      edgecolor=BORDER_COLORS[mode], linewidth=lw)
        # Improvement label above the p99 bar for each baseline
        if mode != "streambazaar" and sb_p99 > 0:
            p99_val = kpis[mode].get("latency_p99", 0.0)
            imp = _improvement_label(sb_p99, p99_val, lower_is_better=True)
            color_imp = "#27ae60" if imp.startswith("+") else "#e74c3c"
            p99_bar = bars[percentiles.index("p99")]
            bx = p99_bar.get_x() + p99_bar.get_width() / 2
            by = p99_bar.get_height()
            ax.text(bx, by * 1.03, imp, ha="center", va="bottom",
                    fontsize=7, fontweight="bold", color=color_imp,
                    bbox=dict(boxstyle="round,pad=0.15", facecolor="white",
                              edgecolor=color_imp, linewidth=0.7, alpha=0.85))

    # Arrow on p99 column pointing at the best (lowest = streambazaar)
    p99_idx = percentiles.index("p99")
    p99_vals = [kpis[m].get("latency_p99", 0.0) for m in modes]
    best_mode_idx = p99_vals.index(min(p99_vals))
    bx = x[p99_idx] + offsets[best_mode_idx]
    by = p99_vals[best_mode_idx]
    ylim_top = max(kpis[m].get("latency_p999", 0.0) for m in modes) * 1.25
    ax.annotate(
        "Best p99 ★",
        xy=(bx, by),
        xytext=(bx + width * 4.5, by + ylim_top * 0.10),
        fontsize=9, fontweight="bold", color="#c0392b",
        arrowprops=dict(arrowstyle="-|>", color="#c0392b", lw=1.8,
                        connectionstyle="arc3,rad=-0.25"),
        ha="left", va="bottom",
    )

    ax.set_title("End-to-End Latency by Percentile — StreamBazaar vs Baselines  (lower is better ↓)",
                 fontsize=12, pad=10)
    ax.set_ylabel("Latency (ms)", fontsize=10)
    ax.set_xticks(x)
    ax.set_xticklabels([f"p{p}" for p in percentiles])
    ax.set_ylim(0, ylim_top)
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(axis="y", alpha=0.35, linestyle="--")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=200)
    plt.close(fig)
    print(f"  Wrote: {out_path}")


def grouped_resource_chart(kpis: Dict[str, Dict[str, float]], out_path: Path) -> None:
    """Grouped bar chart: CPU / Memory / Network utilization side-by-side per mode."""
    resources = [
        ("cpu_util",  "CPU",     "#4c72b0"),
        ("mem_util",  "Memory",  "#dd8452"),
        ("net_util",  "Network", "#55a868"),
    ]
    modes = [m for m in MODES if m in kpis]
    x = np.arange(len(modes))
    width = 0.25
    offsets = [-width, 0.0, width]

    resource_hatches = ["//", "..", "xx"]
    sb_cpu = kpis.get("streambazaar", {}).get("cpu_util", 0.0)
    fig, ax = plt.subplots(figsize=(12, 6))
    all_bars_by_resource: List = []
    for (key, label, color), offset, hatch in zip(resources, offsets, resource_hatches):
        vals = [kpis[m].get(key, 0.0) for m in modes]
        # Use per-mode border colours so each algorithm is distinct across resource groups
        mode_borders = [BORDER_COLORS[m] for m in modes]
        bars = ax.bar(x + offset, vals, width, label=label, color=color, hatch=hatch,
                      edgecolor=mode_borders, linewidth=1.8)
        all_bars_by_resource.append((key, bars, vals))
        for bar, val in zip(bars, vals):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + 0.002,
                        f"{val:.2f}",
                        ha="center", va="bottom", fontsize=7.5, rotation=45)

    # Bold outline on StreamBazaar group + subtle highlight
    sb_idx = modes.index("streambazaar") if "streambazaar" in modes else -1
    if sb_idx >= 0:
        ax.axvspan(x[sb_idx] - width * 1.8, x[sb_idx] + width * 1.8,
                   color="#1f77b4", alpha=0.08, zorder=0)

    # Arrow on CPU (highest variance) pointing at the best (lowest = streambazaar)
    cpu_vals = [kpis[m].get("cpu_util", 0.0) for m in modes]
    best_cpu_idx = cpu_vals.index(min(cpu_vals))
    _, cpu_bars, _ = all_bars_by_resource[0]
    bx = cpu_bars[best_cpu_idx].get_x() + cpu_bars[best_cpu_idx].get_width() / 2
    by = cpu_bars[best_cpu_idx].get_height()
    ylim_top = max(max(kpis[m].get(k, 0.0) for m in modes) for k, _, _ in resources) * 1.35
    ax.annotate(
        "Lowest CPU ★",
        xy=(bx, by),
        xytext=(bx - width * 6, by + ylim_top * 0.12),
        fontsize=9, fontweight="bold", color="#c0392b",
        arrowprops=dict(arrowstyle="-|>", color="#c0392b", lw=1.8,
                        connectionstyle="arc3,rad=0.3"),
        ha="center", va="bottom",
    )

    ax.set_title("Resource Utilization: CPU / Memory / Network per Scheduler  (lower = less overhead)",
                 fontsize=12, pad=10)
    ax.set_ylabel("Utilization (%)", fontsize=10)
    ax.set_xticks(x)
    ax.set_xticklabels([MODE_LABELS[m] for m in modes], fontsize=9)
    ax.legend(loc="upper right", fontsize=9)
    ax.grid(axis="y", alpha=0.35, linestyle="--")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=200)
    plt.close(fig)
    print(f"  Wrote: {out_path}")


def improvement_heatmap(kpis: Dict[str, Dict[str, float]], out_path: Path) -> None:
    if "streambazaar" not in kpis:
        return
    sb = kpis["streambazaar"]
    baselines = [m for m in MODES if m != "streambazaar" and m in kpis]
    metrics = [
        ("latency_p50", "Latency p50", True),
        ("latency_p99", "Latency p99", True),
        ("throughput_out_avg", "Throughput out", False),
        ("throughput_in_avg", "Throughput in", False),
        ("drain_ratio", "Drain ratio", False),
        ("rue", "RUE", False),
        ("eei", "EEI", False),
        ("fpp", "FPP", False),
        ("mis", "MIS", True),
        ("cpu_util", "CPU util", True),
        ("mem_util", "Mem util", True),
        ("net_util", "Net util", True),
    ]

    data = np.zeros((len(metrics), len(baselines)))
    for j, baseline in enumerate(baselines):
        bkpi = kpis[baseline]
        for i, (key, _, lib) in enumerate(metrics):
            sbv = sb.get(key, 0.0)
            bv = bkpi.get(key, 0.0)
            if abs(bv) < 1e-12:
                data[i, j] = 0.0
            elif lib:
                data[i, j] = (bv - sbv) / abs(bv) * 100.0   # positive = SB lower = better
            else:
                data[i, j] = (sbv - bv) / abs(bv) * 100.0   # positive = SB higher = better

    fig, ax = plt.subplots(figsize=(len(baselines) * 2.2 + 2, len(metrics) * 0.65 + 1.5))
    im = ax.imshow(data, cmap="RdYlGn", aspect="auto", vmin=-50, vmax=200)
    plt.colorbar(im, ax=ax, label="% improvement of StreamBazaar over baseline")

    ax.set_xticks(range(len(baselines)))
    ax.set_xticklabels([MODE_LABELS[b] for b in baselines], fontsize=10)
    ax.set_yticks(range(len(metrics)))
    ax.set_yticklabels([m[1] for m in metrics], fontsize=9)
    ax.set_title("StreamBazaar Improvement over Baselines (%)\n(green = StreamBazaar better)", fontsize=11, pad=10)

    for i in range(len(metrics)):
        for j in range(len(baselines)):
            ax.text(j, i, f"{data[i, j]:+.1f}%", ha="center", va="center", fontsize=8,
                    color="black" if abs(data[i, j]) < 120 else "white")

    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=200)
    plt.close(fig)
    print(f"  Wrote: {out_path}")


# ---------- main ----------

def find_latest_run(results_root: Path) -> Path | None:
    runs = sorted(results_root.glob("run_*"), reverse=True)
    for r in runs:
        if any((r / "csv" / m).exists() for m in MODES):
            return r
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot StreamBazaar vs baseline comparison charts")
    parser.add_argument("--run-dir", default=None,
                        help="Path to a run_YYYYMMDD_HHMMSS directory (auto-detected if omitted)")
    parser.add_argument("--kpi-json", default=None,
                        help="Path to a summary.json with mode_kpis (overrides --run-dir)")
    parser.add_argument("--results-root", default="evaluation/results/true_baseline_runs",
                        help="Root directory to search for runs when --run-dir is omitted")
    parser.add_argument("--warmup-sec", type=int, default=15)
    parser.add_argument("--fig-dir", default=None,
                        help="Output directory for figures (defaults to <run-dir>/figures)")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]

    if args.kpi_json:
        kpi_json_path = Path(args.kpi_json) if Path(args.kpi_json).is_absolute() else root / args.kpi_json
        kpis = load_kpis_from_summary_json(kpi_json_path)
        fig_dir = Path(args.fig_dir) if args.fig_dir else kpi_json_path.parent / "figures"
        print(f"Loaded KPIs from {kpi_json_path}")
    else:
        if args.run_dir:
            run_dir = Path(args.run_dir) if Path(args.run_dir).is_absolute() else root / args.run_dir
        else:
            results_root = root / args.results_root
            run_dir = find_latest_run(results_root)
            if run_dir is None:
                print(f"No runs found under {results_root}")
                return
        print(f"Loading KPIs from CSVs in: {run_dir}")
        kpis = load_kpis_from_run(run_dir, args.warmup_sec)
        fig_dir = Path(args.fig_dir) if args.fig_dir else run_dir / "figures"

    if not kpis:
        print("No KPI data found. Run evaluation/run_true_baseline_measurements.py first.")
        return

    available = list(kpis.keys())
    print(f"Modes with data: {available}")

    print("\nGenerating figures...")

    # 1. Grouped latency chart (all percentiles, all modes)
    grouped_latency_chart(kpis, fig_dir / "latency_all_percentiles.png")

    # 2. Individual latency bars
    bar_group(kpis, "latency_p50", "Median Latency (p50)", "ms", fig_dir / "latency_p50.png", lower_is_better=True)
    bar_group(kpis, "latency_p99", "Tail Latency (p99)", "ms", fig_dir / "latency_p99.png", lower_is_better=True)

    # 3. Throughput
    bar_group(kpis, "throughput_out_avg", "Output Throughput (avg)", "msgs/sec",
              fig_dir / "throughput_out.png")
    bar_group(kpis, "throughput_in_avg", "Input Throughput (avg)", "msgs/sec",
              fig_dir / "throughput_in.png")
    bar_group(kpis, "drain_ratio", "Drain Ratio (throughput_out / throughput_in)", "ratio",
              fig_dir / "drain_ratio.png")

    # 4. Proprietary KPIs
    bar_group(kpis, "rue", "Resource Utilization Efficiency (RUE)", "score", fig_dir / "rue.png")
    bar_group(kpis, "eei", "Economic Efficiency Index (EEI)", "score", fig_dir / "eei.png")
    bar_group(kpis, "fpp", "Fairness-Performance Product (FPP)", "score", fig_dir / "fpp.png")
    bar_group(kpis, "mis", "Migration Impact Score (MIS)", "score", fig_dir / "mis.png", lower_is_better=True)

    # 5. Resource utilization breakdown
    grouped_resource_chart(kpis, fig_dir / "resource_utilization.png")
    bar_group(kpis, "cpu_util", "CPU Utilization", "%", fig_dir / "cpu_util.png", lower_is_better=True)
    bar_group(kpis, "mem_util", "Memory Utilization", "%", fig_dir / "mem_util.png", lower_is_better=True)
    bar_group(kpis, "net_util", "Network Utilization", "%", fig_dir / "net_util.png", lower_is_better=True)

    # 6. Improvement heatmap
    improvement_heatmap(kpis, fig_dir / "improvement_heatmap.png")

    # 6. Print summary table
    print("\n=== KPI Summary ===")
    metric_keys = ["latency_p50", "latency_p99", "throughput_out_avg", "throughput_in_avg",
                   "drain_ratio", "rue", "eei", "fpp", "mis", "tlvr",
                   "cpu_util", "mem_util", "net_util"]
    header = f"{'Metric':<28}" + "".join(f"{MODE_LABELS[m]:>16}" for m in MODES if m in kpis)
    print(header)
    print("-" * len(header))
    for mk in metric_keys:
        row = f"{mk:<28}"
        for m in MODES:
            if m in kpis:
                row += f"{kpis[m].get(mk, 0.0):>16.4f}"
        print(row)

    print(f"\nAll figures saved to: {fig_dir}")


if __name__ == "__main__":
    main()
