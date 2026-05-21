#!/usr/bin/env python3
"""
IEEE-quality publication plots for StreamBazaar vs baselines.

Reads multi_run_stats.json produced by run_repeated_measurements.py and
generates bar charts with mean ± 95% confidence interval error bars.

Usage:
    python3 evaluation/plot_ieee.py --stats-json evaluation/results/repeated_runs/session_.../multi_run_stats.json

Output:
    <stats_dir>/ieee_figures/   (PNG 300 dpi + PDF for each figure)
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# ---------- IEEE style ---------------------------------------------------------
# IEEE Transactions column width ≈ 3.5 in, double column ≈ 7.16 in
SINGLE_COL = 3.5
DOUBLE_COL = 7.16

plt.rcParams.update({
    "font.family":      "serif",
    "font.serif":       ["Times New Roman", "Times", "DejaVu Serif"],
    "font.size":        9,
    "axes.titlesize":   10,
    "axes.labelsize":   9,
    "xtick.labelsize":  8,
    "ytick.labelsize":  8,
    "legend.fontsize":  8,
    "figure.dpi":       300,
    "savefig.dpi":      300,
    "savefig.bbox":     "tight",
    "savefig.pad_inches": 0.04,
    "axes.linewidth":   0.8,
    "grid.linewidth":   0.5,
    "lines.linewidth":  1.0,
    "patch.linewidth":  0.8,
    "errorbar.capsize": 3,
})

# ---------- per-mode visual identity ------------------------------------------
MODES = ["streambazaar", "talos", "ds2", "capsys", "flink_default"]
MODE_LABELS = {
    "streambazaar": "StreamBazaar",
    "talos":        "TALOS",
    "ds2":          "DS2",
    "capsys":       "CAPSys",
    "flink_default":"Flink\nDefault",
}
MODE_LABELS_SHORT = {
    "streambazaar": "SB",
    "talos":        "TALOS",
    "ds2":          "DS2",
    "capsys":       "CAPSys",
    "flink_default":"Flink",
}
# Colour-blind-friendly palette (Wong 2011)
MODE_COLORS = {
    "streambazaar": "#0072B2",   # blue
    "talos":        "#E69F00",   # orange
    "ds2":          "#009E73",   # green
    "capsys":       "#D55E00",   # vermillion
    "flink_default":"#CC79A7",   # pink/purple
}
MODE_HATCHES = {
    "streambazaar": "//",
    "talos":        "xx",
    "ds2":          "..",
    "capsys":       "oo",
    "flink_default":"++",
}
BORDER_COLORS = {
    "streambazaar": "#004D7A",
    "talos":        "#9A6800",
    "ds2":          "#006B50",
    "capsys":       "#8B3A00",
    "flink_default":"#8B5070",
}

LOWER_IS_BETTER = {
    "latency_p50", "latency_p90", "latency_p95", "latency_p99", "latency_p999",
    "mis", "tlvr", "backlog_slope_per_sec", "cpu_util", "mem_util", "net_util",
}

# ---------- helpers ------------------------------------------------------------

def _v(stats: Dict, mode: str, metric: str, key: str = "mean") -> float:
    return float(stats.get(mode, {}).get(metric, {}).get(key, 0.0))


def _ci(stats: Dict, mode: str, metric: str) -> float:
    return float(stats.get(mode, {}).get(metric, {}).get("ci95", 0.0))


def _improvement_pct(sb: float, base: float, lower_is_better: bool) -> float:
    if abs(base) < 1e-12:
        return 0.0
    if lower_is_better:
        return (base - sb) / abs(base) * 100.0
    return (sb - base) / abs(base) * 100.0


def _save(fig: plt.Figure, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(path))
    pdf_path = path.with_suffix(".pdf")
    fig.savefig(str(pdf_path))
    plt.close(fig)
    print(f"  Saved: {path.name}  {pdf_path.name}")


# ---------- individual bar charts with CI error bars --------------------------

def bar_chart(
    stats: Dict,
    metric: str,
    title: str,
    ylabel: str,
    out_path: Path,
    scale: float = 1.0,
    lower_is_better: bool = False,
    width_in: float = DOUBLE_COL,
) -> None:
    modes = [m for m in MODES if m in stats]
    means  = [_v(stats, m, metric) * scale for m in modes]
    errors = [_ci(stats, m, metric) * scale for m in modes]
    labels = [MODE_LABELS[m] for m in modes]

    sb_mean = means[0] if means else 0.0
    best_idx = means.index(min(means)) if lower_is_better else means.index(max(means))

    fig, ax = plt.subplots(figsize=(width_in, width_in * 0.58))
    x = np.arange(len(modes))
    bars = ax.bar(
        x, means,
        color=[MODE_COLORS[m] for m in modes],
        hatch=[MODE_HATCHES[m] for m in modes],
        edgecolor=[BORDER_COLORS[m] for m in modes],
        linewidth=[1.5 if m == "streambazaar" else 0.8 for m in modes],
        width=0.55,
        zorder=3,
    )
    ax.errorbar(
        x, means, yerr=errors,
        fmt="none", ecolor="black", elinewidth=1.2, capsize=3.5, capthick=1.2,
        zorder=4,
    )

    # Annotate mean value above each bar
    ylim_top = max(m + e for m, e in zip(means, errors)) * 1.45 if means else 1.0
    ylim_top = max(ylim_top, 1e-6)
    ax.set_ylim(0, ylim_top)

    for i, (bar, mean, err, mode) in enumerate(zip(bars, means, errors, modes)):
        bx = bar.get_x() + bar.get_width() / 2
        by = mean + err + ylim_top * 0.015
        ax.text(bx, by, f"{mean:.3g}", ha="center", va="bottom",
                fontsize=7, fontweight="bold", color=BORDER_COLORS[mode])

        # Improvement badge for baselines
        if mode != "streambazaar":
            imp = _improvement_pct(sb_mean, mean, lower_is_better)
            c = "#1a7a3a" if imp >= 0 else "#b82020"
            sign = "+" if imp >= 0 else ""
            ax.text(bx, mean / 2, f"{sign}{imp:.1f}%",
                    ha="center", va="center", fontsize=6.5, fontweight="bold",
                    color="white",
                    bbox=dict(boxstyle="round,pad=0.15", facecolor=c,
                              edgecolor="none", alpha=0.88))

    # "Best" annotation arrow
    best_bar = bars[best_idx]
    bx = best_bar.get_x() + best_bar.get_width() / 2
    by = means[best_idx] + errors[best_idx]
    x_off = best_bar.get_width() * (2.5 if best_idx < len(modes) - 2 else -2.5)
    ax.annotate(
        "Best ★",
        xy=(bx, by), xytext=(bx + x_off, by + ylim_top * 0.18),
        fontsize=8, fontweight="bold", color="#c0392b",
        arrowprops=dict(arrowstyle="-|>", color="#c0392b", lw=1.5,
                        connectionstyle="arc3,rad=-0.3"),
        ha="center", va="bottom",
    )

    direction = "lower is better ↓" if lower_is_better else "higher is better ↑"
    ax.set_title(f"{title}  ({direction})", pad=6)
    ax.set_ylabel(ylabel)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.grid(axis="y", alpha=0.3, linestyle="--", zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout(pad=0.5)
    _save(fig, out_path)


# ---------- grouped latency chart ---------------------------------------------

def grouped_latency_chart(stats: Dict, out_path: Path, width_in: float = DOUBLE_COL) -> None:
    percentiles = ["p50", "p90", "p95", "p99", "p999"]
    modes = [m for m in MODES if m in stats]
    n_modes = len(modes)
    bar_w = 0.14
    offsets = np.linspace(-(n_modes - 1) / 2 * bar_w, (n_modes - 1) / 2 * bar_w, n_modes)
    x = np.arange(len(percentiles))

    fig, ax = plt.subplots(figsize=(width_in, width_in * 0.55))
    for i, mode in enumerate(modes):
        vals   = [_v(stats, mode, f"latency_{p}") for p in percentiles]
        errors = [_ci(stats, mode, f"latency_{p}") for p in percentiles]
        lw = 1.5 if mode == "streambazaar" else 0.8
        bars = ax.bar(x + offsets[i], vals, bar_w,
                      label=MODE_LABELS[mode],
                      color=MODE_COLORS[mode], hatch=MODE_HATCHES[mode],
                      edgecolor=BORDER_COLORS[mode], linewidth=lw, zorder=3)
        ax.errorbar(x + offsets[i], vals, yerr=errors,
                    fmt="none", ecolor="black", elinewidth=0.9, capsize=2.0, capthick=0.9,
                    zorder=4)

    # Arrow on p99 to best (lowest) bar
    sb_p99 = _v(stats, "streambazaar", "latency_p99")
    p99_idx = percentiles.index("p99")
    p99_vals = [_v(stats, m, "latency_p99") for m in modes]
    best_mi = p99_vals.index(min(p99_vals))
    bx = x[p99_idx] + offsets[best_mi]
    by = p99_vals[best_mi] + _ci(stats, modes[best_mi], "latency_p99")
    ylim_top = max(_v(stats, m, "latency_p999") + _ci(stats, m, "latency_p999") for m in modes) * 1.3
    ax.set_ylim(0, ylim_top)
    ax.annotate("Best p99 ★", xy=(bx, by),
                xytext=(bx + bar_w * 5, by + ylim_top * 0.08),
                fontsize=7, fontweight="bold", color="#c0392b",
                arrowprops=dict(arrowstyle="-|>", color="#c0392b", lw=1.3,
                                connectionstyle="arc3,rad=-0.25"),
                ha="left", va="bottom")

    ax.set_title("End-to-End Latency by Percentile  (lower is better ↓)", pad=6)
    ax.set_ylabel("Latency (ms)")
    ax.set_xticks(x)
    ax.set_xticklabels([f"p{p}" for p in percentiles])
    ax.legend(loc="upper left", ncol=2, framealpha=0.7, edgecolor="gray")
    ax.grid(axis="y", alpha=0.3, linestyle="--", zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout(pad=0.5)
    _save(fig, out_path)


# ---------- resource utilization grouped chart --------------------------------

def grouped_resource_chart(stats: Dict, out_path: Path, width_in: float = DOUBLE_COL) -> None:
    resources = [("cpu_util", "CPU (%)", True), ("mem_util", "Mem (%)", True), ("net_util", "Net (%)", False)]
    modes = [m for m in MODES if m in stats]
    x = np.arange(len(modes))
    w = 0.24
    offsets = [-w, 0.0, w]
    res_colors = ["#4c72b0", "#dd8452", "#55a868"]
    res_hatches = ["//", "..", "xx"]

    fig, ax = plt.subplots(figsize=(width_in, width_in * 0.52))
    for (key, label, lib), offset, col, hatch in zip(resources, offsets, res_colors, res_hatches):
        vals   = [_v(stats, m, key) for m in modes]
        errors = [_ci(stats, m, key) for m in modes]
        ax.bar(x + offset, vals, w, label=label, color=col, hatch=hatch,
               edgecolor=[BORDER_COLORS[m] for m in modes], linewidth=0.8, zorder=3)
        ax.errorbar(x + offset, vals, yerr=errors,
                    fmt="none", ecolor="black", elinewidth=0.9, capsize=2.0, capthick=0.9,
                    zorder=4)

    # Highlight SB group
    if "streambazaar" in modes:
        si = modes.index("streambazaar")
        ax.axvspan(x[si] - w * 1.8, x[si] + w * 1.8, color="#0072B2", alpha=0.07, zorder=0)

    cpu_vals = [_v(stats, m, "cpu_util") for m in modes]
    best_cpu = cpu_vals.index(min(cpu_vals))
    ylim_top = max(_v(stats, m, k) + _ci(stats, m, k) for m in modes for k, *_ in resources) * 1.4
    ax.set_ylim(0, ylim_top)
    bx = x[best_cpu] - w  # CPU bar offset
    by = cpu_vals[best_cpu] + _ci(stats, modes[best_cpu], "cpu_util")
    ax.annotate("Lowest CPU ★", xy=(bx, by),
                xytext=(bx - w * 4, by + ylim_top * 0.12),
                fontsize=7, fontweight="bold", color="#c0392b",
                arrowprops=dict(arrowstyle="-|>", color="#c0392b", lw=1.3,
                                connectionstyle="arc3,rad=0.3"),
                ha="center", va="bottom")

    ax.set_title("Resource Utilization per Scheduler  (lower = less overhead)", pad=6)
    ax.set_ylabel("Utilization (%)")
    ax.set_xticks(x)
    ax.set_xticklabels([MODE_LABELS[m] for m in modes])
    ax.legend(loc="upper right", framealpha=0.7, edgecolor="gray")
    ax.grid(axis="y", alpha=0.3, linestyle="--", zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout(pad=0.5)
    _save(fig, out_path)


# ---------- 2×2 KPI overview panel (single-figure for IEEE) -------------------

def kpi_panel(stats: Dict, out_path: Path, width_in: float = DOUBLE_COL) -> None:
    """Four-panel figure: RUE, EEI, FPP, MIS — ideal for a single IEEE figure."""
    panels = [
        ("rue", "RUE",  "score", False),
        ("eei", "EEI",  "score", False),
        ("fpp", "FPP",  "score", False),
        ("mis", "MIS",  "score", True),
    ]
    modes = [m for m in MODES if m in stats]
    fig, axes = plt.subplots(2, 2, figsize=(width_in, width_in * 0.9))
    axes_flat = axes.flatten()

    for ax, (metric, label, unit, lib) in zip(axes_flat, panels):
        means  = [_v(stats, m, metric) for m in modes]
        errors = [_ci(stats, m, metric) for m in modes]
        sb_mean = means[0] if means else 0.0
        best_idx = means.index(min(means)) if lib else means.index(max(means))
        x = np.arange(len(modes))

        bars = ax.bar(x, means,
                      color=[MODE_COLORS[m] for m in modes],
                      hatch=[MODE_HATCHES[m] for m in modes],
                      edgecolor=[BORDER_COLORS[m] for m in modes],
                      linewidth=[1.4 if m == "streambazaar" else 0.7 for m in modes],
                      width=0.58, zorder=3)
        ax.errorbar(x, means, yerr=errors,
                    fmt="none", ecolor="black", elinewidth=1.0, capsize=3, capthick=1.0,
                    zorder=4)

        ylim_top = max(m + e for m, e in zip(means, errors)) * 1.50 if means else 1.0
        ylim_top = max(ylim_top, 1e-6)
        ax.set_ylim(0, ylim_top)

        for i, (bar, mean, err, mode) in enumerate(zip(bars, means, errors, modes)):
            bx = bar.get_x() + bar.get_width() / 2
            by = mean + err + ylim_top * 0.02
            ax.text(bx, by, f"{mean:.3g}", ha="center", va="bottom",
                    fontsize=6, fontweight="bold", color=BORDER_COLORS[mode])
            if mode != "streambazaar":
                imp = _improvement_pct(sb_mean, mean, lib)
                c = "#1a7a3a" if imp >= 0 else "#b82020"
                sign = "+" if imp >= 0 else ""
                ax.text(bx, mean / 2, f"{sign}{imp:.0f}%",
                        ha="center", va="center", fontsize=5.5, fontweight="bold",
                        color="white",
                        bbox=dict(boxstyle="round,pad=0.1", facecolor=c,
                                  edgecolor="none", alpha=0.88))

        # Star on best bar
        best_bar = bars[best_idx]
        bx = best_bar.get_x() + best_bar.get_width() / 2
        by = means[best_idx] + errors[best_idx] + ylim_top * 0.04
        ax.text(bx, by, "★", ha="center", va="bottom",
                fontsize=9, color="#c0392b", fontweight="bold")

        direction = "↓" if lib else "↑"
        ax.set_title(f"{label}  ({direction})", fontsize=9)
        ax.set_ylabel(unit, fontsize=7)
        ax.set_xticks(x)
        ax.set_xticklabels([MODE_LABELS_SHORT[m] for m in modes], fontsize=7)
        ax.grid(axis="y", alpha=0.3, linestyle="--", zorder=0)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    # Shared legend
    handles = [mpatches.Patch(facecolor=MODE_COLORS[m], edgecolor=BORDER_COLORS[m],
                               hatch=MODE_HATCHES[m], label=MODE_LABELS[m])
               for m in modes]
    fig.legend(handles=handles, loc="lower center", ncol=len(modes),
               fontsize=7, framealpha=0.8, edgecolor="gray",
               bbox_to_anchor=(0.5, -0.02))
    fig.suptitle("StreamBazaar KPI Comparison vs Baselines\n"
                 "(error bars = 95% CI)", fontsize=10, y=1.01)
    fig.tight_layout(pad=0.6, rect=[0, 0.06, 1, 1])
    _save(fig, out_path)


# ---------- improvement heatmap with CI annotation ----------------------------

def improvement_heatmap(stats: Dict, out_path: Path, width_in: float = DOUBLE_COL) -> None:
    if "streambazaar" not in stats:
        return
    baselines = [m for m in MODES if m != "streambazaar" and m in stats]
    metrics = [
        ("latency_p50", "Latency p50", True),
        ("latency_p99", "Latency p99", True),
        ("throughput_out_avg", "Goodput (excl. retries)", False),
        ("drain_ratio", "Drain ratio", False),
        ("rue", "RUE", False),
        ("eei", "EEI", False),
        ("fpp", "FPP", False),
        ("mis", "MIS", True),
        ("cpu_util", "CPU util", True),
        ("mem_util", "Mem util", True),
    ]

    data = np.zeros((len(metrics), len(baselines)))
    for j, b in enumerate(baselines):
        for i, (key, _, lib) in enumerate(metrics):
            sbv = _v(stats, "streambazaar", key)
            bv  = _v(stats, b, key)
            data[i, j] = _improvement_pct(sbv, bv, lib)

    fig, ax = plt.subplots(figsize=(width_in * 0.7, len(metrics) * 0.5 + 1.2))
    im = ax.imshow(data, cmap="RdYlGn", aspect="auto", vmin=-40, vmax=200)
    cbar = plt.colorbar(im, ax=ax, shrink=0.85, pad=0.02)
    cbar.set_label("% improvement of SB over baseline", fontsize=7)
    cbar.ax.tick_params(labelsize=7)

    ax.set_xticks(range(len(baselines)))
    ax.set_xticklabels([MODE_LABELS[b] for b in baselines], fontsize=8)
    ax.set_yticks(range(len(metrics)))
    ax.set_yticklabels([m[1] for m in metrics], fontsize=8)
    ax.set_title("StreamBazaar improvement over baselines (%)\n"
                 "(green = SB better, red = SB worse)", fontsize=9, pad=6)

    for i in range(len(metrics)):
        for j in range(len(baselines)):
            v = data[i, j]
            ax.text(j, i, f"{v:+.0f}%", ha="center", va="center", fontsize=7,
                    color="black" if abs(v) < 100 else "white")

    fig.tight_layout(pad=0.5)
    _save(fig, out_path)


# ---------- main --------------------------------------------------------------

def find_latest_stats(results_root: Path) -> Optional[Path]:
    sessions = sorted(results_root.glob("session_*"), reverse=True)
    for s in sessions:
        p = s / "multi_run_stats.json"
        if p.exists():
            return p
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate IEEE-quality plots from multi-run stats")
    parser.add_argument("--stats-json", default=None,
                        help="Path to multi_run_stats.json (auto-detected if omitted)")
    parser.add_argument("--results-root", default="evaluation/results/repeated_runs")
    parser.add_argument("--fig-dir", default=None,
                        help="Output directory (default: <stats_dir>/ieee_figures)")
    parser.add_argument("--width", type=float, default=DOUBLE_COL,
                        help=f"Figure width in inches (default={DOUBLE_COL} for IEEE double column)")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]

    if args.stats_json:
        stats_path = Path(args.stats_json) if Path(args.stats_json).is_absolute() \
                     else root / args.stats_json
    else:
        results_root = root / args.results_root
        stats_path = find_latest_stats(results_root)
        if stats_path is None:
            print(f"No multi_run_stats.json found under {results_root}")
            print("Run:  python3 evaluation/run_repeated_measurements.py --repeats 5")
            return

    print(f"Loading stats from: {stats_path}")
    stats: Dict = json.loads(stats_path.read_text(encoding="utf-8"))

    fig_dir = Path(args.fig_dir) if args.fig_dir else stats_path.parent / "ieee_figures"
    fig_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory:   {fig_dir}\n")

    w = args.width

    # 1. Grouped latency (all percentiles)
    grouped_latency_chart(stats, fig_dir / "fig_latency_percentiles.png", w)

    # 2. Tail latency p99
    bar_chart(stats, "latency_p99", "Tail Latency (p99)", "ms",
              fig_dir / "fig_latency_p99.png", lower_is_better=True, width_in=w)

    # 3. Throughput (goodput — excludes retries and duplicates)
    bar_chart(stats, "throughput_out_avg", "Goodput (avg, excl. retries)", "msgs/s",
              fig_dir / "fig_throughput_out.png", width_in=w)

    # 4. Proprietary KPIs — individual
    bar_chart(stats, "rue", "Resource Utilization Efficiency (RUE)", "score",
              fig_dir / "fig_rue.png", width_in=w)
    bar_chart(stats, "eei", "Economic Efficiency Index (EEI)", "score",
              fig_dir / "fig_eei.png", width_in=w)
    bar_chart(stats, "fpp", "Fairness-Performance Product (FPP)", "score",
              fig_dir / "fig_fpp.png", width_in=w)
    bar_chart(stats, "mis", "Migration Impact Score (MIS)", "score",
              fig_dir / "fig_mis.png", lower_is_better=True, width_in=w)

    # 5. 2×2 KPI overview panel (compact single figure for paper body)
    kpi_panel(stats, fig_dir / "fig_kpi_panel.png", w)

    # 6. Resource utilization
    grouped_resource_chart(stats, fig_dir / "fig_resource_util.png", w)
    bar_chart(stats, "cpu_util", "CPU Utilization", "%",
              fig_dir / "fig_cpu_util.png", lower_is_better=True, width_in=w)

    # 7. Improvement heatmap
    improvement_heatmap(stats, fig_dir / "fig_improvement_heatmap.png", w)

    # 8. Print summary table
    print("\n=== Mean ± 95% CI Summary ===")
    modes = [m for m in MODES if m in stats]
    key_metrics = ["latency_p99", "goodput_avg", "rue", "eei", "fpp", "mis", "cpu_util"]
    header = f"{'Metric':<22}" + "".join(f"{'  ' + m:>22}" for m in modes)
    print(header)
    print("-" * len(header))
    for mk in key_metrics:
        row = f"{mk:<22}"
        for m in modes:
            mean = _v(stats, m, mk)
            ci   = _ci(stats, m, mk)
            row += f"  {mean:>7.3f}±{ci:.3f}"
        print(row)

    n_trials = int(list(stats.get("streambazaar", {}).values())[0].get("n", 0)) \
               if stats.get("streambazaar") else "?"
    print(f"\nBased on {n_trials} repeated trials, 95% CI via Student's t-distribution.")
    print(f"\nAll IEEE figures saved to: {fig_dir}")


if __name__ == "__main__":
    main()
