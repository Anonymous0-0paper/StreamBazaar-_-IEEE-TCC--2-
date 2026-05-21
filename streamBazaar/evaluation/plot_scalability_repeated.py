#!/usr/bin/env python3
"""
IEEE-style plotting for run_scalability_repeated.py aggregated results.

Each plot shows ALL node counts together so scales can be compared directly.
Plot types per metric (all saved individually, no subplots, no radar):
  - Grouped bar chart   (x = node counts, groups = modes, mean ± std)
  - Line chart          (x = node counts, one line per mode, ± std band)
  - Grouped box plot    (x = node counts, boxes per mode within each group)
  - Grouped swarm plot  (x = node counts, jittered runs per mode per group)
  - Grouped violin plot (x = node counts, violins per mode within each group)

Metrics:
  rue, throughput_out, throughput_in,
  latency_p50, latency_p99, latency_p999,
  eei, fpp, mis, cpu_util, mem_util, net_util

Normalization: StreamBazaar = 1.0 at every node count.
For lower-is-better metrics (latency_*, *_util) value < 1 means baseline is better.

Usage:
    python3 evaluation/plot_scalability_repeated.py \\
        --results-json evaluation/results/scalability_repeated/TIMESTAMP/aggregated.json \\
        --out-dir      evaluation/results/scalability_repeated/TIMESTAMP/plots
"""
from __future__ import annotations

import argparse
import json
import warnings
from pathlib import Path
from typing import Dict, List

import matplotlib
matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

warnings.filterwarnings("ignore", category=UserWarning)

# ── Configuration ─────────────────────────────────────────────────────────────

METRICS_TO_REPORT = [
    "rue", "throughput_out", "throughput_in",
    "latency_p50", "latency_p99", "latency_p999",
    "eei", "fpp", "mis",
    "cpu_util", "mem_util", "net_util",
]

METRIC_LABELS: Dict[str, str] = {
    "rue":            "Resource Utilization Efficiency (RUE)",
    "throughput_out": "Output Throughput (msgs/s)",
    "throughput_in":  "Input Throughput (msgs/s)",
    "latency_p50":    "P50 Latency (ms)",
    "latency_p99":    "P99 Latency (ms)",
    "latency_p999":   "P999 Latency (ms)",
    "eei":            "Energy Efficiency Index (EEI)",
    "fpp":            "Fairness & Priority Performance (FPP)",
    "mis":            "Multi-tenant Isolation Score (MIS)",
    "cpu_util":       "CPU Utilization (%)",
    "mem_util":       "Memory Utilization (%)",
    "net_util":       "Network Utilization (%)",
}

METRIC_SHORT: Dict[str, str] = {
    "rue":            "RUE",
    "throughput_out": "Throughput Out",
    "throughput_in":  "Throughput In",
    "latency_p50":    "P50 Latency",
    "latency_p99":    "P99 Latency",
    "latency_p999":   "P999 Latency",
    "eei":            "EEI",
    "fpp":            "FPP",
    "mis":            "MIS",
    "cpu_util":       "CPU Util.",
    "mem_util":       "Memory Util.",
    "net_util":       "Network Util.",
}

LOWER_BETTER = {"latency_p50", "latency_p99", "latency_p999", "cpu_util", "mem_util", "net_util"}

MODE_DISPLAY: Dict[str, str] = {
    "streambazaar":  "StreamBazaar",
    "talos":         "Talos",
    "ds2":           "DS2",
    "capsys":        "CAPSys",
    "flink_default": "Flink",
}

# Colorblind-safe IEEE palette
PALETTE = ["#0072B2", "#D55E00", "#009E73", "#CC79A7", "#E69F00"]
HATCHES = ["", "///", "\\\\\\", "|||", "xxx"]
MARKERS = ["o", "s", "^", "D", "v"]
LINESTYLES = ["-", "--", "-.", ":", (0, (3, 1, 1, 1))]

FONT_SIZE   = 14
LABEL_SIZE  = 14
TICK_SIZE   = 14
LEGEND_SIZE = 14
FIG_W, FIG_H = 8, 4


# ── Style ─────────────────────────────────────────────────────────────────────

def apply_ieee_style() -> None:
    sns.set_theme(style="whitegrid", font_scale=1.0)
    plt.rcParams.update({
        "font.family":     "serif",
        "font.serif":      ["Times New Roman", "DejaVu Serif"],
        "axes.titlesize":  FONT_SIZE,
        "axes.labelsize":  LABEL_SIZE,
        "xtick.labelsize": TICK_SIZE,
        "ytick.labelsize": TICK_SIZE,
        "legend.fontsize": LEGEND_SIZE,
        "figure.dpi":      150,
        "axes.edgecolor":  "black",
        "axes.linewidth":  1.2,
        "grid.linestyle":  "--",
        "grid.alpha":      0.5,
        "axes.grid":       True,
        "axes.grid.axis":  "y",
    })


# ── I/O ───────────────────────────────────────────────────────────────────────

def load_aggregated(path: Path) -> Dict:
    with path.open(encoding="utf-8") as fp:
        return json.load(fp)


def _save(fig: plt.Figure, out_dir: Path, stem: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"{stem}.{ext}", bbox_inches="tight", dpi=200)
    print(f"  saved → {stem}.pdf/png")
    plt.close(fig)


# ── Normalization helpers ──────────────────────────────────────────────────────

def _sb_mean(aggregated: Dict, nc: int, metric: str) -> float:
    v = aggregated.get(str(nc), {}).get("streambazaar", {}).get(metric, {}).get("mean", None)
    return float(v) if v and abs(float(v)) > 1e-12 else 1.0


def _norm_mean(aggregated: Dict, nc: int, mode: str, metric: str) -> float:
    raw = aggregated.get(str(nc), {}).get(mode, {}).get(metric, {}).get("mean", 0.0)
    return float(raw) / _sb_mean(aggregated, nc, metric)


def _norm_std(aggregated: Dict, nc: int, mode: str, metric: str) -> float:
    raw = aggregated.get(str(nc), {}).get(mode, {}).get(metric, {}).get("std", 0.0)
    return float(raw) / _sb_mean(aggregated, nc, metric)


def _norm_runs(aggregated: Dict, nc: int, mode: str, metric: str) -> List[float]:
    runs = aggregated.get(str(nc), {}).get(mode, {}).get(metric, {}).get("runs", [])
    denom = _sb_mean(aggregated, nc, metric)
    return [float(r) / denom for r in runs]


# ── Shared decoration ─────────────────────────────────────────────────────────

def _ref_line(ax: plt.Axes) -> None:
    ax.axhline(1.0, color="black", linewidth=1.3, linestyle="--", alpha=0.65, zorder=0)


def _ylabel(ax: plt.Axes) -> None:
    ax.set_ylabel("Normalized Value\n(StreamBazaar = 1)", fontsize=LABEL_SIZE)


def _arrow(metric: str) -> str:
    return "↓ lower = better" if metric in LOWER_BETTER else "↑ higher = better"


def _mode_legend(ax: plt.Axes, modes: List[str]) -> None:
    handles = [
        mpatches.Patch(
            facecolor=PALETTE[k], hatch=HATCHES[k],
            edgecolor="black", linewidth=0.8,
            label=MODE_DISPLAY.get(modes[k], modes[k]),
        )
        for k in range(len(modes))
    ]
    ax.legend(handles=handles, loc="upper left",
              frameon=True, framealpha=0.9, edgecolor="black",
              fontsize=LEGEND_SIZE, ncol=min(len(modes), 3))


def _group_xticks(ax: plt.Axes, node_counts: List[int], n_modes: int, group_span: float) -> None:
    group_centers = np.arange(len(node_counts)) * group_span
    ax.set_xticks(group_centers)
    ax.set_xticklabels([f"{nc} Nodes" for nc in node_counts], fontsize=TICK_SIZE)


# ── Grouped position calculator ───────────────────────────────────────────────

def _group_positions(
    node_counts: List[int], modes: List[str], slot_w: float, group_gap: float = 1.0
) -> Dict:
    """Return {(nc, mode): x_center} and the group span."""
    n_modes    = len(modes)
    group_span = n_modes * slot_w + group_gap
    positions  = {}
    for gi, nc in enumerate(node_counts):
        group_origin = gi * group_span
        for mi, mode in enumerate(modes):
            positions[(nc, mode)] = group_origin + (mi - n_modes / 2 + 0.5) * slot_w
    return positions, group_span


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Grouped bar chart
# ═══════════════════════════════════════════════════════════════════════════════

def plot_bar(
    aggregated: Dict, metric: str,
    node_counts: List[int], modes: List[str], out_dir: Path,
) -> None:
    slot_w = 0.14
    positions, group_span = _group_positions(node_counts, modes, slot_w, group_gap=0.35)

    apply_ieee_style()
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))

    for mode, color, hatch in zip(modes, PALETTE, HATCHES):
        for nc in node_counts:
            m = _norm_mean(aggregated, nc, mode, metric)
            s = _norm_std(aggregated, nc, mode, metric)
            ax.bar(
                positions[(nc, mode)], m,
                width=slot_w * 0.88,
                color=color, hatch=hatch,
                edgecolor="black", linewidth=0.8,
                yerr=s, capsize=4,
                error_kw={"elinewidth": 1.4, "ecolor": "black", "capthick": 1.4},
                zorder=3,
            )

    _ref_line(ax)
    _group_xticks(ax, node_counts, len(modes), group_span)
    _ylabel(ax)
    ax.set_title(f"{METRIC_SHORT[metric]}  ({_arrow(metric)}, normalized)",
                 fontsize=FONT_SIZE, pad=8)
    _mode_legend(ax, modes)
    fig.tight_layout()
    _save(fig, out_dir, f"bar_{metric}")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Line chart with shaded std band
# ═══════════════════════════════════════════════════════════════════════════════

def plot_line(
    aggregated: Dict, metric: str,
    node_counts: List[int], modes: List[str], out_dir: Path,
) -> None:
    apply_ieee_style()
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))

    for mode, color, marker, ls in zip(modes, PALETTE, MARKERS, LINESTYLES):
        ys  = np.array([_norm_mean(aggregated, nc, mode, metric) for nc in node_counts])
        err = np.array([_norm_std(aggregated, nc, mode, metric)  for nc in node_counts])
        ax.plot(node_counts, ys,
                color=color, marker=marker,
                linestyle=ls if isinstance(ls, str) else "-",
                linewidth=2.0, markersize=8,
                label=MODE_DISPLAY.get(mode, mode), zorder=4)
        ax.fill_between(node_counts, ys - err, ys + err,
                        color=color, alpha=0.15, zorder=2)

    _ref_line(ax)
    ax.set_xlabel("Number of Nodes", fontsize=LABEL_SIZE)
    ax.set_xticks(node_counts)
    _ylabel(ax)
    ax.set_title(f"{METRIC_SHORT[metric]} vs. Scale  ({_arrow(metric)}, normalized)",
                 fontsize=FONT_SIZE, pad=8)
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels, loc="upper left",
              frameon=True, framealpha=0.9, edgecolor="black",
              fontsize=LEGEND_SIZE, ncol=min(len(modes), 3))
    fig.tight_layout()
    _save(fig, out_dir, f"line_{metric}")


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Grouped box plot — all node counts in one figure
# ═══════════════════════════════════════════════════════════════════════════════

def plot_box(
    aggregated: Dict, metric: str,
    node_counts: List[int], modes: List[str], out_dir: Path,
) -> None:
    slot_w = 0.14
    positions, group_span = _group_positions(node_counts, modes, slot_w, group_gap=0.35)

    apply_ieee_style()
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))

    for mode, color, hatch in zip(modes, PALETTE, HATCHES):
        for nc in node_counts:
            vals = _norm_runs(aggregated, nc, mode, metric)
            if not vals:
                continue
            bp = ax.boxplot(
                vals,
                positions=[positions[(nc, mode)]],
                widths=[slot_w * 0.82],
                patch_artist=True,
                notch=False,
                showfliers=True,
                flierprops=dict(marker="o", markersize=4,
                                markerfacecolor=color, alpha=0.7),
                medianprops=dict(color="black", linewidth=2.0),
                whiskerprops=dict(color=color, linewidth=1.4),
                capprops=dict(color=color, linewidth=1.4),
                boxprops=dict(facecolor=color, edgecolor="black",
                              linewidth=0.9, alpha=0.75),
                zorder=3,
            )
            for patch in bp["boxes"]:
                patch.set_hatch(hatch)

    _ref_line(ax)
    _group_xticks(ax, node_counts, len(modes), group_span)
    _ylabel(ax)
    ax.set_title(f"{METRIC_SHORT[metric]}  (box, normalized runs)",
                 fontsize=FONT_SIZE, pad=8)
    _mode_legend(ax, modes)
    fig.tight_layout()
    _save(fig, out_dir, f"box_{metric}")


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Grouped swarm / strip plot — all node counts in one figure
# ═══════════════════════════════════════════════════════════════════════════════

def plot_swarm(
    aggregated: Dict, metric: str,
    node_counts: List[int], modes: List[str], out_dir: Path,
) -> None:
    slot_w = 0.14
    positions, group_span = _group_positions(node_counts, modes, slot_w, group_gap=0.35)

    apply_ieee_style()
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))

    for mi, (mode, color) in enumerate(zip(modes, PALETTE)):
        first = True
        for gi, nc in enumerate(node_counts):
            vals = _norm_runs(aggregated, nc, mode, metric)
            if not vals:
                continue
            xc  = positions[(nc, mode)]
            rng = np.random.default_rng(seed=42 + gi * 11 + mi * 7)
            jit = (rng.random(len(vals)) - 0.5) * slot_w * 0.55
            ax.scatter(
                [xc + j for j in jit], vals,
                color=color, s=60, alpha=0.85,
                edgecolors="black", linewidths=0.6, zorder=5,
                label=MODE_DISPLAY.get(mode, mode) if first else "_nolegend_",
            )
            ax.scatter(
                [xc], [float(np.mean(vals))],
                color=color, s=130, marker="D",
                edgecolors="black", linewidths=1.2, zorder=6,
            )
            first = False

    _ref_line(ax)
    _group_xticks(ax, node_counts, len(modes), group_span)
    _ylabel(ax)
    ax.set_title(f"{METRIC_SHORT[metric]}  (swarm, ◆ = mean, normalized)",
                 fontsize=FONT_SIZE, pad=8)
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels, loc="upper left",
              frameon=True, framealpha=0.9, edgecolor="black",
              fontsize=LEGEND_SIZE, ncol=min(len(modes), 3))
    fig.tight_layout()
    _save(fig, out_dir, f"swarm_{metric}")


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Grouped violin plot — all node counts in one figure
# ═══════════════════════════════════════════════════════════════════════════════

def plot_violin(
    aggregated: Dict, metric: str,
    node_counts: List[int], modes: List[str], out_dir: Path,
) -> None:
    slot_w = 0.14
    positions, group_span = _group_positions(node_counts, modes, slot_w, group_gap=0.35)

    apply_ieee_style()
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))

    any_drawn = False
    for mode, color, hatch in zip(modes, PALETTE, HATCHES):
        for nc in node_counts:
            vals = _norm_runs(aggregated, nc, mode, metric)
            xc   = positions[(nc, mode)]
            if len(vals) < 2:
                if vals:
                    ax.scatter([xc], vals, color=color, s=80,
                               edgecolors="black", linewidths=1.0, zorder=5)
                continue
            parts = ax.violinplot(vals, positions=[xc], widths=[slot_w * 0.88],
                                  showmeans=True, showmedians=False, showextrema=True)
            for pc in parts["bodies"]:
                pc.set_facecolor(color)
                pc.set_edgecolor("black")
                pc.set_linewidth(1.0)
                pc.set_alpha(0.72)
                pc.set_hatch(hatch)
            for key in ("cmeans", "cmaxes", "cmins", "cbars"):
                if key in parts:
                    parts[key].set_color(color)
                    parts[key].set_linewidth(1.4)
            any_drawn = True

    if not any_drawn:
        plt.close(fig)
        return

    _ref_line(ax)
    _group_xticks(ax, node_counts, len(modes), group_span)
    _ylabel(ax)
    ax.set_title(f"{METRIC_SHORT[metric]}  (violin, normalized runs)",
                 fontsize=FONT_SIZE, pad=8)
    _mode_legend(ax, modes)
    fig.tight_layout()
    _save(fig, out_dir, f"violin_{metric}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="IEEE-style plots from run_scalability_repeated.py aggregated.json"
    )
    parser.add_argument("--results-json", required=True,
                        help="Path to aggregated.json")
    parser.add_argument("--out-dir", default=None,
                        help="Output directory (default: <json_dir>/plots)")
    parser.add_argument("--metrics", nargs="+", default=METRICS_TO_REPORT)
    parser.add_argument("--modes",   nargs="+", default=None)
    parser.add_argument("--skip-violin", action="store_true")
    args = parser.parse_args()

    results_path = Path(args.results_json).resolve()
    out_dir = (Path(args.out_dir).resolve() if args.out_dir
               else results_path.parent / "plots")
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading {results_path}")
    aggregated = load_aggregated(results_path)

    node_counts = sorted(int(k) for k in aggregated.keys())
    if not node_counts:
        print("[error] aggregated.json is empty"); return

    first_nc = str(node_counts[0])
    all_modes = list(aggregated[first_nc].keys())
    order     = ["streambazaar", "talos", "ds2", "capsys", "flink_default"]
    modes     = [m for m in order if m in all_modes]
    modes    += [m for m in all_modes if m not in modes]
    if args.modes:
        modes = [m for m in modes if m in args.modes]

    available = set(aggregated[first_nc].get(modes[0], {}).keys())
    metrics   = [m for m in args.metrics if m in available]
    missing   = [m for m in args.metrics if m not in available]
    if missing:
        print(f"  [warn] not in JSON, skipped: {missing}")

    print(f"Node counts : {node_counts}")
    print(f"Modes       : {modes}")
    print(f"Metrics     : {metrics}")
    print(f"Output dir  : {out_dir}\n")

    for metric in metrics:
        print(f"── {METRIC_SHORT.get(metric, metric)} ──")
        plot_bar(aggregated, metric, node_counts, modes, out_dir)
        plot_line(aggregated, metric, node_counts, modes, out_dir)
        plot_box(aggregated, metric, node_counts, modes, out_dir)
        plot_swarm(aggregated, metric, node_counts, modes, out_dir)
        if not args.skip_violin:
            plot_violin(aggregated, metric, node_counts, modes, out_dir)

    print(f"\n[done] all plots → {out_dir}")


if __name__ == "__main__":
    main()
