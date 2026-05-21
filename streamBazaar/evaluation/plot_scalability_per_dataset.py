#!/usr/bin/env python3
"""
IEEE-style per-dataset plots from a run_scalability_repeated.py experiment directory.

Re-processes the raw per-run CSVs to extract per-dataset (fraud / clickstream /
iot / ml) metrics, aggregates across runs (mean ± std), then generates the same
five IEEE plot types — one complete set per dataset.

Available per-dataset metrics (extracted from tenant-level CSV columns):
  throughput_out, throughput_in,
  latency_p50, latency_p99, latency_p999,
  cpu_util, mem_util, net_util

Cluster-wide metrics (rue, eei, fpp, mis) have no per-dataset breakdown and
are therefore not included here; use plot_scalability_repeated.py for those.

Usage:
    python3 evaluation/plot_scalability_per_dataset.py \\
        --repeated-dir evaluation/results/scalability_repeated/repeated_TIMESTAMP \\
        --out-dir      evaluation/results/scalability_repeated/repeated_TIMESTAMP/plots_per_dataset

    # restrict to specific datasets or metrics:
    python3 ... --datasets fraud iot --metrics latency_p99 throughput_out
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

warnings.filterwarnings("ignore", category=UserWarning)

# ── Constants ─────────────────────────────────────────────────────────────────

DATASETS = ["fraud", "clickstream", "iot", "ml"]

DATASET_DISPLAY = {
    "fraud":       "Credit-Card Fraud",
    "clickstream": "Web Analytics",
    "iot":         "IoT Sensors",
    "ml":          "Network Intrusion",
}

METRICS = [
    "throughput_out", "throughput_in",
    "latency_p50", "latency_p99", "latency_p999",
    "cpu_util", "mem_util", "net_util",
]

METRIC_SHORT = {
    "throughput_out": "Goodput (excl. retries)",
    "throughput_in":  "Throughput In",
    "latency_p50":    "P50 Latency",
    "latency_p99":    "P99 Latency",
    "latency_p999":   "P999 Latency",
    "cpu_util":       "CPU Util.",
    "mem_util":       "Memory Util.",
    "net_util":       "Network Util.",
}

LOWER_BETTER = {"latency_p50", "latency_p99", "latency_p999", "cpu_util", "mem_util", "net_util"}

MODES = ["streambazaar", "talos", "ds2", "capsys", "flink_default"]
MODE_DISPLAY = {
    "streambazaar":  "StreamBazaar",
    "talos":         "Talos",
    "ds2":           "DS2",
    "capsys":        "CAPSys",
    "flink_default": "Flink",
}

PALETTE  = ["#0072B2", "#D55E00", "#009E73", "#CC79A7", "#E69F00"]
HATCHES  = ["", "///", "\\\\\\", "|||", "xxx"]
MARKERS  = ["o", "s", "^", "D", "v"]
LINESTYLES = ["-", "--", "-.", ":", (0, (3, 1, 1, 1))]

FONT_SIZE   = 14
LABEL_SIZE  = 14
TICK_SIZE   = 14
LEGEND_SIZE = 14
FIG_W, FIG_H = 8, 4

WARMUP_SEC_SB    = 5   # warmup for streambazaar (mirrors run_scalability_experiment.py)
WARMUP_SEC_OTHER = 0


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


# ── CSV extraction helpers ────────────────────────────────────────────────────

def _mean_nz(vals: List[float]) -> float:
    nz = [v for v in vals if abs(v) > 1e-12]
    return sum(nz) / len(nz) if nz else 0.0


def _col_series(rows: List[Dict], col: str) -> List[float]:
    out = []
    for r in rows:
        try:
            out.append(float(r.get(col, 0) or 0))
        except (TypeError, ValueError):
            out.append(0.0)
    return out


def _steady_rows(rows: List[Dict], warmup_sec: int) -> List[Dict]:
    if warmup_sec <= 0 or not rows:
        return rows
    try:
        t0 = int(float(rows[0].get("timestamp_epoch", 0) or 0))
    except Exception:
        return rows
    cutoff = t0 + warmup_sec
    filtered = [r for r in rows if _ts(r) >= cutoff]
    return filtered if filtered else rows


def _ts(row: Dict) -> int:
    try:
        return int(float(row.get("timestamp_epoch", 0) or 0))
    except Exception:
        return 0


def _tenant_cols(all_cols: List[str], dataset: str, prefix: str, suffix: str) -> List[str]:
    """
    Collect all columns matching  <prefix>_tenant_<dataset>[_<num>]<suffix>
    e.g. prefix='throughput', suffix='_out'  → throughput_tenant_fraud_out,
                                                throughput_tenant_fraud_2_out, ...
    """
    pat = re.compile(
        rf"^{re.escape(prefix)}_tenant_{re.escape(dataset)}(?:_\d+)?{re.escape(suffix)}$"
    )
    return [c for c in all_cols if pat.match(c)]


def extract_dataset_kpis(csv_path: Path, dataset: str, mode: str) -> Dict[str, float]:
    """Return per-dataset KPI dict from one CSV file."""
    warmup = WARMUP_SEC_SB if mode == "streambazaar" else WARMUP_SEC_OTHER

    with csv_path.open(encoding="utf-8") as fp:
        rows = list(csv.DictReader(fp))
    if not rows:
        return {m: 0.0 for m in METRICS}

    rows = _steady_rows(rows, warmup)
    all_cols = list(rows[0].keys())

    def avg_cols(cols: List[str]) -> float:
        vals = []
        for c in cols:
            vals.extend(_col_series(rows, c))
        return _mean_nz(vals)

    # throughput — prefer goodput column (excludes retries/duplicates); fall back to _out
    tput_goodput_cols = _tenant_cols(all_cols, dataset, "throughput", "_goodput")
    tput_out_cols     = _tenant_cols(all_cols, dataset, "throughput", "_out")
    tput_in_cols      = _tenant_cols(all_cols, dataset, "throughput", "_in")

    # latency
    lat_p50_cols  = _tenant_cols(all_cols, dataset, "latency", "_p50_ms")
    lat_p99_cols  = _tenant_cols(all_cols, dataset, "latency", "_p99_ms")
    lat_p999_cols = _tenant_cols(all_cols, dataset, "latency", "_p999_ms")

    # per-tenant resource
    cpu_cols = _tenant_cols(all_cols, dataset, "checkpoint", "_cpu")
    mem_cols = _tenant_cols(all_cols, dataset, "checkpoint", "_memory")
    net_cols = _tenant_cols(all_cols, dataset, "checkpoint", "_network")

    goodput_val = avg_cols(tput_goodput_cols)
    return {
        "throughput_out": goodput_val if goodput_val > 1e-12 else avg_cols(tput_out_cols),
        "throughput_in":  avg_cols(tput_in_cols),
        "latency_p50":    avg_cols(lat_p50_cols),
        "latency_p99":    avg_cols(lat_p99_cols),
        "latency_p999":   avg_cols(lat_p999_cols),
        "cpu_util":       avg_cols(cpu_cols),
        "mem_util":       avg_cols(mem_cols),
        "net_util":       avg_cols(net_cols),
    }


# ── Directory walker ──────────────────────────────────────────────────────────

def build_per_dataset_aggregated(repeated_dir: Path, modes: List[str]) -> Dict:
    """
    Walk repeated_dir/run_*/scalability_*/csv_n{N}_{mode}/  and build:
      {dataset: {nc: {mode: {metric: {mean, std, runs}}}}}
    """
    run_dirs = sorted(repeated_dir.glob("run_*"))
    if not run_dirs:
        raise FileNotFoundError(f"No run_* subdirectories found in {repeated_dir}")

    # raw[dataset][nc][mode] → list of per-run dicts {metric: float}
    raw: Dict = {}
    for ds in DATASETS:
        raw[ds] = {}

    node_counts_seen = set()

    for run_dir in run_dirs:
        scalability_dirs = list(run_dir.glob("scalability_*"))
        if not scalability_dirs:
            print(f"  [warn] no scalability_* subdir in {run_dir.name}, skipping")
            continue
        scalability_dir = scalability_dirs[0]

        # csv_n{N}_{mode}
        for csv_dir in sorted(scalability_dir.glob("csv_n*_*")):
            m = re.match(r"csv_n(\d+)_(.+)$", csv_dir.name)
            if not m:
                continue
            nc   = int(m.group(1))
            mode = m.group(2)
            if mode not in modes:
                continue

            csvs = sorted(csv_dir.glob("prometheus_metrics_*.csv"))
            if not csvs:
                continue
            csv_path = csvs[-1]
            node_counts_seen.add(nc)

            for ds in DATASETS:
                kpis = extract_dataset_kpis(csv_path, ds, mode)
                raw[ds].setdefault(nc, {}).setdefault(mode, []).append(kpis)

    # aggregate runs → mean / std / runs list
    def _std(vals: List[float]) -> float:
        if len(vals) < 2:
            return 0.0
        m = sum(vals) / len(vals)
        return math.sqrt(sum((v - m) ** 2 for v in vals) / (len(vals) - 1))

    node_counts = sorted(node_counts_seen)
    aggregated: Dict = {}
    for ds in DATASETS:
        aggregated[ds] = {}
        for nc in node_counts:
            aggregated[ds][nc] = {}
            for mode in modes:
                run_list = raw[ds].get(nc, {}).get(mode, [])
                aggregated[ds][nc][mode] = {}
                for metric in METRICS:
                    vals = [r.get(metric, 0.0) for r in run_list]
                    aggregated[ds][nc][mode][metric] = {
                        "mean": sum(vals) / len(vals) if vals else 0.0,
                        "std":  _std(vals),
                        "runs": vals,
                    }
    return aggregated, node_counts


# ── Normalization ─────────────────────────────────────────────────────────────

def _sb_mean(agg: Dict, ds: str, nc: int, metric: str) -> float:
    v = agg[ds][nc].get("streambazaar", {}).get(metric, {}).get("mean", None)
    return float(v) if v and abs(float(v)) > 1e-12 else 1.0


def _nm(agg, ds, nc, mode, metric):
    raw = agg[ds][nc].get(mode, {}).get(metric, {}).get("mean", 0.0)
    return float(raw) / _sb_mean(agg, ds, nc, metric)


def _ns(agg, ds, nc, mode, metric):
    raw = agg[ds][nc].get(mode, {}).get(metric, {}).get("std", 0.0)
    return float(raw) / _sb_mean(agg, ds, nc, metric)


def _nr(agg, ds, nc, mode, metric):
    runs  = agg[ds][nc].get(mode, {}).get(metric, {}).get("runs", [])
    denom = _sb_mean(agg, ds, nc, metric)
    return [float(r) / denom for r in runs]


# ── Shared plot helpers ───────────────────────────────────────────────────────

def _save(fig: plt.Figure, out_dir: Path, stem: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "png"):
        fig.savefig(out_dir / f"{stem}.{ext}", bbox_inches="tight", dpi=200)
    print(f"  saved → {stem}.pdf/png")
    plt.close(fig)


def _ref_line(ax):
    ax.axhline(1.0, color="black", linewidth=1.3, linestyle="--", alpha=0.65, zorder=0)


def _ylabel(ax):
    ax.set_ylabel("Normalized Value\n(StreamBazaar = 1)", fontsize=LABEL_SIZE)


def _arrow(metric):
    return "↓ lower = better" if metric in LOWER_BETTER else "↑ higher = better"


def _mode_legend(ax, modes):
    handles = [
        mpatches.Patch(facecolor=PALETTE[k], hatch=HATCHES[k],
                       edgecolor="black", linewidth=0.8,
                       label=MODE_DISPLAY.get(modes[k], modes[k]))
        for k in range(len(modes))
    ]
    ax.legend(handles=handles, loc="upper left",
              frameon=True, framealpha=0.9, edgecolor="black",
              fontsize=LEGEND_SIZE, ncol=min(len(modes), 3))


def _group_positions(node_counts, modes, slot_w=0.14, group_gap=0.35):
    n_modes    = len(modes)
    group_span = n_modes * slot_w + group_gap
    pos = {}
    for gi, nc in enumerate(node_counts):
        origin = gi * group_span
        for mi, mode in enumerate(modes):
            pos[(nc, mode)] = origin + (mi - n_modes / 2 + 0.5) * slot_w
    return pos, group_span


def _group_xticks(ax, node_counts, group_span):
    centers = [i * group_span for i in range(len(node_counts))]
    ax.set_xticks(centers)
    ax.set_xticklabels([f"{nc} Nodes" for nc in node_counts], fontsize=TICK_SIZE)


# ═══════════════════════════════════════════════════════════════════════════════
# Plot functions (same five types as plot_scalability_repeated.py)
# Each accepts dataset label for title; saves to out_dir/<dataset>/
# ═══════════════════════════════════════════════════════════════════════════════

def plot_bar(agg, ds, metric, node_counts, modes, out_dir):
    pos, gs = _group_positions(node_counts, modes)
    apply_ieee_style()
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))
    slot_w = 0.14
    for mode, color, hatch in zip(modes, PALETTE, HATCHES):
        for nc in node_counts:
            m = _nm(agg, ds, nc, mode, metric)
            s = _ns(agg, ds, nc, mode, metric)
            ax.bar(pos[(nc, mode)], m, width=slot_w * 0.88,
                   color=color, hatch=hatch, edgecolor="black", linewidth=0.8,
                   yerr=s, capsize=4,
                   error_kw={"elinewidth": 1.4, "ecolor": "black", "capthick": 1.4},
                   zorder=3)
    _ref_line(ax)
    _group_xticks(ax, node_counts, gs)
    _ylabel(ax)
    ax.set_title(f"{METRIC_SHORT[metric]} — {DATASET_DISPLAY[ds]}  ({_arrow(metric)}, norm.)",
                 fontsize=FONT_SIZE, pad=8)
    _mode_legend(ax, modes)
    fig.tight_layout()
    _save(fig, out_dir / ds, f"bar_{metric}")


def plot_line(agg, ds, metric, node_counts, modes, out_dir):
    apply_ieee_style()
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))
    for mode, color, marker, ls in zip(modes, PALETTE, MARKERS, LINESTYLES):
        ys  = np.array([_nm(agg, ds, nc, mode, metric) for nc in node_counts])
        err = np.array([_ns(agg, ds, nc, mode, metric) for nc in node_counts])
        ax.plot(node_counts, ys, color=color, marker=marker,
                linestyle=ls if isinstance(ls, str) else "-",
                linewidth=2.0, markersize=8,
                label=MODE_DISPLAY.get(mode, mode), zorder=4)
        ax.fill_between(node_counts, ys - err, ys + err, color=color, alpha=0.15, zorder=2)
    _ref_line(ax)
    ax.set_xlabel("Number of Nodes", fontsize=LABEL_SIZE)
    ax.set_xticks(node_counts)
    _ylabel(ax)
    ax.set_title(f"{METRIC_SHORT[metric]} vs. Scale — {DATASET_DISPLAY[ds]}  ({_arrow(metric)}, norm.)",
                 fontsize=FONT_SIZE, pad=8)
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels, loc="upper left",
              frameon=True, framealpha=0.9, edgecolor="black",
              fontsize=LEGEND_SIZE, ncol=min(len(modes), 3))
    fig.tight_layout()
    _save(fig, out_dir / ds, f"line_{metric}")


def plot_box(agg, ds, metric, node_counts, modes, out_dir):
    slot_w = 0.14
    pos, gs = _group_positions(node_counts, modes, slot_w)
    apply_ieee_style()
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))
    for mode, color, hatch in zip(modes, PALETTE, HATCHES):
        for nc in node_counts:
            vals = _nr(agg, ds, nc, mode, metric)
            if not vals:
                continue
            bp = ax.boxplot(vals, positions=[pos[(nc, mode)]], widths=[slot_w * 0.82],
                            patch_artist=True, notch=False, showfliers=True,
                            flierprops=dict(marker="o", markersize=4,
                                            markerfacecolor=color, alpha=0.7),
                            medianprops=dict(color="black", linewidth=2.0),
                            whiskerprops=dict(color=color, linewidth=1.4),
                            capprops=dict(color=color, linewidth=1.4),
                            boxprops=dict(facecolor=color, edgecolor="black",
                                          linewidth=0.9, alpha=0.75),
                            zorder=3)
            for patch in bp["boxes"]:
                patch.set_hatch(hatch)
    _ref_line(ax)
    _group_xticks(ax, node_counts, gs)
    _ylabel(ax)
    ax.set_title(f"{METRIC_SHORT[metric]} — {DATASET_DISPLAY[ds]}  (box, norm. runs)",
                 fontsize=FONT_SIZE, pad=8)
    _mode_legend(ax, modes)
    fig.tight_layout()
    _save(fig, out_dir / ds, f"box_{metric}")


def plot_swarm(agg, ds, metric, node_counts, modes, out_dir):
    slot_w = 0.14
    pos, gs = _group_positions(node_counts, modes, slot_w)
    apply_ieee_style()
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))
    for mi, (mode, color) in enumerate(zip(modes, PALETTE)):
        first = True
        for gi, nc in enumerate(node_counts):
            vals = _nr(agg, ds, nc, mode, metric)
            if not vals:
                continue
            xc  = pos[(nc, mode)]
            rng = np.random.default_rng(seed=42 + gi * 11 + mi * 7)
            jit = (rng.random(len(vals)) - 0.5) * slot_w * 0.55
            ax.scatter([xc + j for j in jit], vals,
                       color=color, s=60, alpha=0.85,
                       edgecolors="black", linewidths=0.6, zorder=5,
                       label=MODE_DISPLAY.get(mode, mode) if first else "_nolegend_")
            ax.scatter([xc], [float(np.mean(vals))],
                       color=color, s=130, marker="D",
                       edgecolors="black", linewidths=1.2, zorder=6)
            first = False
    _ref_line(ax)
    _group_xticks(ax, node_counts, gs)
    _ylabel(ax)
    ax.set_title(f"{METRIC_SHORT[metric]} — {DATASET_DISPLAY[ds]}  (swarm, ◆ = mean, norm.)",
                 fontsize=FONT_SIZE, pad=8)
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels, loc="upper left",
              frameon=True, framealpha=0.9, edgecolor="black",
              fontsize=LEGEND_SIZE, ncol=min(len(modes), 3))
    fig.tight_layout()
    _save(fig, out_dir / ds, f"swarm_{metric}")


def plot_violin(agg, ds, metric, node_counts, modes, out_dir):
    slot_w = 0.14
    pos, gs = _group_positions(node_counts, modes, slot_w)
    apply_ieee_style()
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))
    any_drawn = False
    for mode, color, hatch in zip(modes, PALETTE, HATCHES):
        for nc in node_counts:
            vals = _nr(agg, ds, nc, mode, metric)
            xc   = pos[(nc, mode)]
            if len(vals) < 2:
                if vals:
                    ax.scatter([xc], vals, color=color, s=80,
                               edgecolors="black", linewidths=1.0, zorder=5)
                continue
            parts = ax.violinplot(vals, positions=[xc], widths=[slot_w * 0.88],
                                  showmeans=True, showmedians=False, showextrema=True)
            for pc in parts["bodies"]:
                pc.set_facecolor(color); pc.set_edgecolor("black")
                pc.set_linewidth(1.0);   pc.set_alpha(0.72); pc.set_hatch(hatch)
            for key in ("cmeans", "cmaxes", "cmins", "cbars"):
                if key in parts:
                    parts[key].set_color(color)
                    parts[key].set_linewidth(1.4)
            any_drawn = True
    if not any_drawn:
        plt.close(fig); return
    _ref_line(ax)
    _group_xticks(ax, node_counts, gs)
    _ylabel(ax)
    ax.set_title(f"{METRIC_SHORT[metric]} — {DATASET_DISPLAY[ds]}  (violin, norm. runs)",
                 fontsize=FONT_SIZE, pad=8)
    _mode_legend(ax, modes)
    fig.tight_layout()
    _save(fig, out_dir / ds, f"violin_{metric}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Per-dataset IEEE plots from a run_scalability_repeated experiment directory"
    )
    parser.add_argument("--repeated-dir", required=True,
                        help="Path to repeated_TIMESTAMP directory (contains run_1/, run_2/, ...)")
    parser.add_argument("--out-dir", default=None,
                        help="Output root (default: <repeated-dir>/plots_per_dataset)")
    parser.add_argument("--datasets", nargs="+", default=DATASETS,
                        choices=DATASETS)
    parser.add_argument("--metrics",  nargs="+", default=METRICS,
                        choices=METRICS)
    parser.add_argument("--modes",    nargs="+", default=MODES)
    parser.add_argument("--skip-violin", action="store_true")
    parser.add_argument("--save-json", action="store_true",
                        help="Also dump per_dataset_aggregated.json next to out-dir")
    args = parser.parse_args()

    repeated_dir = Path(args.repeated_dir).resolve()
    out_dir = (Path(args.out_dir).resolve() if args.out_dir
               else repeated_dir / "plots_per_dataset")

    print(f"Scanning  {repeated_dir}")
    agg, node_counts = build_per_dataset_aggregated(repeated_dir, args.modes)

    print(f"Node counts : {node_counts}")
    print(f"Datasets    : {args.datasets}")
    print(f"Metrics     : {args.metrics}")
    print(f"Modes       : {args.modes}")
    print(f"Output dir  : {out_dir}\n")

    if args.save_json:
        jp = repeated_dir / "per_dataset_aggregated.json"
        # convert int keys for JSON
        serial = {ds: {str(nc): v for nc, v in agg[ds].items()} for ds in agg}
        jp.write_text(json.dumps(serial, indent=2), encoding="utf-8")
        print(f"  saved aggregated JSON → {jp}\n")

    for ds in args.datasets:
        print(f"\n{'━'*56}")
        print(f"  Dataset: {DATASET_DISPLAY[ds]}")
        print(f"{'━'*56}")
        for metric in args.metrics:
            print(f"  ── {METRIC_SHORT[metric]}")
            plot_bar(agg, ds, metric, node_counts, args.modes, out_dir)
            plot_line(agg, ds, metric, node_counts, args.modes, out_dir)
            plot_box(agg, ds, metric, node_counts, args.modes, out_dir)
            plot_swarm(agg, ds, metric, node_counts, args.modes, out_dir)
            if not args.skip_violin:
                plot_violin(agg, ds, metric, node_counts, args.modes, out_dir)

    print(f"\n[done] all per-dataset plots → {out_dir}")
    print("  Subdirectories: " + "  ".join(args.datasets))


if __name__ == "__main__":
    main()
