#!/usr/bin/env python3
"""
IEEE-quality scalability comparison plots — all modes across 1/2/4/8/16 nodes.

All metrics are normalized so StreamBazaar = 1.0 at every node count.
  - Latency  (lower-is-better): normalized = competitor / sb  → sb=1, <1 is better
  - Throughput/RUE/EEI (higher-is-better): normalized = competitor / sb → sb=1, >1 is better
  - MIS / CPU (lower-is-better): same sign convention as latency

Usage:
    python3 evaluation/plot_scalability.py \
        --results-json evaluation/results/scalability_runs/scalability_.../scalability_comparison.json

Figures produced (PNG 300 dpi + PDF):
    fig_throughput_vs_nodes.{png,pdf}
    fig_scalability_efficiency.{png,pdf}
    fig_latency_p99_vs_nodes.{png,pdf}
    fig_rue_vs_nodes.{png,pdf}
    fig_eei_vs_nodes.{png,pdf}
    fig_mis_vs_nodes.{png,pdf}
    fig_cpu_vs_nodes.{png,pdf}
    fig_scalability_panel.{png,pdf}
    per_dataset/<dataset>/fig_latency_p99_<dataset>.{png,pdf}
    per_dataset/<dataset>/fig_throughput_<dataset>.{png,pdf}
    per_dataset/<dataset>/fig_latency_p99_bar_<dataset>.{png,pdf}
    per_dataset/<dataset>/fig_throughput_bar_<dataset>.{png,pdf}
    per_dataset/<dataset>/fig_cpu_bar_<dataset>.{png,pdf}
    per_dataset/<dataset>/fig_rue_bar_<dataset>.{png,pdf}
    per_dataset/<dataset>/fig_eei_bar_<dataset>.{png,pdf}
    fig_cpu_dataset_bars.{png,pdf}
    fig_rue_dataset_bars.{png,pdf}
    fig_eei_dataset_bars.{png,pdf}
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# ── IEEE style ----------------------------------------------------------------
FONT_SIZE   = 16
FIG_SIZE    = (8, 4)
DOUBLE_COL  = 7.16
SINGLE_COL  = 3.5

plt.rcParams.update({
    "font.family":        "serif",
    "font.serif":         ["Times New Roman", "Times", "DejaVu Serif"],
    "font.size":          FONT_SIZE,
    "axes.titlesize":     FONT_SIZE,
    "axes.labelsize":     FONT_SIZE,
    "xtick.labelsize":    FONT_SIZE - 2,
    "ytick.labelsize":    FONT_SIZE - 2,
    "legend.fontsize":    FONT_SIZE - 3,
    "figure.dpi":         300,
    "savefig.dpi":        300,
    "savefig.bbox":       "tight",
    "savefig.pad_inches": 0.04,
    "axes.linewidth":     1.0,
    "grid.linewidth":     0.6,
    "lines.linewidth":    2.0,
    "lines.markersize":   8,
})

MODES = ["streambazaar", "talos", "ds2", "capsys", "flink_default"]
MODE_LABELS = {
    "streambazaar":  "StreamBazaar",
    "talos":         "TALOS",
    "ds2":           "DS2",
    "capsys":        "CAPSys",
    "flink_default": "Flink Default",
}

# Line styles: color + marker + linestyle + dashes for IEEE distinction
MODE_STYLES = {
    "streambazaar":  dict(color="#0072B2", marker="o",  ls="-",   lw=2.4, zorder=5,
                          markersize=9),
    "talos":         dict(color="#E69F00", marker="s",  ls="--",  lw=1.8,
                          dashes=(5, 2)),
    "ds2":           dict(color="#009E73", marker="^",  ls="-.",  lw=1.8),
    "capsys":        dict(color="#D55E00", marker="D",  ls=":",   lw=1.8),
    "flink_default": dict(color="#CC79A7", marker="v",  ls=(0,(3,1,1,1)), lw=1.8),
}

# Bar hatch patterns (one per mode)
MODE_HATCHES = {
    "streambazaar":  "",
    "talos":         "///",
    "ds2":           "\\\\\\",
    "capsys":        "xxx",
    "flink_default": "ooo",
}

LOWER_IS_BETTER = {"latency_p50", "latency_p99", "latency_p999", "mis", "cpu_util"}

DATASET_LABELS = {
    "fraud":       "Fraud Detection\n(high priority)",
    "clickstream": "Clickstream\n(low priority)",
    "ml":          "ML Workload\n(high priority)",
    "iot":         "IoT Sensors\n(medium priority)",
    "web":         "Web Analytics\n(low priority)",
    "intrusion":   "Network Intrusion\n(high priority)",
}
DATASET_ORDER = ["fraud", "clickstream", "ml", "iot", "web", "intrusion"]


# ── Normalization helpers -------------------------------------------------------

NAN = float("nan")


def _raw(d: Dict, *keys) -> float:
    """Walk nested dicts; return NaN if key missing or value is 0/empty."""
    val = d
    for k in keys:
        if not isinstance(val, dict):
            return NAN
        val = val.get(k, None)
        if val is None:
            return NAN
    try:
        f = float(val)
        return f if abs(f) > 1e-12 else NAN   # treat exact-zero as missing
    except (TypeError, ValueError):
        return NAN


def normalize(data: Dict, ns: List[int], modes: List[str], metric: str
              ) -> Dict[int, Dict[str, float]]:
    """
    Returns norm[nc][mode] = value / sb_value  (StreamBazaar = 1.0).
    Returns NaN when either the value or the SB baseline is missing/zero.
    """
    norm: Dict[int, Dict[str, float]] = {}
    for nc in ns:
        sb_val = _raw(data, nc, "streambazaar", metric)
        norm[nc] = {}
        for m in modes:
            raw = _raw(data, nc, m, metric)
            if np.isnan(sb_val) or np.isnan(raw):
                norm[nc][m] = NAN
            else:
                norm[nc][m] = raw / sb_val
    return norm


def normalize_per_ds(
    per_ds: Dict, ns: List[int], modes: List[str],
    datasets: List[str], metric: str,
) -> Dict[int, Dict[str, Dict[str, float]]]:
    """norm[nc][mode][dataset] = value / sb_value.  NaN when data is absent/zero."""
    norm: Dict[int, Dict[str, Dict[str, float]]] = {}
    for nc in ns:
        norm[nc] = {}
        for ds in datasets:
            sb_val = _raw(per_ds, nc, "streambazaar", ds, metric)
            for m in modes:
                raw = _raw(per_ds, nc, m, ds, metric)
                if np.isnan(sb_val) or np.isnan(raw):
                    result = NAN
                else:
                    result = raw / sb_val
                norm[nc].setdefault(m, {})[ds] = result
    return norm


# ── Per-dataset CSV loading ---------------------------------------------------

def _mean_nonzero(values: List[float]) -> float:
    nz = [v for v in values if abs(v) > 1e-12]
    return sum(nz) / len(nz) if nz else 0.0


def _load_csv_series(csv_path: Path, warmup_sec: int = 15) -> Tuple[List[dict], List[str]]:
    with csv_path.open("r", encoding="utf-8") as fp:
        rows = list(csv.DictReader(fp))
    if not rows:
        return [], []
    cols = list(rows[0].keys())
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
    return rows, cols


def _series(rows: List[dict], col: str) -> List[float]:
    out = []
    for r in rows:
        try:
            out.append(float(r.get(col, "0") or 0.0))
        except Exception:
            out.append(0.0)
    return out


def _base_dataset(name: str) -> str:
    return re.sub(r"_\d+$", "", name)


def _detect_datasets(cols: List[str]) -> List[str]:
    found = set()
    for c in cols:
        m = re.match(r"latency_tenant_([a-z0-9_]+)_p99_ms", c)
        if m:
            found.add(_base_dataset(m.group(1)))
    return [d for d in DATASET_ORDER if d in found] + \
           sorted(d for d in found if d not in DATASET_ORDER)


def load_per_dataset_kpis(
    results_dir: Path,
    node_counts: List[int],
    modes: List[str],
    aggregate_data: Optional[Dict] = None,
    warmup_sec: int = 15,
) -> Dict[int, Dict[str, Dict[str, Dict[str, float]]]]:
    result: Dict[int, Dict[str, Dict[str, Dict[str, float]]]] = {}
    for nc in node_counts:
        result[nc] = {}
        for mode in modes:
            csv_dir = results_dir / f"csv_n{nc}_{mode}"
            if not csv_dir.exists():
                continue
            csvs = sorted(csv_dir.glob("prometheus_metrics_*.csv"))
            if not csvs:
                continue
            rows, cols = _load_csv_series(csvs[-1], warmup_sec)
            if not rows:
                continue
            tenant_cols: Dict[str, List[str]] = {}
            for c in cols:
                m2 = re.match(r"latency_tenant_([a-z0-9_]+)_p99_ms", c)
                if m2:
                    base = _base_dataset(m2.group(1))
                    tenant_cols.setdefault(base, []).append(m2.group(1))

            # cluster-level RUE and EEI (no per-tenant breakdown in CSV)
            cluster_rue = _mean_nonzero(
                [v for col in cols if col.endswith("rue_cluster")
                 for v in _series(rows, col)]
            )
            cluster_eei = _mean_nonzero(
                [v for col in cols if re.fullmatch(r"(node\d+_)?eei", col)
                 for v in _series(rows, col)]
            )
            # fall back to aggregate JSON if CSV values are zero
            if aggregate_data and abs(cluster_rue) < 1e-12:
                cluster_rue = float(aggregate_data.get(nc, {}).get(mode, {}).get("rue", 0.0))
            if aggregate_data and abs(cluster_eei) < 1e-12:
                cluster_eei = float(aggregate_data.get(nc, {}).get(mode, {}).get("eei", 0.0))

            result[nc][mode] = {}
            for base_ds, tenants in tenant_cols.items():
                def _avg(suffix: str, _t: List[str] = tenants) -> float:
                    vals = []
                    for t in _t:
                        vals.extend(_series(rows, f"latency_tenant_{t}{suffix}"))
                    return _mean_nonzero(vals)

                def _avg_tp(suffix: str, _t: List[str] = tenants) -> float:
                    vals = []
                    for t in _t:
                        vals.extend(_series(rows, f"throughput_tenant_{t}{suffix}"))
                    return _mean_nonzero(vals)

                def _avg_cpu(_t: List[str] = tenants) -> float:
                    vals = []
                    for t in _t:
                        vals.extend(_series(rows, f"checkpoint_tenant_{t}_cpu"))
                    return _mean_nonzero(vals)

                goodput = _avg_tp("_goodput")
                result[nc][mode][base_ds] = {
                    "latency_p50":    _avg("_p50_ms"),
                    "latency_p99":    _avg("_p99_ms"),
                    "latency_p999":   _avg("_p999_ms"),
                    "throughput_in":  _avg_tp("_in"),
                    # Use goodput (excl. retries/duplicates) as the headline throughput metric;
                    # fall back to raw out only when goodput column is absent/zero.
                    "throughput_out": goodput if goodput > 1e-12 else _avg_tp("_out"),
                    "cpu_util":       _avg_cpu(),
                    "rue":            cluster_rue,
                    "eei":            cluster_eei,
                }
    return result


# ── Helpers -------------------------------------------------------------------

def _v(data: Dict, nc: int, mode: str, metric: str) -> float:
    return float(data.get(nc, {}).get(mode, {}).get(metric, 0.0))


def _save(fig: plt.Figure, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(path))
    fig.savefig(str(path.with_suffix(".pdf")))
    plt.close(fig)
    print(f"  Saved: {path.name}  {path.with_suffix('.pdf').name}")


def _legend(ax: plt.Axes, modes: List[str], **kwargs) -> None:
    handles = [
        plt.Line2D([0], [0], label=MODE_LABELS[m], **MODE_STYLES[m])
        for m in modes
    ]
    ax.legend(handles=handles, framealpha=0.85, edgecolor="gray", **kwargs)


def _grid(ax: plt.Axes) -> None:
    ax.grid(alpha=0.3, linestyle="--", zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def _norm_ylabel(metric: str, orig_ylabel: str) -> str:
    return f"Normalized {orig_ylabel}\n(StreamBazaar = 1)"


def _ref_line(ax: plt.Axes) -> None:
    ax.axhline(1.0, color="black", ls=":", lw=1.2, alpha=0.6, zorder=2)


# ── Aggregate line plots (normalized) ----------------------------------------

def plot_metric_vs_nodes(
    data, ns, modes, metric, title, ylabel, out_path,
    lower_is_better=False, add_ideal=False,
) -> None:
    norm = normalize(data, ns, modes, metric)

    fig, ax = plt.subplots(figsize=FIG_SIZE)
    for m in modes:
        vals = [norm[nc].get(m, NAN) for nc in ns]
        ax.plot(ns, vals, label=MODE_LABELS[m], **MODE_STYLES[m])

    if add_ideal:
        ax.plot(ns, [ns[i] / ns[0] for i in range(len(ns))],
                "k:", lw=1.2, label="Ideal linear", zorder=3)

    _ref_line(ax)
    direction = "↓ lower is better" if lower_is_better else "↑ higher is better"
    ax.set_title(f"{title}  ({direction})", pad=6)
    ax.set_ylabel(_norm_ylabel(metric, ylabel))
    ax.set_xlabel("Number of Nodes")
    ax.set_xticks(ns)
    ax.set_xticklabels([str(n) for n in ns])
    _legend(ax, modes, loc="best")
    _grid(ax)
    fig.tight_layout(pad=0.5)
    _save(fig, out_path)


def plot_scalability_efficiency(data, ns, modes, out_path) -> None:
    fig, ax = plt.subplots(figsize=FIG_SIZE)
    for m in modes:
        base = _v(data, ns[0], m, "throughput_out")
        if base < 1e-6:
            continue
        effs = [(_v(data, nc, m, "throughput_out") / base) / (nc / ns[0]) * 100
                for nc in ns]
        ax.plot(ns, effs, label=MODE_LABELS[m], **MODE_STYLES[m])
    ax.axhline(100, color="black", ls=":", lw=1.2, label="Ideal (100%)")
    ax.set_title("Horizontal Scalability Efficiency  (↑ higher is better)", pad=6)
    ax.set_ylabel("Scalability Efficiency (%)")
    ax.set_xlabel("Number of Nodes")
    ax.set_xticks(ns)
    ax.set_xticklabels([str(n) for n in ns])
    ax.set_ylim(0, 115)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter())
    _legend(ax, modes, loc="lower left")
    _grid(ax)
    fig.tight_layout(pad=0.5)
    _save(fig, out_path)


# ── Per-dataset: one figure per dataset, line chart (normalized) ──────────────

def plot_per_dataset_line(
    per_ds: Dict, ns: List[int], modes: List[str],
    dataset: str, metric: str,
    title: str, ylabel: str, out_path: Path,
    lower_is_better: bool = False,
) -> None:
    """Line chart for a single dataset, normalized to StreamBazaar=1."""
    norm = normalize_per_ds(per_ds, ns, modes, [dataset], metric)

    fig, ax = plt.subplots(figsize=FIG_SIZE)
    for m in modes:
        vals = [norm.get(nc, {}).get(m, {}).get(dataset, NAN) for nc in ns]
        # matplotlib line plots skip NaN points automatically
        ax.plot(ns, vals, label=MODE_LABELS[m], **MODE_STYLES[m])

    _ref_line(ax)
    direction = "↓ lower is better" if lower_is_better else "↑ higher is better"
    ds_label = DATASET_LABELS.get(dataset, dataset).replace("\n", " ")
    ax.set_title(f"{title} — {ds_label}  ({direction})", pad=6)
    ax.set_ylabel(_norm_ylabel(metric, ylabel))
    ax.set_xlabel("Number of Nodes")
    ax.set_xticks(ns)
    ax.set_xticklabels([str(n) for n in ns])
    _legend(ax, modes, loc="best")
    _grid(ax)
    fig.tight_layout(pad=0.5)
    _save(fig, out_path)


# ── Per-dataset: bar chart (normalized) ──────────────────────────────────────

def plot_per_dataset_bar(
    per_ds: Dict, ns: List[int], modes: List[str],
    dataset: str, metric: str,
    title: str, ylabel: str, out_path: Path,
    lower_is_better: bool = False,
) -> None:
    """Grouped bar chart: x=node counts, groups=algorithms, normalized."""
    norm = normalize_per_ds(per_ds, ns, modes, [dataset], metric)

    x = np.arange(len(ns))
    bar_w = 0.8 / max(len(modes), 1)

    fig, ax = plt.subplots(figsize=FIG_SIZE)
    for i, m in enumerate(modes):
        raw_vals = [norm.get(nc, {}).get(m, {}).get(dataset, NAN) for nc in ns]
        vals = [0.0 if np.isnan(v) else v for v in raw_vals]
        offset = (i - len(modes) / 2 + 0.5) * bar_w
        ax.bar(x + offset, vals, bar_w * 0.88,
               color=MODE_STYLES[m]["color"],
               hatch=MODE_HATCHES[m],
               edgecolor="black", linewidth=0.6,
               label=MODE_LABELS[m], zorder=3)

    ax.axhline(1.0, color="black", ls=":", lw=1.2, alpha=0.7, zorder=2)
    direction = "↓ lower is better" if lower_is_better else "↑ higher is better"
    ds_label = DATASET_LABELS.get(dataset, dataset).replace("\n", " ")
    ax.set_title(f"{title} — {ds_label}  ({direction})", pad=6)
    ax.set_ylabel(_norm_ylabel(metric, ylabel))
    ax.set_xlabel("Number of Nodes")
    ax.set_xticks(x)
    ax.set_xticklabels([str(n) for n in ns])
    ax.legend(framealpha=0.85, edgecolor="gray")
    _grid(ax)
    fig.tight_layout(pad=0.5)
    _save(fig, out_path)


# ── Cross-dataset bar (normalized, all datasets at fixed node count) ──────────

def plot_cross_dataset_bar(
    per_ds: Dict, ns: List[int], modes: List[str],
    datasets: List[str], metric: str,
    title: str, ylabel: str, out_path: Path,
    lower_is_better: bool = False,
) -> None:
    """
    One subplot per node count.
    x-axis = datasets, grouped bars = algorithms, normalized to SB=1.
    """
    n_ns = len(ns)
    fig, axes = plt.subplots(1, n_ns,
                             figsize=(FIG_SIZE[0] * max(n_ns, 1), FIG_SIZE[1]),
                             squeeze=False)
    x = np.arange(len(datasets))
    bar_w = 0.8 / max(len(modes), 1)

    for ax, nc in zip(axes[0], ns):
        norm = normalize_per_ds(per_ds, [nc], modes, datasets, metric)
        for i, m in enumerate(modes):
            raw_vals = [norm.get(nc, {}).get(m, {}).get(ds, NAN) for ds in datasets]
            # Replace NaN with 0 for bar height; mark missing bars with hatching only
            vals = [0.0 if np.isnan(v) else v for v in raw_vals]
            offset = (i - len(modes) / 2 + 0.5) * bar_w
            ax.bar(x + offset, vals, bar_w * 0.88,
                   color=MODE_STYLES[m]["color"],
                   hatch=MODE_HATCHES[m],
                   edgecolor="black", linewidth=0.6,
                   label=MODE_LABELS[m], zorder=3)
        ax.axhline(1.0, color="black", ls=":", lw=1.2, alpha=0.7, zorder=2)
        ax.set_title(f"{nc} Node{'s' if nc > 1 else ''}", fontsize=FONT_SIZE)
        ax.set_xticks(x)
        ds_ticks = [DATASET_LABELS.get(d, d).split("\n")[0] for d in datasets]
        ax.set_xticklabels(ds_ticks, rotation=25, ha="right",
                           fontsize=FONT_SIZE - 3)
        ax.set_ylabel(_norm_ylabel(metric, ylabel), fontsize=FONT_SIZE - 2)
        _grid(ax)

    handles = [
        plt.Rectangle((0, 0), 1, 1,
                       facecolor=MODE_STYLES[m]["color"],
                       hatch=MODE_HATCHES[m],
                       edgecolor="black", linewidth=0.6,
                       label=MODE_LABELS[m])
        for m in modes
    ]
    direction = "↓ lower" if lower_is_better else "↑ higher"
    fig.legend(handles=handles, loc="lower center", ncol=len(modes),
               fontsize=FONT_SIZE - 3, framealpha=0.85, edgecolor="gray",
               bbox_to_anchor=(0.5, -0.05))
    fig.suptitle(f"{title}  ({direction} is better, SB = 1)",
                 fontsize=FONT_SIZE, y=1.02)
    fig.tight_layout(pad=0.55, rect=[0, 0.08, 1, 1])
    _save(fig, out_path)


# ── Panel (normalized) --------------------------------------------------------

def plot_panel(data, ns, modes, out_path) -> None:
    panels = [
        ("throughput_out", "Goodput\n(norm.)", False,  True),
        (None,             "Scale Eff. (%)",      False,  False),
        ("latency_p99",    "Latency p99\n(norm.)", True,  False),
        ("rue",            "RUE\n(norm.)",          False, False),
        ("eei",            "EEI\n(norm.)",          False, False),
        ("mis",            "MIS\n(norm.)",           True,  False),
    ]
    fig, axes = plt.subplots(2, 3, figsize=(DOUBLE_COL * 1.6, DOUBLE_COL * 1.0))
    for ax, (metric, ylabel, lib, with_ideal) in zip(axes.flatten(), panels):
        if metric is None:
            for m in modes:
                base = _v(data, ns[0], m, "throughput_out")
                if base < 1e-6:
                    continue
                effs = [(_v(data, nc, m, "throughput_out") / base) / (nc / ns[0]) * 100
                        for nc in ns]
                ax.plot(ns, effs, **MODE_STYLES[m])
            ax.axhline(100, color="black", ls=":", lw=1.0)
            ax.set_ylim(0, 115)
            ax.yaxis.set_major_formatter(mticker.PercentFormatter())
        else:
            norm = normalize(data, ns, modes, metric)
            for m in modes:
                vals = [norm[nc].get(m, NAN) for nc in ns]
                ax.plot(ns, vals, **MODE_STYLES[m])
            if with_ideal:
                ax.plot(ns, [ns[i] / ns[0] for i in range(len(ns))],
                        "k:", lw=1.0)
            _ref_line(ax)
        direction = "↓" if lib else "↑"
        ax.set_title(f"{ylabel}  {direction}", fontsize=FONT_SIZE - 2)
        ax.set_xticks(ns)
        ax.set_xticklabels([str(n) for n in ns], fontsize=FONT_SIZE - 4)
        ax.set_xlabel("Nodes", fontsize=FONT_SIZE - 3)
        _grid(ax)

    handles = [plt.Line2D([0], [0], label=MODE_LABELS[m], **MODE_STYLES[m])
               for m in modes]
    fig.legend(handles=handles, loc="lower center", ncol=len(modes),
               fontsize=FONT_SIZE - 4, framealpha=0.85, edgecolor="gray",
               bbox_to_anchor=(0.5, -0.02))
    fig.suptitle("StreamBazaar vs Baselines — Scalability Overview (SB = 1)",
                 fontsize=FONT_SIZE, y=1.01)
    fig.tight_layout(pad=0.55, rect=[0, 0.06, 1, 1])
    _save(fig, out_path)


# ── CMD printing --------------------------------------------------------------

def print_summary(data: Dict, ns: List[int], modes: List[str]) -> None:
    metrics = [
        ("throughput_out", "Goodput (msgs/s)", False),
        ("latency_p99",    "Latency p99 (ms)",    True),
        ("rue",            "RUE",                  False),
        ("eei",            "EEI",                  False),
        ("mis",            "MIS",                  True),
        ("cpu_util",       "CPU %",                True),
    ]
    print("\n" + "═" * 80)
    print("  AGGREGATE METRICS (all datasets combined) — normalized to StreamBazaar = 1")
    print("═" * 80)
    for metric, label, lib in metrics:
        direction = "↓ lower is better" if lib else "↑ higher is better"
        print(f"\n── {label}  {direction} ──")
        norm = normalize(data, ns, modes, metric)
        header = f"{'Mode':<18}" + "".join(f"  {n}N".rjust(9) for n in ns)
        print(header)
        print("-" * len(header))
        for m in modes:
            def _fmt(v: float) -> str:
                return "     N/A" if np.isnan(v) else f"{v:>8.3f}"
            row = f"{m:<18}" + "".join(f"  {_fmt(norm[nc].get(m, NAN))}" for nc in ns)
            marker = "  ← baseline" if m == "streambazaar" else ""
            print(row + marker)


def print_per_dataset_summary(
    per_ds: Dict, ns: List[int], modes: List[str], datasets: List[str],
) -> None:
    metrics = [
        ("latency_p99",    "Latency p99 (ms)",    True),
        ("latency_p999",   "Latency p999 (ms)",   True),
        ("throughput_out", "Goodput (excl. retries)", False),
        ("throughput_in",  "Throughput in",        False),
    ]

    print("\n" + "═" * 80)
    print("  PER-DATASET METRICS — normalized to StreamBazaar = 1")
    print("═" * 80)

    for ds in datasets:
        label = DATASET_LABELS.get(ds, ds).replace("\n", " ")
        print(f"\n{'─' * 80}")
        print(f"  Dataset: {label}")
        print(f"{'─' * 80}")

        for metric, mlabel, lib in metrics:
            direction = "↓ lower" if lib else "↑ higher"
            print(f"\n  {mlabel}  ({direction} is better)")
            norm = normalize_per_ds(per_ds, ns, modes, [ds], metric)
            header = f"  {'Mode':<18}" + "".join(f"  {n}N".rjust(9) for n in ns)
            print(header)
            print("  " + "-" * (len(header) - 2))
            for m in modes:
                vals = [norm.get(nc, {}).get(m, {}).get(ds, NAN) for nc in ns]
                marker = " ◀ baseline" if m == "streambazaar" else ""
                def _fmt2(v: float) -> str:
                    return "     N/A" if np.isnan(v) else f"{v:>8.3f}"
                row = f"  {m:<18}" + "".join(f"  {_fmt2(v)}" for v in vals) + marker
                print(row)


# ── Entry point ---------------------------------------------------------------

def find_latest(results_root: Path) -> Optional[Path]:
    for s in sorted(results_root.glob("scalability_*"), reverse=True):
        p = s / "scalability_comparison.json"
        if p.exists():
            return p
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot scalability comparison figures")
    parser.add_argument("--results-json",  default=None)
    parser.add_argument("--results-root",  default="evaluation/results/scalability_runs")
    parser.add_argument("--fig-dir",       default=None)
    parser.add_argument("--warmup-sec",    type=int, default=15)
    parser.add_argument("--no-per-dataset", action="store_true",
                        help="Skip per-dataset figures and tables")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]

    if args.results_json:
        json_path = Path(args.results_json) if Path(args.results_json).is_absolute() \
                    else root / args.results_json
    else:
        json_path = find_latest(root / args.results_root)
        if json_path is None:
            print("No scalability_comparison.json found.")
            print("Run: python3 evaluation/run_scalability_experiment.py --node-counts 1 2 4")
            return

    print(f"Loading: {json_path}")
    raw  = json.loads(json_path.read_text(encoding="utf-8"))
    data = {int(k): {m: v for m, v in mv.items()} for k, mv in raw.items()}
    ns   = sorted(data.keys())
    modes = [m for m in MODES if any(m in data.get(nc, {}) for nc in ns)]
    print(f"Node counts: {ns}")
    print(f"Modes: {modes}")

    fig_dir = Path(args.fig_dir) if args.fig_dir \
              else json_path.parent / "scalability_figures"
    fig_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output: {fig_dir}\n")

    # ── Aggregate line figures (normalized) ──────────────────────────────────
    plot_metric_vs_nodes(
        data, ns, modes, "throughput_out",
        "Aggregate Goodput vs Nodes", "Goodput (msgs/s)",
        fig_dir / "fig_throughput_vs_nodes.png",
        add_ideal=True,
    )
    plot_scalability_efficiency(
        data, ns, modes, fig_dir / "fig_scalability_efficiency.png"
    )
    plot_metric_vs_nodes(
        data, ns, modes, "latency_p99",
        "Tail Latency (p99) vs Nodes", "Latency (ms)",
        fig_dir / "fig_latency_p99_vs_nodes.png",
        lower_is_better=True,
    )
    plot_metric_vs_nodes(
        data, ns, modes, "rue",
        "Resource Utilization Efficiency (RUE)", "RUE Score",
        fig_dir / "fig_rue_vs_nodes.png",
    )
    plot_metric_vs_nodes(
        data, ns, modes, "eei",
        "Economic Efficiency Index (EEI)", "EEI Score",
        fig_dir / "fig_eei_vs_nodes.png",
    )
    plot_metric_vs_nodes(
        data, ns, modes, "mis",
        "Migration Impact Score (MIS)", "MIS Score",
        fig_dir / "fig_mis_vs_nodes.png",
        lower_is_better=True,
    )
    plot_metric_vs_nodes(
        data, ns, modes, "cpu_util",
        "CPU Utilization per Node", "CPU (%)",
        fig_dir / "fig_cpu_vs_nodes.png",
        lower_is_better=True,
    )
    plot_panel(data, ns, modes, fig_dir / "fig_scalability_panel.png")

    # ── Aggregate CMD summary ─────────────────────────────────────────────────
    print_summary(data, ns, modes)

    # ── Per-dataset ───────────────────────────────────────────────────────────
    if not args.no_per_dataset:
        print("\n\nLoading per-dataset metrics from CSV directories …")
        per_ds = load_per_dataset_kpis(
            json_path.parent, ns, modes,
            aggregate_data=data,
            warmup_sec=args.warmup_sec,
        )
        datasets: List[str] = []
        for nc in ns:
            for m in modes:
                for ds in per_ds.get(nc, {}).get(m, {}).keys():
                    if ds not in datasets:
                        datasets.append(ds)
        datasets = [d for d in DATASET_ORDER if d in datasets] + \
                   [d for d in datasets if d not in DATASET_ORDER]

        if not datasets:
            print("  No per-dataset CSV data found — skipping per-dataset output.")
            print("  (CSV directories must exist alongside scalability_comparison.json)")
        else:
            print(f"Datasets found: {datasets}\n")

            for ds in datasets:
                ds_dir = fig_dir / "per_dataset" / ds
                ds_dir.mkdir(parents=True, exist_ok=True)

                # ── Line charts (normalized) ──────────────────────────────
                plot_per_dataset_line(
                    per_ds, ns, modes, ds, "latency_p99",
                    "Tail Latency p99", "Latency (ms)",
                    ds_dir / f"fig_latency_p99_{ds}.png",
                    lower_is_better=True,
                )
                plot_per_dataset_line(
                    per_ds, ns, modes, ds, "latency_p999",
                    "Tail Latency p999", "Latency (ms)",
                    ds_dir / f"fig_latency_p999_{ds}.png",
                    lower_is_better=True,
                )
                plot_per_dataset_line(
                    per_ds, ns, modes, ds, "throughput_out",
                    "Goodput (excl. retries)", "msgs/s",
                    ds_dir / f"fig_throughput_{ds}.png",
                    lower_is_better=False,
                )

                # ── Bar charts (normalized, x=node counts) ────────────────
                plot_per_dataset_bar(
                    per_ds, ns, modes, ds, "latency_p99",
                    "Latency p99", "Latency (ms)",
                    ds_dir / f"fig_latency_p99_bar_{ds}.png",
                    lower_is_better=True,
                )
                plot_per_dataset_bar(
                    per_ds, ns, modes, ds, "throughput_out",
                    "Goodput (excl. retries)", "msgs/s",
                    ds_dir / f"fig_throughput_bar_{ds}.png",
                    lower_is_better=False,
                )
                plot_per_dataset_bar(
                    per_ds, ns, modes, ds, "cpu_util",
                    "CPU Utilization", "CPU (%)",
                    ds_dir / f"fig_cpu_bar_{ds}.png",
                    lower_is_better=True,
                )
                plot_per_dataset_bar(
                    per_ds, ns, modes, ds, "rue",
                    "Resource Utilization Efficiency (RUE)", "RUE Score",
                    ds_dir / f"fig_rue_bar_{ds}.png",
                    lower_is_better=False,
                )
                plot_per_dataset_bar(
                    per_ds, ns, modes, ds, "eei",
                    "Economic Efficiency Index (EEI)", "EEI Score",
                    ds_dir / f"fig_eei_bar_{ds}.png",
                    lower_is_better=False,
                )

            # ── Cross-dataset bar charts ──────────────────────────────────
            plot_cross_dataset_bar(
                per_ds, ns, modes, datasets, "latency_p99",
                "Latency p99", "ms",
                fig_dir / "fig_latency_p99_dataset_bars.png",
                lower_is_better=True,
            )
            plot_cross_dataset_bar(
                per_ds, ns, modes, datasets, "throughput_out",
                "Goodput (excl. retries)", "msgs/s",
                fig_dir / "fig_throughput_dataset_bars.png",
                lower_is_better=False,
            )
            plot_cross_dataset_bar(
                per_ds, ns, modes, datasets, "cpu_util",
                "CPU Utilization", "CPU (%)",
                fig_dir / "fig_cpu_dataset_bars.png",
                lower_is_better=True,
            )
            plot_cross_dataset_bar(
                per_ds, ns, modes, datasets, "rue",
                "Resource Utilization Efficiency (RUE)", "RUE Score",
                fig_dir / "fig_rue_dataset_bars.png",
                lower_is_better=False,
            )
            plot_cross_dataset_bar(
                per_ds, ns, modes, datasets, "eei",
                "Economic Efficiency Index (EEI)", "EEI Score",
                fig_dir / "fig_eei_dataset_bars.png",
                lower_is_better=False,
            )

            print_per_dataset_summary(per_ds, ns, modes, datasets)

    print(f"\nAll figures saved to: {fig_dir}")


if __name__ == "__main__":
    main()
