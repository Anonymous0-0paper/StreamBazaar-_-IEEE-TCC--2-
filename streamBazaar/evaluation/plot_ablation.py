#!/usr/bin/env python3
"""
StreamBazaar ablation study — IEEE-quality figures and LaTeX tables.

Reads ablation_comparison.json produced by run_ablation_experiment.py and
generates:

  Figures (PNG 300 dpi + PDF):
    fig_ablation_bar_panel.{png,pdf}     — 2×3 panel: all KPIs as bar charts
    fig_ablation_latency.{png,pdf}       — latency p50/p99/p999
    fig_ablation_throughput.{png,pdf}    — throughput in/out
    fig_ablation_efficiency.{png,pdf}    — RUE, EEI, FPP
    fig_ablation_radar.{png,pdf}         — normalised radar (spider) chart

  LaTeX tables:
    table_ablation_main.tex    — main 5-metric table for the paper
    table_ablation_full.tex    — extended table with all KPIs

Usage
-----
    python3 evaluation/plot_ablation.py \
        --results-json evaluation/results/ablation_runs/ablation_.../ablation_comparison.json
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# ── IEEE style ----------------------------------------------------------------
DOUBLE_COL = 7.16
SINGLE_COL = 3.5

plt.rcParams.update({
    "font.family":        "serif",
    "font.serif":         ["Times New Roman", "Times", "DejaVu Serif"],
    "font.size":          9,
    "axes.titlesize":     10,
    "axes.labelsize":     9,
    "xtick.labelsize":    7.5,
    "ytick.labelsize":    8,
    "legend.fontsize":    7.5,
    "figure.dpi":         300,
    "savefig.dpi":        300,
    "savefig.bbox":       "tight",
    "savefig.pad_inches": 0.04,
    "axes.linewidth":     0.8,
    "grid.linewidth":     0.5,
    "lines.linewidth":    1.6,
})

# ── Variant config ------------------------------------------------------------

VARIANTS = [
    "full",
    "no_backpressure_urgency",
    "no_currency_decay",
    "no_latency_sensitivity",
    "no_priority",
    "no_auction",
]

VARIANT_LABELS = {
    "full":                    "Full StreamBazaar",
    "no_backpressure_urgency": "w/o BP Urgency",
    "no_currency_decay":       "w/o Curr. Decay",
    "no_latency_sensitivity":  "w/o Lat. Sensitivity",
    "no_priority":             "w/o Priority",
    "no_auction":              "w/o Auction",
}

VARIANT_LABELS_LATEX = {
    "full":                    r"Full \texttt{StreamBazaar}",
    "no_backpressure_urgency": r"w/o Backpressure Urgency",
    "no_currency_decay":       r"w/o Currency Decay",
    "no_latency_sensitivity":  r"w/o Latency Sensitivity",
    "no_priority":             r"w/o Priority Weighting",
    "no_auction":              r"w/o Auction (Proportional)",
}

# Colour palette: full = blue (SB house colour), ablations = greys + accent
VARIANT_COLORS = {
    "full":                    "#0072B2",
    "no_backpressure_urgency": "#E69F00",
    "no_currency_decay":       "#009E73",
    "no_latency_sensitivity":  "#D55E00",
    "no_priority":             "#CC79A7",
    "no_auction":              "#56B4E9",
}

# Hatch pattern for accessibility (printed B&W)
VARIANT_HATCH = {
    "full":                    "",
    "no_backpressure_urgency": "//",
    "no_currency_decay":       "\\\\",
    "no_latency_sensitivity":  "xx",
    "no_priority":             "..",
    "no_auction":              "++",
}

LOWER_IS_BETTER = {"latency_p50", "latency_p99", "latency_p999", "mis", "cpu_util"}


# ── Helpers -------------------------------------------------------------------

def _v(data: Dict, variant: str, metric: str) -> float:
    return float(data.get(variant, {}).get(metric, 0.0))


def _norm(data: Dict, variant: str, metric: str) -> float:
    """Return value normalised to Full StreamBazaar (full = 1.0). 0 if full is 0."""
    val  = _v(data, variant, metric)
    full = _v(data, "full", metric)
    if abs(full) < 1e-9:
        return 0.0
    return val / full


def _save(fig: plt.Figure, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(path))
    fig.savefig(str(path.with_suffix(".pdf")))
    plt.close(fig)
    print(f"  Saved: {path.name}  {path.with_suffix('.pdf').name}")


def _grid(ax: plt.Axes) -> None:
    ax.grid(axis="y", alpha=0.3, linestyle="--", zorder=0)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def _annotate_delta(ax: plt.Axes, x: float, bar_val: float,
                    full_val: float, lower_is_better: bool,
                    fontsize: float = 6.5) -> None:
    """Annotate a bar with its % degradation vs Full StreamBazaar."""
    if abs(full_val) < 1e-9:
        return
    pct = (bar_val - full_val) / abs(full_val) * 100.0
    # Degrade = goes in the wrong direction
    degrade = pct > 0 if lower_is_better else pct < 0
    color = "#CC0000" if degrade else "#007700"
    sign = "+" if pct >= 0 else ""
    ax.text(x, bar_val * 1.03, f"{sign}{pct:.1f}%",
            ha="center", va="bottom", fontsize=fontsize,
            color=color, fontweight="bold")


def _build_legend_handles(variants: List[str]) -> List[mpatches.Patch]:
    return [
        mpatches.Patch(
            facecolor=VARIANT_COLORS[v],
            hatch=VARIANT_HATCH[v],
            label=VARIANT_LABELS[v],
            edgecolor="white" if v == "full" else "black",
            linewidth=0.5,
        )
        for v in variants
    ]


# ── Individual bar chart ------------------------------------------------------

def _bar_ax(ax: plt.Axes, data: Dict, variants: List[str],
            metric: str, ylabel: str, lower_is_better: bool,
            title: str) -> None:
    x = np.arange(len(variants))
    for i, v in enumerate(variants):
        nval = _norm(data, v, metric)
        ax.bar(
            x[i], nval, width=0.65,
            color=VARIANT_COLORS[v],
            hatch=VARIANT_HATCH[v],
            edgecolor="black", linewidth=0.5,
            zorder=3,
        )
        if v != "full":
            pct = (nval - 1.0) * 100.0
            degrade = pct > 0 if lower_is_better else pct < 0
            color = "#CC0000" if degrade else "#007700"
            sign = "+" if pct >= 0 else ""
            ax.text(x[i], nval + 0.015, f"{sign}{pct:.1f}%",
                    ha="center", va="bottom", fontsize=6,
                    color=color, fontweight="bold")
    # Reference line at 1.0 (= Full StreamBazaar)
    ax.axhline(1.0, color="black", linewidth=0.8, linestyle="--", zorder=4)
    ax.set_xticks(x)
    ax.set_xticklabels([VARIANT_LABELS[v] for v in variants],
                       rotation=30, ha="right", fontsize=7)
    ax.set_ylabel(ylabel)
    direction = "↓ lower is better" if lower_is_better else "↑ higher is better"
    ax.set_title(f"{title}  ({direction})", fontsize=8.5)
    _grid(ax)


# ── 2×3 panel ----------------------------------------------------------------

def plot_ablation_panel(data: Dict, variants: List[str],
                        out_path: Path, width_in: float = DOUBLE_COL) -> None:
    panels = [
        ("throughput_out", "Norm. Throughput", False, "Throughput Out"),
        ("latency_p99",    "Norm. Latency p99", True,  "Tail Latency p99"),
        ("rue",            "Norm. RUE",         False, "Resource Util. Eff."),
        ("eei",            "Norm. EEI",         False, "Economic Eff. Index"),
        ("mis",            "Norm. MIS",         True,  "Migration Impact"),
        ("cpu_util",       "Norm. CPU",         True,  "CPU Utilisation"),
    ]
    fig, axes = plt.subplots(2, 3, figsize=(width_in, width_in * 0.75))
    for ax, (metric, ylabel, lib, title) in zip(axes.flatten(), panels):
        _bar_ax(ax, data, variants, metric, ylabel, lib, title)

    handles = _build_legend_handles(variants)
    fig.legend(handles=handles, loc="lower center", ncol=3,
               fontsize=7, framealpha=0.85, edgecolor="gray",
               bbox_to_anchor=(0.5, -0.04))
    fig.suptitle("StreamBazaar Ablation Study", fontsize=10, y=1.01)
    fig.tight_layout(pad=0.6, rect=[0, 0.06, 1, 1])
    _save(fig, out_path)


# ── Latency detail ------------------------------------------------------------

def plot_latency(data: Dict, variants: List[str],
                 out_path: Path, width_in: float = DOUBLE_COL) -> None:
    percentiles = [
        ("latency_p50",  "p50"),
        ("latency_p99",  "p99"),
        ("latency_p999", "p999"),
    ]
    x = np.arange(len(variants))
    bar_w = 0.25
    fig, ax = plt.subplots(figsize=(width_in, width_in * 0.45))
    offsets = [-bar_w, 0, bar_w]
    for (metric, plabel), offset in zip(percentiles, offsets):
        vals = [_norm(data, v, metric) for v in variants]
        ax.bar(x + offset, vals, bar_w * 0.9,
               label=f"Latency {plabel}",
               edgecolor="black", linewidth=0.4, zorder=3)
    ax.axhline(1.0, color="black", linewidth=0.8, linestyle="--", zorder=4)
    ax.set_xticks(x)
    ax.set_xticklabels([VARIANT_LABELS[v] for v in variants],
                       rotation=30, ha="right", fontsize=7)
    ax.set_ylabel("Normalised Latency  (Full = 1.0)")
    ax.set_title("End-to-End Latency by Percentile  (↓ lower is better)", pad=5)
    ax.legend(fontsize=7.5, framealpha=0.85, edgecolor="gray")
    _grid(ax)
    fig.tight_layout(pad=0.5)
    _save(fig, out_path)


# ── Throughput detail ---------------------------------------------------------

def plot_throughput(data: Dict, variants: List[str],
                    out_path: Path, width_in: float = DOUBLE_COL) -> None:
    x = np.arange(len(variants))
    bar_w = 0.35
    fig, ax = plt.subplots(figsize=(width_in, width_in * 0.42))
    in_vals  = [_norm(data, v, "throughput_in")  for v in variants]
    out_vals = [_norm(data, v, "throughput_out") for v in variants]
    ax.bar(x - bar_w / 2, in_vals,  bar_w * 0.9, label="Ingress",
           edgecolor="black", linewidth=0.4, zorder=3)
    ax.bar(x + bar_w / 2, out_vals, bar_w * 0.9, label="Egress (goodput)",
           edgecolor="black", linewidth=0.4, zorder=3)
    ax.axhline(1.0, color="black", linewidth=0.8, linestyle="--", zorder=4)
    ax.set_xticks(x)
    ax.set_xticklabels([VARIANT_LABELS[v] for v in variants],
                       rotation=30, ha="right", fontsize=7)
    ax.set_ylabel("Normalised Throughput  (Full = 1.0)")
    ax.set_title("Throughput: Ingress vs. Egress Goodput  (↑ higher is better)", pad=5)
    ax.legend(fontsize=7.5, framealpha=0.85, edgecolor="gray")
    _grid(ax)
    fig.tight_layout(pad=0.5)
    _save(fig, out_path)


# ── Efficiency metrics --------------------------------------------------------

def plot_efficiency(data: Dict, variants: List[str],
                    out_path: Path, width_in: float = DOUBLE_COL) -> None:
    x = np.arange(len(variants))
    bar_w = 0.25
    metrics = [("rue", "RUE"), ("eei", "EEI"), ("fpp", "FPP")]
    fig, ax = plt.subplots(figsize=(width_in, width_in * 0.42))
    offsets = [-bar_w, 0, bar_w]
    for (metric, label), offset in zip(metrics, offsets):
        vals = [_norm(data, v, metric) for v in variants]
        ax.bar(x + offset, vals, bar_w * 0.9, label=label,
               edgecolor="black", linewidth=0.4, zorder=3)
    ax.axhline(1.0, color="black", linewidth=0.8, linestyle="--", zorder=4)
    ax.set_xticks(x)
    ax.set_xticklabels([VARIANT_LABELS[v] for v in variants],
                       rotation=30, ha="right", fontsize=7)
    ax.set_ylabel("Normalised Score  (Full = 1.0)")
    ax.set_title("Efficiency Metrics: RUE, EEI, FPP  (↑ higher is better)", pad=5)
    ax.legend(fontsize=7.5, framealpha=0.85, edgecolor="gray")
    _grid(ax)
    fig.tight_layout(pad=0.5)
    _save(fig, out_path)


# ── Radar / spider chart ------------------------------------------------------

def plot_radar(data: Dict, variants: List[str],
               out_path: Path, width_in: float = SINGLE_COL * 2.0) -> None:
    """Normalised radar chart — each axis is [0,1] relative to best variant."""
    radar_metrics = [
        ("throughput_out", False, "Throughput"),
        ("latency_p99",    True,  "Latency p99"),
        ("rue",            False, "RUE"),
        ("eei",            False, "EEI"),
        ("fpp",            False, "FPP"),
        ("mis",            True,  "MIS"),
    ]
    labels = [r[2] for r in radar_metrics]
    N = len(labels)
    angles = np.linspace(0, 2 * np.pi, N, endpoint=False).tolist()
    angles += angles[:1]  # close polygon

    # Normalise each metric to [0,1] where 1 = best across variants
    def normalise(metric: str, lower_is_better: bool) -> Dict[str, float]:
        vals = {v: _v(data, v, metric) for v in variants}
        lo = min(vals.values())
        hi = max(vals.values())
        rng = max(hi - lo, 1e-9)
        if lower_is_better:
            return {v: (hi - val) / rng for v, val in vals.items()}
        else:
            return {v: (val - lo) / rng for v, val in vals.items()}

    norm: List[Dict[str, float]] = [normalise(m, lib) for m, lib, _ in radar_metrics]

    fig, ax = plt.subplots(figsize=(width_in, width_in),
                           subplot_kw=dict(polar=True))
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_yticks([0.25, 0.5, 0.75, 1.0])
    ax.set_yticklabels(["0.25", "0.5", "0.75", "1.0"], fontsize=6)
    ax.set_ylim(0, 1)

    for v in variants:
        vals_v = [norm[i][v] for i in range(N)] + [norm[0][v]]
        ax.plot(angles, vals_v, color=VARIANT_COLORS[v], lw=1.6,
                label=VARIANT_LABELS[v])
        ax.fill(angles, vals_v, color=VARIANT_COLORS[v], alpha=0.08)

    ax.legend(loc="lower right", bbox_to_anchor=(1.35, -0.05),
              fontsize=7, framealpha=0.85, edgecolor="gray")
    ax.set_title("Normalised Ablation Radar\n(1.0 = best per axis)", pad=14, fontsize=9)
    fig.tight_layout(pad=0.5)
    _save(fig, out_path)


# ── LaTeX tables --------------------------------------------------------------

def _latex_table_main(data: Dict, variants: List[str]) -> str:
    """5-metric ablation table, all values normalised to Full StreamBazaar = 1.000."""
    metrics = [
        ("throughput_out", "Throughput~$\\uparrow$", False),
        ("latency_p99",    "Lat.~p99~$\\downarrow$", True),
        ("rue",            "RUE~$\\uparrow$",         False),
        ("eei",            "EEI~$\\uparrow$",         False),
        ("mis",            "MIS~$\\downarrow$",       True),
    ]
    col_fmt = "l" + "r" * len(metrics)
    lines: List[str] = []
    lines.append(r"\begin{table}[t]")
    lines.append(r"  \centering")
    lines.append(r"  \caption{Ablation study results (4 nodes, 16 tenants). "
                 r"All values normalised to full \texttt{StreamBazaar} $= 1.000$. "
                 r"\textbf{Bold} = closest to Full. "
                 r"\textcolor{red}{Red} = degraded, \textcolor{teal}{teal} = improved.}")
    lines.append(r"  \label{tab:ablation}")
    lines.append(r"  \setlength{\tabcolsep}{4pt}")
    lines.append(r"  \begin{tabular}{" + col_fmt + "}")
    lines.append(r"    \toprule")

    # Header row — metric name already contains arrow
    hdr = "    Variant"
    for _, label, _ in metrics:
        hdr += f" & {label}"
    hdr += r" \\"
    lines.append(hdr)
    lines.append(r"    \midrule")

    for v in variants:
        row = f"    {VARIANT_LABELS_LATEX[v]}"
        for metric, _, lower_is_better in metrics:
            nval = _norm(data, v, metric)
            fmt = f"{nval:.3f}"
            if v == "full":
                fmt = r"\textbf{1.000}"
            else:
                pct = (nval - 1.0) * 100.0
                degrade = pct > 0 if lower_is_better else pct < 0
                sign = "+" if pct >= 0 else ""
                color_cmd = r"\textcolor{red}" if degrade else r"\textcolor{teal}"
                delta_str = color_cmd + r"{" + f"{sign}{pct:.1f}\\%" + "}"
                fmt += r"~\scriptsize{" + delta_str + "}"
            row += f" & {fmt}"
        row += r" \\"
        if v == "full":
            row = row.replace(r"\\", r"\\ \midrule", 1)
        lines.append(row)

    lines.append(r"    \bottomrule")
    lines.append(r"  \end{tabular}")
    lines.append(r"  \vspace{-4pt}")
    lines.append(r"\end{table}")
    return "\n".join(lines)


def _latex_table_full(data: Dict, variants: List[str]) -> str:
    """Extended table with all 9 KPIs normalised to Full = 1.000 — for appendix."""
    metrics = [
        ("throughput_out", "Tput Out~$\\uparrow$",  False),
        ("throughput_in",  "Tput In~$\\uparrow$",   False),
        ("latency_p50",    "Lat p50~$\\downarrow$",  True),
        ("latency_p99",    "Lat p99~$\\downarrow$",  True),
        ("latency_p999",   "Lat p999~$\\downarrow$", True),
        ("rue",            "RUE~$\\uparrow$",        False),
        ("eei",            "EEI~$\\uparrow$",        False),
        ("mis",            "MIS~$\\downarrow$",      True),
        ("cpu_util",       "CPU~$\\downarrow$",      True),
    ]
    col_fmt = "l" + "r" * len(metrics)
    lines: List[str] = []
    lines.append(r"\begin{table*}[t]")
    lines.append(r"  \centering")
    lines.append(r"  \caption{Full ablation results (all KPIs), "
                 r"normalised to full \texttt{StreamBazaar} $= 1.000$.}")
    lines.append(r"  \label{tab:ablation_full}")
    lines.append(r"  \setlength{\tabcolsep}{3pt}")
    lines.append(r"  \begin{tabular}{" + col_fmt + "}")
    lines.append(r"    \toprule")

    hdr = "    Variant"
    for _, label, _ in metrics:
        hdr += f" & {label}"
    hdr += r" \\"
    lines.append(hdr)
    lines.append(r"    \midrule")

    for v in variants:
        row = f"    {VARIANT_LABELS_LATEX[v]}"
        for metric, _, lower_is_better in metrics:
            nval = _norm(data, v, metric)
            if v == "full":
                fmt = r"\textbf{1.000}"
            else:
                pct = (nval - 1.0) * 100.0
                degrade = pct > 0 if lower_is_better else pct < 0
                sign = "+" if pct >= 0 else ""
                color_cmd = r"\textcolor{red}" if degrade else r"\textcolor{teal}"
                delta_str = color_cmd + r"{" + f"{sign}{pct:.1f}\\%" + "}"
                fmt = f"{nval:.3f}~\\scriptsize{{{delta_str}}}"
            row += f" & {fmt}"
        row += r" \\"
        if v == "full":
            row = row.replace(r"\\", r"\\ \midrule", 1)
        lines.append(row)

    lines.append(r"    \bottomrule")
    lines.append(r"  \end{tabular}")
    lines.append(r"\end{table*}")
    return "\n".join(lines)


# ── CMD summary ---------------------------------------------------------------

def print_summary(data: Dict, variants: List[str]) -> None:
    metrics = [
        ("throughput_out", "Throughput (msgs/s)", False),
        ("latency_p99",    "Latency p99 (ms)",    True),
        ("rue",            "RUE",                  False),
        ("eei",            "EEI",                  False),
        ("fpp",            "FPP",                  False),
        ("mis",            "MIS",                  True),
        ("cpu_util",       "CPU %",                True),
    ]
    print("\n" + "═" * 76)
    print("  ABLATION STUDY RESULTS")
    print("═" * 76)
    full_kpis = data.get("full", {})
    for metric, label, lib in metrics:
        direction = "↓ lower is better" if lib else "↑ higher is better"
        print(f"\n── {label}  ({direction}) ──")
        for v in variants:
            val  = _v(data, v, metric)
            fval = float(full_kpis.get(metric, 0.0))
            if v == "full" or abs(fval) < 1e-9:
                delta = ""
            else:
                pct = (val - fval) / abs(fval) * 100.0
                degrade = pct > 0 if lib else pct < 0
                tag = "▲ worse" if degrade else "▼ better"
                delta = f"   {pct:+.1f}%  {tag}"
            marker = " ◀ baseline" if v == "full" else ""
            print(f"  {VARIANT_LABELS[v]:<28}  {val:>10.2f}{delta}{marker}")


# ── Entry point ---------------------------------------------------------------

def find_latest(results_root: Path) -> Optional[Path]:
    for s in sorted(results_root.glob("ablation_*"), reverse=True):
        p = s / "ablation_comparison.json"
        if p.exists():
            return p
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot StreamBazaar ablation study")
    parser.add_argument("--results-json",  default=None)
    parser.add_argument("--results-root",  default="evaluation/results/ablation_runs")
    parser.add_argument("--fig-dir",       default=None)
    parser.add_argument("--width",         type=float, default=DOUBLE_COL)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]

    if args.results_json:
        json_path = (Path(args.results_json) if Path(args.results_json).is_absolute()
                     else root / args.results_json)
    else:
        json_path = find_latest(root / args.results_root)
        if json_path is None:
            print("No ablation_comparison.json found.")
            print("Run: python3 evaluation/run_ablation_experiment.py")
            return

    print(f"Loading: {json_path}")
    data = json.loads(json_path.read_text(encoding="utf-8"))

    # Keep only variants present in the JSON
    variants = [v for v in VARIANTS if v in data]
    print(f"Variants: {variants}\n")

    fig_dir = (Path(args.fig_dir) if args.fig_dir
               else json_path.parent / "ablation_figures")
    fig_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output: {fig_dir}\n")

    w = args.width

    # ── Figures ──
    plot_ablation_panel(data, variants,
                        fig_dir / "fig_ablation_bar_panel.png", w)
    plot_latency(data, variants,
                 fig_dir / "fig_ablation_latency.png", w)
    plot_throughput(data, variants,
                    fig_dir / "fig_ablation_throughput.png", w)
    plot_efficiency(data, variants,
                    fig_dir / "fig_ablation_efficiency.png", w)
    plot_radar(data, variants,
               fig_dir / "fig_ablation_radar.png")

    # ── LaTeX tables ──
    tex_main = _latex_table_main(data, variants)
    tex_full = _latex_table_full(data, variants)
    (fig_dir / "table_ablation_main.tex").write_text(tex_main, encoding="utf-8")
    (fig_dir / "table_ablation_full.tex").write_text(tex_full, encoding="utf-8")
    print(f"  Saved: table_ablation_main.tex")
    print(f"  Saved: table_ablation_full.tex")

    # ── CMD summary ──
    print_summary(data, variants)
    print(f"\nAll outputs saved to: {fig_dir}")


if __name__ == "__main__":
    main()
