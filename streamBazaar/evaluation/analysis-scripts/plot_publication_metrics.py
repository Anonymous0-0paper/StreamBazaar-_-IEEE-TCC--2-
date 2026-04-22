#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt


def read_csv_rows(csv_path: Path) -> List[Dict[str, float | str]]:
    rows: List[Dict[str, float | str]] = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            parsed: Dict[str, float | str] = {}
            for k, v in row.items():
                if k in ("timestamp_iso",):
                    parsed[k] = v
                else:
                    try:
                        parsed[k] = float(v) if v is not None and v != "" else 0.0
                    except Exception:
                        parsed[k] = v or ""
            rows.append(parsed)
    return rows


def series(rows: List[Dict[str, float | str]], key: str) -> List[float]:
    vals: List[float] = []
    for r in rows:
        v = r.get(key, 0.0)
        vals.append(float(v) if isinstance(v, (int, float)) else 0.0)
    return vals


def timestamps(rows: List[Dict[str, float | str]]) -> List[float]:
    return [float(r.get("timestamp_epoch", 0.0)) for r in rows]


def line_plot(x: List[float], ys: Dict[str, List[float]], title: str, ylabel: str, out_path: Path) -> None:
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(11, 4.8))
    for label, values in ys.items():
        ax.plot(x, values, linewidth=2.0, label=label)
    ax.set_title(title, fontsize=13)
    ax.set_xlabel("Unix Time (s)")
    ax.set_ylabel(ylabel)
    ax.legend(loc="best", fontsize=9)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=220)
    plt.close(fig)
    print(f"Wrote figure: {out_path}")


def box_plot(data: Dict[str, List[float]], title: str, ylabel: str, out_path: Path) -> None:
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(10, 4.8))
    labels = list(data.keys())
    values = [data[k] for k in labels]
    ax.boxplot(values, tick_labels=labels, showfliers=False)
    ax.set_title(title, fontsize=13)
    ax.set_ylabel(ylabel)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=220)
    plt.close(fig)
    print(f"Wrote figure: {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate publication-ready time-series and box plots from Prometheus CSV exports")
    parser.add_argument("--csv", required=True, help="CSV produced by evaluation/export_prometheus_csv.py")
    parser.add_argument("--fig-dir", default="evaluation/results/figures_publication")
    parser.add_argument("--tenants", default="tenant-fraud,tenant-clickstream,tenant-ml")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    fig_dir = Path(args.fig_dir)
    tenants = [t.strip() for t in args.tenants.split(",") if t.strip()]

    rows = read_csv_rows(csv_path)
    if not rows:
        raise RuntimeError(f"No data rows in {csv_path}")

    x = timestamps(rows)

    # Time-series KPI dashboard plots
    line_plot(
        x,
        {
            "RUE": series(rows, "rue_cluster"),
            "TLVR": series(rows, "tlvr_cluster"),
            "EEI": series(rows, "eei"),
            "FPP": series(rows, "fpp"),
            "MIS": series(rows, "mis"),
        },
        "Core KPI Time Series",
        "Value",
        fig_dir / "kpi_timeseries.png",
    )

    line_plot(
        x,
        {
            "Throughput In (msg/s)": series(rows, "msg_in_rate_total"),
            "Throughput Out (msg/s)": series(rows, "msg_out_rate_total"),
            "Bytes In (B/s)": series(rows, "bytes_in_rate_total"),
            "Bytes Out (B/s)": series(rows, "bytes_out_rate_total"),
        },
        "Traffic Time Series",
        "Rate",
        fig_dir / "traffic_timeseries.png",
    )

    line_plot(
        x,
        {
            "Checkpoint CPU": series(rows, "checkpoint_cpu_cluster"),
            "Checkpoint Memory": series(rows, "checkpoint_memory_cluster"),
            "Checkpoint Network": series(rows, "checkpoint_network_cluster"),
        },
        "Checkpoint Utilization Time Series",
        "Utilization (%)",
        fig_dir / "checkpoint_util_timeseries.png",
    )

    # Tenant latency time-series and boxplots
    latency_keys = ["p50", "p90", "p95", "p99", "p999"]
    for tenant in tenants:
        safe = tenant.replace("-", "_")
        ys = {
            "p50": series(rows, f"latency_{safe}_p50_ms"),
            "p90": series(rows, f"latency_{safe}_p90_ms"),
            "p95": series(rows, f"latency_{safe}_p95_ms"),
            "p99": series(rows, f"latency_{safe}_p99_ms"),
            "p99.9": series(rows, f"latency_{safe}_p999_ms"),
        }
        line_plot(
            x,
            ys,
            f"Latency Percentiles Over Time ({tenant})",
            "Latency (ms)",
            fig_dir / f"latency_timeseries_{safe}.png",
        )

        box_data = {
            k: series(rows, f"latency_{safe}_{k}_ms") for k in latency_keys
        }
        box_plot(
            box_data,
            f"Latency Distribution by Percentile ({tenant})",
            "Latency (ms)",
            fig_dir / f"latency_boxplot_{safe}.png",
        )

    # Throughput boxplot across tenants
    tp_box = {}
    for tenant in tenants:
        safe = tenant.replace("-", "_")
        tp_box[tenant] = series(rows, f"throughput_{safe}_total")
    box_plot(tp_box, "Throughput Distribution by Tenant", "Messages/sec", fig_dir / "throughput_boxplot_tenants.png")

    # Migration overhead plots
    mig_transfer = {}
    mig_downtime = {}
    for tenant in tenants:
        safe = tenant.replace("-", "_")
        mig_transfer[tenant] = series(rows, f"migration_{safe}_transfer_sec")
        mig_downtime[tenant] = series(rows, f"migration_{safe}_downtime_sec")
    box_plot(mig_transfer, "Migration Transfer Time Distribution", "Seconds", fig_dir / "migration_transfer_boxplot.png")
    box_plot(mig_downtime, "Migration Downtime Distribution", "Seconds", fig_dir / "migration_downtime_boxplot.png")


if __name__ == "__main__":
    main()
