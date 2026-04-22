#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Mapping


LOWER_IS_BETTER = {"tlvr", "mis"}


def load_results(path: Path) -> Dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_per_scheduler_metrics(out_dir: Path, metrics_by_scheduler: Mapping[str, Mapping[str, float]]) -> None:
    per_dir = out_dir / "per_scheduler"
    per_dir.mkdir(parents=True, exist_ok=True)

    for scheduler, metrics in metrics_by_scheduler.items():
        safe = scheduler.lower().replace(" ", "_")

        json_path = per_dir / f"{safe}_all_metrics.json"
        json_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

        csv_path = per_dir / f"{safe}_all_metrics.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as fp:
            writer = csv.writer(fp)
            writer.writerow(["metric", "value"])
            for key in sorted(metrics.keys()):
                writer.writerow([key, metrics[key]])


def avg_latency(metrics: Mapping[str, float], percentile: str) -> float:
    suffix = f"_{percentile}_ms"
    vals: List[float] = []
    for key, value in metrics.items():
        if key.startswith("latency_tenant_") and key.endswith(suffix):
            vals.append(float(value))
    if not vals:
        return 0.0
    return sum(vals) / len(vals)


def kpi_vector(metrics: Mapping[str, float]) -> Dict[str, float]:
    return {
        "latency_p50_ms": avg_latency(metrics, "p50"),
        "latency_p90_ms": avg_latency(metrics, "p90"),
        "latency_p95_ms": avg_latency(metrics, "p95"),
        "latency_p99_ms": avg_latency(metrics, "p99"),
        "latency_p999_ms": avg_latency(metrics, "p999"),
        "throughput_msgs_per_sec": float(metrics.get("system_throughput_msgs_per_sec", 0.0)),
        "rue": float(metrics.get("rue_cluster", 0.0)),
        "eei": float(metrics.get("eei", 0.0)),
        "fpp": float(metrics.get("fpp", 0.0)),
        "mis": float(metrics.get("mis", 0.0)),
        "tlvr": float(metrics.get("tlvr_cluster", 0.0)),
    }


def pct_improvement(streambazaar_val: float, baseline_val: float, metric: str) -> float:
    if abs(baseline_val) < 1e-9:
        return 0.0
    if metric in LOWER_IS_BETTER or metric.startswith("latency_"):
        # Positive means StreamBazaar lower (better) than baseline.
        return ((baseline_val - streambazaar_val) / baseline_val) * 100.0
    return ((streambazaar_val - baseline_val) / baseline_val) * 100.0


def save_comparison_tables(out_dir: Path, metrics_by_scheduler: Mapping[str, Mapping[str, float]]) -> Dict[str, Dict[str, float]]:
    vectors = {name: kpi_vector(metrics) for name, metrics in metrics_by_scheduler.items()}

    table_path = out_dir / "kpi_comparison_table.csv"
    metric_names = [
        "latency_p50_ms",
        "latency_p90_ms",
        "latency_p95_ms",
        "latency_p99_ms",
        "latency_p999_ms",
        "throughput_msgs_per_sec",
        "rue", 
        "eei",
        "fpp",
        "mis",
        "tlvr",
    ]

    schedulers = ["StreamBazaar", "TALOS", "DS2", "CAPSys", "FlinkDefault"]
    schedulers = [s for s in schedulers if s in vectors]

    

    with table_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp)
        writer.writerow(["metric"] + schedulers)
        for metric in metric_names:
            row = [metric]
            for scheduler in schedulers:
                row.append(vectors[scheduler].get(metric, 0.0))
            writer.writerow(row)

    return vectors


def save_improvement_report(out_dir: Path, vectors: Mapping[str, Mapping[str, float]]) -> None:
    sb = vectors.get("StreamBazaar")
    if not sb:
        raise RuntimeError("StreamBazaar vector missing in comparison output")

    baselines = [name for name in ("TALOS", "DS2", "CAPSys", "FlinkDefault") if name in vectors]
    metric_names = [
        "latency_p50_ms",
        "latency_p90_ms",
        "latency_p95_ms",
        "latency_p99_ms",
        "latency_p999_ms",
        "throughput_msgs_per_sec",
        "rue",
        "eei",
        "fpp",
        "mis",
        "tlvr",
    ]

    lines: List[str] = []
    lines.append("StreamBazaar Improvement Report vs Individual Baselines")
    lines.append(f"Generated: {datetime.now().isoformat()}")
    lines.append("")
    lines.append("Interpretation:")
    lines.append("- Positive (%) means StreamBazaar is better.")
    lines.append("- For TLVR and MIS, lower values are better (explicitly handled).")
    lines.append("")

    for baseline in baselines:
        lines.append(f"=== StreamBazaar vs {baseline} ===")
        base = vectors[baseline]
        for metric in metric_names:
            imp = pct_improvement(float(sb.get(metric, 0.0)), float(base.get(metric, 0.0)), metric)
            direction = "lower-better" if (metric in LOWER_IS_BETTER or metric.startswith("latency_")) else "higher-better"
            lines.append(
                f"{metric}: StreamBazaar={sb.get(metric, 0.0):.6f}, {baseline}={base.get(metric, 0.0):.6f}, "
                f"improvement={imp:.3f}% ({direction})"
            )
        lines.append("")

    report_path = out_dir / "streambazaar_improvement_report.txt"
    report_path.write_text("\n".join(lines), encoding="utf-8")



def main() -> None:
    parser = argparse.ArgumentParser(description="Save individual baseline metric outputs and improvement report")
    parser.add_argument(
        "--input",
        default="evaluation/baseline_comparison_results.json",
        help="Path to baseline comparison result JSON",
    )
    parser.add_argument(
        "--out-dir",
        default="evaluation/results/baseline_outputs",
        help="Output directory for baseline metric artifacts",
    )
    args = parser.parse_args()

    results = load_results(Path(args.input))
    metrics_by_scheduler = results.get("metrics_by_scheduler", {})
    if not metrics_by_scheduler:
        raise RuntimeError("No metrics_by_scheduler found in baseline comparison results")

    ts = datetime.now().strftime("run_%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir) / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    save_per_scheduler_metrics(out_dir, metrics_by_scheduler)
    vectors = save_comparison_tables(out_dir, metrics_by_scheduler)
    save_improvement_report(out_dir, vectors)

    summary = {
        "input": str(Path(args.input).resolve()),
        "output_dir": str(out_dir.resolve()),
        "schedulers": sorted(metrics_by_scheduler.keys()),
        "metric_count_per_scheduler": {k: len(v) for k, v in metrics_by_scheduler.items()},
        "artifacts": {
            "per_scheduler_dir": str((out_dir / "per_scheduler").resolve()),
            "kpi_comparison_table": str((out_dir / "kpi_comparison_table.csv").resolve()),
            "improvement_report": str((out_dir / "streambazaar_improvement_report.txt").resolve()),
        },
    }

    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"[baseline-output] wrote: {out_dir}")
    print(f"[baseline-output] improvement report: {out_dir / 'streambazaar_improvement_report.txt'}")


if __name__ == "__main__":
    main()
