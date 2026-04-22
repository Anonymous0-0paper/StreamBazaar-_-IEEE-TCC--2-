#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from statistics import mean, stdev
from typing import Dict, List


def ci95(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    return 1.96 * stdev(values) / (len(values) ** 0.5)


def collect_metric(reports: List[Dict], tenant: str, section: str, key: str) -> List[float]:
    vals = []
    for rep in reports:
        v = rep.get("tenants", {}).get(tenant, {}).get(section, {}).get(key)
        if isinstance(v, (int, float)):
            vals.append(float(v))
    return vals


def collect_global_metric(reports: List[Dict], section: str, key: str) -> List[float]:
    vals = []
    for rep in reports:
        v = rep.get(section, {}).get(key)
        if isinstance(v, (int, float)):
            vals.append(float(v))
    return vals


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate evaluation report JSON files")
    parser.add_argument("--reports-dir", default="evaluation/results/raw")
    parser.add_argument("--out", default="evaluation/results/summary.json")
    args = parser.parse_args()

    reports_dir = Path(args.reports_dir)
    report_files = sorted(reports_dir.glob("run_*_evaluation_report_*.json"))
    if not report_files:
        raise RuntimeError(f"No run_*_evaluation_report_*.json files in {reports_dir}")

    reports = [json.loads(p.read_text(encoding="utf-8")) for p in report_files]
    tenants = ["tenant-fraud", "tenant-clickstream", "tenant-ml"]

    summary = {
        "runs": len(reports),
        "files": [str(p) for p in report_files],
        "tenants": {},
        "advanced_kpis": {},
    }

    metric_plan = {
        "latency": ["p50", "p90", "p95", "p99", "mean", "max"],
        "throughput": ["avg_throughput", "max_throughput", "min_throughput", "throughput_variance"],
        "resource_usage": ["cpu_percent", "memory_percent", "cpu_peak", "memory_peak"],
    }

    for tenant in tenants:
        tenant_summary: Dict[str, Dict[str, float]] = {}
        for section, keys in metric_plan.items():
            sec_summary: Dict[str, Dict[str, float]] = {}
            for key in keys:
                values = collect_metric(reports, tenant, section, key)
                if values:
                    sec_summary[key] = {
                        "mean": mean(values),
                        "std": stdev(values) if len(values) > 1 else 0.0,
                        "ci95": ci95(values),
                        "n": len(values),
                    }
            tenant_summary[section] = sec_summary
        summary["tenants"][tenant] = tenant_summary

    for key in [
        "resource_utilization_efficiency",
        "tail_latency_violation_rate",
        "economic_efficiency_index",
        "fairness_performance_product",
        "migration_impact_score",
    ]:
        values = collect_global_metric(reports, "advanced_kpis", key)
        if values:
            summary["advanced_kpis"][key] = {
                "mean": mean(values),
                "std": stdev(values) if len(values) > 1 else 0.0,
                "ci95": ci95(values),
                "n": len(values),
            }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote summary: {out_path}")


if __name__ == "__main__":
    main()
