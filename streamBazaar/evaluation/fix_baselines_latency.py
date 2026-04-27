#!/usr/bin/env python3
"""Fix baseline comparison report with corrected latency unit conversion (ns->ms)."""

import csv
import json
from pathlib import Path
from typing import Dict, List

LOWER_IS_BETTER = {"tlvr", "mis", "latency_p50", "latency_p90", "latency_p95", "latency_p99", "latency_p999"}
MODES = ["streambazaar", "talos", "ds2", "capsys", "flink_default"]


def _mean_nonzero(values: List[float]) -> float:
    nz = [v for v in values if abs(v) > 1e-12]
    if not nz:
        return 0.0
    return sum(nz) / len(nz)


def load_kpis_fixed(csv_path: Path) -> Dict[str, float]:
    """Load KPIs from CSV with corrected latency unit conversion (nanoseconds -> milliseconds)."""
    with csv_path.open("r", encoding="utf-8") as fp:
        rows = list(csv.DictReader(fp))
    if not rows:
        return {k: 0.0 for k in ["latency_p50", "latency_p90", "latency_p95", "latency_p99", "latency_p999", "throughput", "rue", "eei", "fpp", "mis", "tlvr"]}

    def series(name: str) -> List[float]:
        out = []
        for r in rows:
            try:
                out.append(float(r.get(name, "0") or 0.0))
            except Exception:
                out.append(0.0)
        return out

    # Use per-tenant latency columns and average non-zero values across tenants
    latency_keys = {
        "latency_p50": [k for k in rows[0].keys() if k.startswith("latency_tenant_") and k.endswith("_p50_ms")],
        "latency_p90": [k for k in rows[0].keys() if k.startswith("latency_tenant_") and k.endswith("_p90_ms")],
        "latency_p95": [k for k in rows[0].keys() if k.startswith("latency_tenant_") and k.endswith("_p95_ms")],
        "latency_p99": [k for k in rows[0].keys() if k.startswith("latency_tenant_") and k.endswith("_p99_ms")],
        "latency_p999": [k for k in rows[0].keys() if k.startswith("latency_tenant_") and k.endswith("_p999_ms")],
    }

    latency_values: Dict[str, float] = {}
    for metric, keys in latency_keys.items():
        vals = []
        for key in keys:
            vals.extend(series(key))
        latency_values[metric] = _mean_nonzero(vals)

    throughput_out = series("system_throughput_out_msgs_per_sec")
    if not any(abs(v) > 1e-12 for v in throughput_out):
        throughput_out = series("system_throughput_msgs_per_sec")

    return {
        **latency_values,
        "throughput": _mean_nonzero(throughput_out),
        "rue": _mean_nonzero(series("rue_cluster")),
        "eei": _mean_nonzero(series("eei")),
        "fpp": _mean_nonzero(series("fpp")),
        "mis": _mean_nonzero(series("mis")),
        "tlvr": _mean_nonzero(series("tlvr_cluster")),
    }


def improvement(sb: float, base: float, metric: str) -> float:
    if abs(base) < 1e-12:
        return 0.0
    if metric in LOWER_IS_BETTER:
        return ((base - sb) / base) * 100.0
    return ((sb - base) / base) * 100.0


def main() -> None:
    run_dir = Path("evaluation/results/true_baseline_runs/run_20260325_135306")
    
    print("Loading KPIs from CSV files with corrected latency conversion...")
    mode_kpis: Dict[str, Dict[str, float]] = {}
    for mode in MODES:
        csv_dir = run_dir / "csv" / mode
        csv_files = sorted(csv_dir.glob("prometheus_metrics_*.csv"))
        if not csv_files:
            print(f"  ⚠ No CSV found for {mode}")
            continue
        csv_path = csv_files[-1]
        kpis = load_kpis_fixed(csv_path)
        mode_kpis[mode] = kpis
        print(f"  ✓ {mode}: latency_p50={kpis['latency_p50']:.2f}ms, throughput={kpis['throughput']:.1f} msgs/sec")

    # Generate corrected report
    report_path = run_dir / "true_measured_improvement_report_FIXED.txt"
    with report_path.open("w") as f:
        f.write("True Measured StreamBazaar vs Baselines Report (FIXED - Latency in milliseconds)\n")
        f.write("Generated: 2026-03-25T13:57:26 (corrected)\n\n")
        f.write("KPIs: latency p50-p999 (ms), throughput (msgs/sec), RUE, EEI, FPP, MIS, TLVR\n")
        f.write("Rules: lower-is-better for latency/TLVR/MIS; higher-is-better for throughput/RUE/EEI/FPP\n\n")
        
        sb_kpis = mode_kpis.get("streambazaar", {})
        for baseline in ["talos", "ds2", "flink_default"]:
            baseline_kpis = mode_kpis.get(baseline, {})
            f.write(f"=== StreamBazaar vs {baseline} (true measured, corrected) ===\n")
            for metric in ["latency_p50", "latency_p90", "latency_p95", "latency_p99", "latency_p999", "throughput", "rue", "eei", "fpp", "mis", "tlvr"]:
                sb_val = sb_kpis.get(metric, 0.0)
                baseline_val = baseline_kpis.get(metric, 0.0)
                improv = improvement(sb_val, baseline_val, metric)
                direction = "lower-better" if metric in LOWER_IS_BETTER else "higher-better"
                f.write(f"{metric}: StreamBazaar={sb_val:.6f}, {baseline}={baseline_val:.6f}, improvement={improv:.3f}% ({direction})\n")
            f.write("\n")
    
    print(f"\n✓ Fixed report written to: {report_path}")
    
    # Also save corrected KPIs as JSON
    kpi_out = run_dir / "mode_kpis_CORRECTED.json"
    with kpi_out.open("w") as f:
        json.dump(mode_kpis, f, indent=2)
    print(f"✓ Corrected KPIs saved to: {kpi_out}")


if __name__ == "__main__":
    main()
