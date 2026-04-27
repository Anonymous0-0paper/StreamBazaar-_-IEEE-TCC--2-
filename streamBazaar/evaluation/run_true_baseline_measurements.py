#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List


MODES = ["streambazaar", "talos", "ds2", "capsys", "flink_default"]
LOWER_IS_BETTER = {"tlvr", "mis", "latency_p50", "latency_p90", "latency_p95", "latency_p99", "latency_p999", "backlog_slope_per_sec"}


def run(cmd: List[str], cwd: Path, env: Dict[str, str] | None = None, check: bool = True) -> subprocess.CompletedProcess:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.run(cmd, cwd=cwd, env=merged_env, check=check, text=True)


def wait_for_stream_coordinator_mode(mode: str, timeout: int = 120) -> None:
    start = time.time()
    while True:
        try:
            out = subprocess.check_output(["curl", "-fsS", "http://localhost:18085/health"], text=True)
            payload = json.loads(out)
            if str(payload.get("scheduler_mode", "")).lower() == mode:
                return
        except Exception:
            pass
        if time.time() - start > timeout:
            raise TimeoutError(f"Timed out waiting for stream-coordinator scheduler_mode={mode}")
        time.sleep(2)


def latest_csv(csv_dir: Path) -> Path:
    files = sorted(csv_dir.glob("prometheus_metrics_*.csv"))
    if not files:
        raise RuntimeError("No CSV output found")
    return files[-1]


def _mean_nonzero(values: List[float]) -> float:
    nz = [v for v in values if abs(v) > 1e-12]
    if not nz:
        return 0.0
    return sum(nz) / len(nz)


def _percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    if len(s) == 1:
        return float(s[0])
    pos = (q / 100.0) * (len(s) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(s) - 1)
    frac = pos - lo
    return float(s[lo] * (1.0 - frac) + s[hi] * frac)


def load_kpis(csv_path: Path, warmup_sec: int = 15) -> Dict[str, float]:
    with csv_path.open("r", encoding="utf-8") as fp:
        rows = list(csv.DictReader(fp))
    if not rows:
        return {
            k: 0.0
            for k in [
                "latency_p50", "latency_p90", "latency_p95", "latency_p99", "latency_p999",
                "throughput", "throughput_out_avg_msgs_per_sec", "throughput_out_p50_msgs_per_sec", "throughput_out_p95_msgs_per_sec",
                "throughput_in_avg_msgs_per_sec", "goodput_avg_msgs_per_sec", "drain_ratio", "backlog_slope_per_sec",
                "rue", "eei", "fpp", "mis", "tlvr",
            ]
        }

    # Use a fixed steady-state window by excluding warmup seconds from KPI aggregation.
    first_ts = 0
    try:
        first_ts = int(float(rows[0].get("timestamp_epoch", "0") or 0))
    except Exception:
        first_ts = 0
    steady_rows = rows
    if first_ts > 0 and warmup_sec > 0:
        cutoff = first_ts + warmup_sec
        filtered = []
        for r in rows:
            try:
                ts = int(float(r.get("timestamp_epoch", "0") or 0))
                if ts >= cutoff:
                    filtered.append(r)
            except Exception:
                continue
        if filtered:
            steady_rows = filtered

    def series(name: str) -> List[float]:
        out = []
        for r in steady_rows:
            try:
                out.append(float(r.get(name, "0") or 0.0))
            except Exception:
                out.append(0.0)
        return out

    # Use per-tenant latency columns and average non-zero values across tenants.
    latency_keys = {
        "latency_p50": [k for k in steady_rows[0].keys() if k.startswith("latency_tenant_") and k.endswith("_p50_ms")],
        "latency_p90": [k for k in steady_rows[0].keys() if k.startswith("latency_tenant_") and k.endswith("_p90_ms")],
        "latency_p95": [k for k in steady_rows[0].keys() if k.startswith("latency_tenant_") and k.endswith("_p95_ms")],
        "latency_p99": [k for k in steady_rows[0].keys() if k.startswith("latency_tenant_") and k.endswith("_p99_ms")],
        "latency_p999": [k for k in steady_rows[0].keys() if k.startswith("latency_tenant_") and k.endswith("_p999_ms")],
    }

    latency_values: Dict[str, float] = {}
    for metric, keys in latency_keys.items():
        vals = []
        for key in keys:
            vals.extend(series(key))
        latency_values[metric] = _mean_nonzero(vals)

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
    goodput_avg = _mean_nonzero(goodput_series)

    return {
        **latency_values,
        "throughput": out_avg,
        "throughput_out_avg_msgs_per_sec": out_avg,
        "throughput_out_p50_msgs_per_sec": _percentile(out_series, 50.0),
        "throughput_out_p95_msgs_per_sec": _percentile(out_series, 95.0),
        "throughput_in_avg_msgs_per_sec": in_avg,
        "goodput_avg_msgs_per_sec": goodput_avg,
        "drain_ratio": out_avg / max(in_avg, 1e-6),
        "backlog_slope_per_sec": _mean_nonzero(series("system_backlog_slope_per_sec")),
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
    parser = argparse.ArgumentParser(description="Run true measured baseline experiments across scheduler modes")
    parser.add_argument("--duration-sec", type=int, default=120)
    parser.add_argument("--input-rate", type=int, default=100000)
    parser.add_argument("--records-per-tenant", type=int, default=50000)
    parser.add_argument("--dataset", default="iot-sensors")
    parser.add_argument("--tenant-id", default="tenant-iot")
    parser.add_argument("--warmup-sec", type=int, default=15)
    parser.add_argument("--out-dir", default="evaluation/results/true_baseline_runs")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    out_root = root / args.out_dir
    run_id = datetime.now().strftime("run_%Y%m%d_%H%M%S")
    run_dir = out_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    mode_results: Dict[str, Dict[str, float]] = {}
    mode_csv: Dict[str, str] = {}

    for mode in MODES:
        print(f"[true-baseline] mode={mode}")
        run(["docker", "compose", "up", "-d", "--build", "stream-coordinator"], cwd=root, env={"SCHEDULER_MODE": mode})
        wait_for_stream_coordinator_mode(mode)

        workload_cmd = [
            "python3", "scripts/run_workloads.py",
            "--datasets", args.dataset,
            "--tenant-ids", args.tenant_id,
            "--records-per-tenant", str(args.records_per_tenant),
            "--input-rate", str(args.input_rate),
            "--duration-sec", str(args.duration_sec),
            "--disable-synthetic-fallback",
            "--skip-download",
        ]
        producer = subprocess.Popen(workload_cmd, cwd=root, text=True)
        try:
            run(
                [
                    "python3", "evaluation/export_prometheus_csv.py",
                    "--duration-sec", str(args.duration_sec),
                    "--interval-sec", "1",
                    "--tenants", "tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot",
                    "--out-dir", str(run_dir / "csv" / mode),
                ],
                cwd=root,
            )
        finally:
            if producer.poll() is None:
                producer.terminate()
                try:
                    producer.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    producer.kill()

        csv_path = latest_csv(run_dir / "csv" / mode)
        mode_csv[mode] = str(csv_path)
        mode_results[mode] = load_kpis(csv_path, warmup_sec=args.warmup_sec)

        (run_dir / f"{mode}_kpis.json").write_text(json.dumps(mode_results[mode], indent=2), encoding="utf-8")

    sb = mode_results["streambazaar"]
    baselines = ["talos", "ds2", "capsys", "flink_default"]
    lines = [
        "True Measured StreamBazaar vs Baselines Report",
        f"Generated: {datetime.now().isoformat()}",
        "",
        "KPIs: latency p50-p999, throughput_out(avg/p50/p95), throughput_in_avg, goodput_avg, drain_ratio, backlog_slope, RUE, EEI, FPP, MIS, TLVR",
        "Rules: lower-is-better for latency/TLVR/MIS; higher-is-better for throughput/RUE/EEI/FPP",
        "",
    ]

    metrics = [
        "latency_p50", "latency_p90", "latency_p95", "latency_p99", "latency_p999",
        "throughput_out_avg_msgs_per_sec", "throughput_out_p50_msgs_per_sec", "throughput_out_p95_msgs_per_sec",
        "throughput_in_avg_msgs_per_sec", "goodput_avg_msgs_per_sec", "drain_ratio", "backlog_slope_per_sec",
        "rue", "eei", "fpp", "mis", "tlvr",
    ]

    for b in baselines:
        lines.append(f"=== StreamBazaar vs {b} (true measured) ===")
        for m in metrics:
            sbv = float(sb.get(m, 0.0))
            bv = float(mode_results[b].get(m, 0.0))
            imp = improvement(sbv, bv, m)
            direction = "lower-better" if m in LOWER_IS_BETTER else "higher-better"
            lines.append(f"{m}: StreamBazaar={sbv:.6f}, {b}={bv:.6f}, improvement={imp:.3f}% ({direction})")
        lines.append("")

    report_txt = run_dir / "true_measured_improvement_report.txt"
    report_txt.write_text("\n".join(lines), encoding="utf-8")

    summary = {
        "run_dir": str(run_dir),
        "mode_csv": mode_csv,
        "mode_kpis": mode_results,
        "report_txt": str(report_txt),
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"[true-baseline] run_dir={run_dir}")
    print(f"[true-baseline] report={report_txt}")


if __name__ == "__main__":
    main()
