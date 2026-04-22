#!/usr/bin/env python3
import argparse
import glob
import json
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import List


def run_cmd(cmd: List[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check)


def latest_report() -> Path:
    reports = sorted(glob.glob("evaluation_report_*.json"))
    if not reports:
        raise RuntimeError("No evaluation report found")
    return Path(reports[-1])


def wait_for_health(endpoints: List[str], timeout_sec: int = 120) -> None:
    start = time.time()
    while True:
        all_ok = True
        for endpoint in endpoints:
            try:
                result = subprocess.run(["curl", "-fsS", endpoint], check=False, capture_output=True, text=True)
                if result.returncode != 0:
                    all_ok = False
                    break
            except Exception:
                all_ok = False
                break
        if all_ok:
            return
        if time.time() - start > timeout_sec:
            raise TimeoutError("Timed out waiting for service health")
        time.sleep(2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run repeatable StreamBazaar experiments for paper reporting")
    parser.add_argument("--runs", type=int, default=3, help="Number of experiment repetitions")
    parser.add_argument("--warmup-sec", type=int, default=20, help="Warmup duration before measurement")
    parser.add_argument("--steady-sec", type=int, default=60, help="Measurement duration")
    parser.add_argument("--records-per-tenant", type=int, default=300, help="Records per tenant for workload producer")
    parser.add_argument(
        "--datasets",
        default="fraud,web-analytics,network-intrusion,iot-sensors",
        help="Comma-separated datasets for workload producer",
    )
    parser.add_argument(
        "--tenant-ids",
        default="tenant-fraud,tenant-web,tenant-intrusion,tenant-iot",
        help="Comma-separated tenant IDs",
    )
    parser.add_argument("--input-rate", type=int, default=1000, help="Default publish rate per dataset")
    parser.add_argument(
        "--input-rates",
        default="",
        help="Optional per-dataset rate overrides: fraud=120000,web-analytics=500000",
    )
    parser.add_argument("--payload-bytes", type=int, default=0, help="Default extra payload bytes per message")
    parser.add_argument("--payload-bytes-map", default="", help="Optional per-dataset payload bytes overrides")
    parser.add_argument("--input-topic-template", default="tenant.{tenant_id}.input")
    parser.add_argument("--output-topic-template", default="tenant.{tenant_id}.output")
    parser.add_argument("--bids-topic", default="streamBazaar.bids")
    parser.add_argument("--alloc-topic", default="streamBazaar.allocations")
    parser.add_argument("--preempt-topic", default="streamBazaar.preemptions")
    parser.add_argument("--metrics-topic", default="streamBazaar.metrics")
    parser.add_argument("--csv-monitor-sec", type=int, default=0, help="If >0, export Prometheus KPIs to CSV for this duration each run")
    parser.add_argument("--out-dir", default="evaluation/results/raw", help="Directory for raw report artifacts")
    args = parser.parse_args()

    out_root = Path(args.out_dir)
    out_root.mkdir(parents=True, exist_ok=True)
    exp_id = datetime.now().strftime("exp_%Y%m%d_%H%M%S")
    out_dir = out_root / exp_id
    out_dir.mkdir(parents=True, exist_ok=True)

    health_endpoints = [
        "http://localhost:18080/health",
        "http://localhost:18081/health",
        "http://localhost:18082/health",
        "http://localhost:18083/health",
        "http://localhost:18084/health",
        "http://localhost:18085/health",
    ]

    wait_for_health(health_endpoints)
    run_cmd(
        [
            "env",
            f"TENANT_IDS={args.tenant_ids}",
            f"INPUT_TOPIC_TEMPLATE={args.input_topic_template}",
            f"OUTPUT_TOPIC_TEMPLATE={args.output_topic_template}",
            f"BIDS_TOPIC={args.bids_topic}",
            f"ALLOC_TOPIC={args.alloc_topic}",
            f"PREEMPT_TOPIC={args.preempt_topic}",
            f"METRICS_TOPIC={args.metrics_topic}",
            "bash",
            "./scripts/create-kafka-topics.sh",
        ]
    )

    run_manifest = []
    csv_dir = out_dir / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_files: List[str] = []
    for run_idx in range(1, args.runs + 1):
        print(f"[experiment] run {run_idx}/{args.runs}")
        workload_total_sec = args.warmup_sec + args.steady_sec + 10

        producer = subprocess.Popen(
            [
                "python3",
                "scripts/run_workloads.py",
                "--duration-sec",
                str(workload_total_sec),
                "--datasets",
                args.datasets,
                "--tenant-ids",
                args.tenant_ids,
                "--records-per-tenant",
                str(args.records_per_tenant),
                "--input-rate",
                str(args.input_rate),
                "--input-rates",
                args.input_rates,
                "--payload-bytes",
                str(args.payload_bytes),
                "--payload-bytes-map",
                args.payload_bytes_map,
                "--bids-topic",
                args.bids_topic,
                "--input-topic-template",
                args.input_topic_template,
            ]
        )
        csv_proc = None

        try:
            print(f"[experiment] warmup {args.warmup_sec}s")
            time.sleep(args.warmup_sec)

            if args.csv_monitor_sec > 0:
                csv_proc = subprocess.Popen(
                    [
                        "python3",
                        "evaluation/export_prometheus_csv.py",
                        "--duration-sec",
                        str(args.csv_monitor_sec),
                        "--out-dir",
                        str(csv_dir),
                        "--tenants",
                        args.tenant_ids,
                    ]
                )

            duration_minutes = args.steady_sec / 60.0
            print(f"[experiment] measurement {args.steady_sec}s ({duration_minutes:.3f} min)")
            run_cmd(["python3", "evaluation/run_evaluation.py", "--duration", str(duration_minutes)])

            report = latest_report()
            dst = out_dir / f"run_{run_idx:02d}_{report.name}"
            shutil.copy2(report, dst)
            run_manifest.append(
                {
                    "run": run_idx,
                    "warmup_sec": args.warmup_sec,
                    "steady_sec": args.steady_sec,
                    "records_per_tenant": args.records_per_tenant,
                    "report": str(dst),
                }
            )

            if args.csv_monitor_sec > 0:
                latest_csv = sorted(csv_dir.glob("prometheus_metrics_*.csv"))
                if latest_csv:
                    csv_files.append(str(latest_csv[-1]))
            print(f"[experiment] saved {dst}")
        finally:
            if producer.poll() is None:
                producer.terminate()
                try:
                    producer.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    producer.kill()
            if csv_proc is not None and csv_proc.poll() is None:
                csv_proc.terminate()
                try:
                    csv_proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    csv_proc.kill()

    manifest_path = out_dir / "run_manifest.json"
    summary_path = out_dir / "summary.json"
    fig_dir = out_dir / "figures"

    run_cmd(
        [
            "python3",
            "evaluation/analysis-scripts/aggregate_reports.py",
            "--reports-dir",
            str(out_dir),
            "--out",
            str(summary_path),
        ]
    )
    run_cmd(
        [
            "python3",
            "evaluation/analysis-scripts/plot_results.py",
            "--summary",
            str(summary_path),
            "--fig-dir",
            str(fig_dir),
        ]
    )

    if csv_files:
        run_cmd(
            [
                "python3",
                "evaluation/analysis-scripts/plot_publication_metrics.py",
                "--csv",
                csv_files[-1],
                "--fig-dir",
                str(out_dir / "figures_publication"),
                "--tenants",
                args.tenant_ids,
            ]
        )

    manifest_path.write_text(
        json.dumps(
            {
                "runs": run_manifest,
                "csv_files": csv_files,
                "summary": str(summary_path),
                "figures": str(fig_dir),
                "publication_figures": str(out_dir / "figures_publication"),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"[experiment] complete -> {out_dir}")


if __name__ == "__main__":
    main()
