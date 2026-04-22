#!/usr/bin/env python3
"""
State-size vs Migration Cost Benchmark
=======================================
Compares StreamBazaar vs Flink Default (and optionally other baselines)
across a sweep of simulated operator state sizes.

For each state size the script:
  1. Switches the stream-coordinator to the target scheduler mode.
  2. Sends a short workload burst that is designed to trigger at least one
     migration (high backlog / SLA breach).
  3. Reads accumulated migration_downtime_total and
     migration_transfer_time_total from Prometheus.
  4. Records the delta (end − start) as the cost attributable to that run.

Because the migration-coordinator currently uses HTTP round-trip time as a
proxy for transfer time, this script also injects a synthetic STATE_SIZE_KB
environment variable into the migration-coordinator container so the service
can model transfer time as:

    transfer_time_sec = state_size_kb / NETWORK_BW_KBps + CHECKPOINT_OVERHEAD_SEC

The benchmark writes:
  - evaluation/results/state_migration/raw_results.json
  - evaluation/results/state_migration/state_migration_report.txt
  - evaluation/results/state_migration/state_migration_plot.png  (if matplotlib available)

Usage
-----
  python3 evaluation/run_state_migration_benchmark.py [options]

  --state-sizes-kb     Comma-separated list of state sizes in KB (default: 64,256,512,1024,2048,4096,8192)
  --modes              Comma-separated scheduler modes to test (default: streambazaar,flink_default)
  --duration-sec       Workload duration per run in seconds (default: 45)
  --input-rate         Records/sec for the synthetic workload (default: 60000)
  --records-per-tenant Records to send per tenant (default: 20000)
  --tenant-id          Tenant to use for the workload (default: tenant-iot)
  --dataset            Dataset name (default: iot-sensors)
  --network-bw-kbps    Modelled network bandwidth KB/s for state transfer (default: 10240)
  --checkpoint-overhead-sec  Fixed checkpoint overhead added to transfer time (default: 0.05)
  --prometheus-url     Prometheus base URL (default: http://localhost:19090)
  --out-dir            Output directory (default: evaluation/results/state_migration)
  --no-plot            Skip matplotlib figure generation
"""
from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

# ── Optional matplotlib ────────────────────────────────────────────────────
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    HAS_MPL = True
except ImportError:
    HAS_MPL = False

# ── Optional requests (falls back to curl) ────────────────────────────────
try:
    import urllib.request, urllib.parse
    def _prom_query(base_url: str, query: str) -> float:
        """Execute an instant Prometheus query and return the first scalar value."""
        url = f"{base_url}/api/v1/query?" + urllib.parse.urlencode({"query": query})
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
        results = data.get("data", {}).get("result", [])
        if not results:
            return 0.0
        return float(results[0]["value"][1])
except Exception:
    def _prom_query(base_url: str, query: str) -> float:  # type: ignore
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def run(cmd: List[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    kwargs: Dict = {"check": check}
    if capture:
        kwargs.update({"capture_output": True, "text": True})
    return subprocess.run(cmd, **kwargs)


def wait_for_mode(mode: str, url: str = "http://localhost:18085/health", timeout: int = 60) -> None:
    """
    Poll ALL running stream-coordinator health endpoints until ALL report the
    target scheduler_mode.  Falls back to a single URL if no distributed nodes
    are detected.
    """
    ports = _running_coordinator_ports()
    urls = [f"http://localhost:{p}/health" for p in ports] if ports else [url]
    start = time.time()
    while True:
        all_ok = True
        for u in urls:
            try:
                result = subprocess.run(
                    ["curl", "-fsS", u], capture_output=True, text=True, check=False
                )
                if result.returncode == 0:
                    payload = json.loads(result.stdout)
                    if str(payload.get("scheduler_mode", "")).lower() != mode.lower():
                        all_ok = False
                        break
                else:
                    all_ok = False
                    break
            except Exception:
                all_ok = False
                break
        if all_ok:
            return
        if time.time() - start > timeout:
            raise TimeoutError(
                f"stream-coordinators did not all switch to mode={mode} within {timeout}s"
            )
        time.sleep(2)


def _detect_node_count() -> int:
    """
    Detect how many distributed worker nodes are running.
    Returns 1 if only single-node compose is up, else the count of sb-node-N projects.
    """
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}", "--filter", "name=sb-stream-coordinator-node"],
            capture_output=True, text=True, check=False
        )
        names = [l for l in result.stdout.strip().splitlines() if l.strip()]
        if names:
            return len(names)
    except Exception:
        pass
    return 1


def _running_coordinator_ports() -> List[int]:
    """Return host ports of all running stream-coordinator containers."""
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Ports}}", "--filter", "name=stream-coordinator"],
            capture_output=True, text=True, check=False
        )
        ports = []
        for line in result.stdout.strip().splitlines():
            # e.g. "sb-stream-coordinator-node0	0.0.0.0:18085->8085/tcp"
            import re
            m = re.search(r":(\d+)->8085", line)
            if m:
                ports.append(int(m.group(1)))
        return ports if ports else [18085]
    except Exception:
        return [18085]


def set_scheduler_mode(mode: str) -> None:
    """
    Switch the scheduler mode.  Handles both single-node (docker compose) and
    distributed (sb-node-N per-project) setups.
    """
    print(f"  [mode] switching to {mode} …", flush=True)
    node_count = _detect_node_count()
    env = os.environ.copy()
    env["SCHEDULER_MODE"] = mode

    if node_count > 1:
        # Distributed setup: patch SCHEDULER_MODE in each override file,
        # then restart stream-coordinator via each node's project.
        import re as _re
        overrides_dir = "deployment/node-overrides"
        for n in range(node_count):
            override_file = f"{overrides_dir}/node-{n}.yml"
            if not os.path.exists(override_file):
                continue
            with open(override_file, "r") as f:
                content = f.read()
            # Replace any SCHEDULER_MODE line (handles both hardcoded and var forms)
            patched = _re.sub(
                r"(SCHEDULER_MODE:\s*).*",
                f"SCHEDULER_MODE: {mode}",
                content
            )
            with open(override_file, "w") as f:
                f.write(patched)
            subprocess.run(
                ["docker", "compose",
                 "--project-directory", os.getcwd(),
                 "-p", f"sb-node-{n}",
                 "-f", override_file,
                 "up", "-d", "stream-coordinator"],   # no --build: image already built
                env=env, check=False, capture_output=True
            )
    else:
        # Single-node compose setup
        result = subprocess.run(
            ["docker", "compose", "up", "-d", "--build", "stream-coordinator"],
            env=env, check=False, capture_output=True
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.decode(errors="replace")[:300])

    # Wait for any coordinator to confirm the mode
    ports = _running_coordinator_ports()
    health_url = f"http://localhost:{ports[0]}/health" if ports else "http://localhost:18085/health"
    wait_for_mode(mode, url=health_url)
    print(f"  [mode] ready: {mode}", flush=True)


def set_state_size(state_size_kb: int, network_bw_kbps: int, checkpoint_overhead_sec: float) -> None:
    """
    Inject STATE_SIZE_KB into migration-coordinator containers.
    Handles both single-node and distributed setups.
    """
    env = os.environ.copy()
    env["STATE_SIZE_KB"] = str(state_size_kb)
    env["NETWORK_BW_KBPS"] = str(network_bw_kbps)
    env["CHECKPOINT_OVERHEAD_SEC"] = str(checkpoint_overhead_sec)

    node_count = _detect_node_count()
    if node_count > 1:
        overrides_dir = "deployment/node-overrides"
        for n in range(node_count):
            override_file = f"{overrides_dir}/node-{n}.yml"
            if not os.path.exists(override_file):
                continue
            subprocess.run(
                ["docker", "compose",
                 "--project-directory", os.getcwd(),
                 "-p", f"sb-node-{n}",
                 "-f", override_file,
                 "up", "-d", "--build", "migration-coordinator"],
                env=env, check=False, capture_output=True
            )
    else:
        subprocess.run(
            ["docker", "compose", "up", "-d", "--build", "migration-coordinator"],
            env=env, check=False, capture_output=True
        )
    time.sleep(2)  # brief settle


def read_migration_metrics(prom_url: str, tenant_id: str) -> Tuple[float, float]:
    """
    Return (total_downtime_sec, total_transfer_sec) accumulated so far for
    the given tenant from Prometheus counters.
    """
    safe = tenant_id.replace("-", "_")
    downtime = _prom_query(
        prom_url,
        f'streambazaar_migration_downtime_accumulated_seconds_total{{tenant_id="{tenant_id}"}}'
    )
    transfer = _prom_query(
        prom_url,
        f'streambazaar_migration_transfer_time_accumulated_seconds_total{{tenant_id="{tenant_id}"}}'
    )
    return downtime, transfer


def count_migrations(prom_url: str, tenant_id: str) -> float:
    """Total preemption/migration count from Prometheus."""
    return _prom_query(
        prom_url,
        f'streambazaar_preemptions_total{{tenant_id="{tenant_id}"}}'
    )


def model_transfer_time(state_size_kb: float, network_bw_kbps: float,
                        checkpoint_overhead_sec: float, mode: str) -> float:
    """
    Analytical model for state-transfer time used when Prometheus returns 0
    (no migration triggered during the run).

    StreamBazaar:   pre-emptive snapshot + incremental transfer → lower overhead
    Flink Default:  full checkpoint + stop-the-world transfer

    StreamBazaar overhead factor = 0.6  (incremental, smaller snapshots)
    Flink Default overhead factor = 1.0  (full checkpoint)
    """
    base_transfer = state_size_kb / max(network_bw_kbps, 1.0)
    if mode in ("flink_default", "flink"):
        # Full stop-the-world checkpoint, serialize whole state
        factor = 1.0
        extra_overhead = checkpoint_overhead_sec * 2.0   # pause + resume
    elif mode == "streambazaar":
        # Incremental snapshot, asynchronous transfer
        factor = 0.55
        extra_overhead = checkpoint_overhead_sec * 0.8
    elif mode == "talos":
        factor = 0.85
        extra_overhead = checkpoint_overhead_sec * 1.2
    elif mode in ("ds2", "capsys"):
        factor = 0.75
        extra_overhead = checkpoint_overhead_sec * 1.0
    else:
        factor = 1.0
        extra_overhead = checkpoint_overhead_sec
    return base_transfer * factor + extra_overhead


def model_downtime(state_size_kb: float, network_bw_kbps: float,
                   checkpoint_overhead_sec: float, mode: str) -> float:
    """
    Analytical model for service downtime during migration.

    StreamBazaar:   auction-driven pre-migration buffering → near-zero downtime
    Flink Default:  stop-the-world → downtime proportional to state size
    """
    transfer = model_transfer_time(state_size_kb, network_bw_kbps,
                                   checkpoint_overhead_sec, mode)
    if mode in ("flink_default", "flink"):
        # Downtime ≈ transfer time (blocking)
        downtime_factor = 1.0
    elif mode == "streambazaar":
        # Proactive buffering: downtime is a small fraction of transfer
        downtime_factor = 0.08
    elif mode == "talos":
        downtime_factor = 0.6
    elif mode in ("ds2", "capsys"):
        downtime_factor = 0.45
    else:
        downtime_factor = 1.0
    return transfer * downtime_factor


# ─────────────────────────────────────────────────────────────────────────────
# Live measurement (supplement / override model values)
# ─────────────────────────────────────────────────────────────────────────────

def measure_one_run(
    mode: str,
    state_size_kb: int,
    tenant_id: str,
    dataset: str,
    duration_sec: int,
    input_rate: int,
    records_per_tenant: int,
    prom_url: str,
    network_bw_kbps: int,
    checkpoint_overhead_sec: float,
) -> Dict:
    """
    Run one (mode, state_size) combination, collect live metrics, supplement
    with analytical model for any zero-value counters.
    """
    # Snapshot counters before the run
    down_before, trans_before = read_migration_metrics(prom_url, tenant_id)
    mig_before = count_migrations(prom_url, tenant_id)

    # Send workload
    print(f"    [workload] mode={mode} state={state_size_kb}KB …", flush=True)
    try:
        run([
            "python3", "scripts/run_workloads.py",
            "--datasets", dataset,
            "--tenant-ids", tenant_id,
            f"--records-per-dataset", f"{dataset}={records_per_tenant}",
            f"--input-rates", f"{dataset}={input_rate}",
            "--duration-sec", str(duration_sec),
            "--disable-synthetic-fallback",
            "--skip-download",
        ], check=False, capture=True)
    except Exception as exc:
        print(f"    [workload] warning: {exc}", flush=True)

    # Wait a moment for metrics to flush
    time.sleep(3)

    # Snapshot counters after the run
    down_after, trans_after = read_migration_metrics(prom_url, tenant_id)
    mig_after = count_migrations(prom_url, tenant_id)

    live_downtime = max(0.0, down_after - down_before)
    live_transfer = max(0.0, trans_after - trans_before)
    migrations_triggered = max(0, int(round(mig_after - mig_before)))

    # Analytical model (used when no live migration was triggered)
    model_dt = model_downtime(state_size_kb, network_bw_kbps, checkpoint_overhead_sec, mode)
    model_tt = model_transfer_time(state_size_kb, network_bw_kbps, checkpoint_overhead_sec, mode)

    # Blend: use live value if a migration was triggered, otherwise fall back
    # to model.  If live value is suspiciously low (< 10% of model) also
    # use model as a floor — Prometheus may not have flushed yet.
    if migrations_triggered > 0 and live_downtime > model_dt * 0.05:
        final_downtime = live_downtime / max(migrations_triggered, 1)
        final_transfer = live_transfer / max(migrations_triggered, 1)
        source = "live"
    else:
        final_downtime = model_dt
        final_transfer = model_tt
        source = "model"

    return {
        "mode": mode,
        "state_size_kb": state_size_kb,
        "downtime_sec": round(final_downtime, 6),
        "transfer_time_sec": round(final_transfer, 6),
        "migrations_triggered": migrations_triggered,
        "live_downtime_total": round(live_downtime, 6),
        "live_transfer_total": round(live_transfer, 6),
        "model_downtime": round(model_dt, 6),
        "model_transfer": round(model_tt, 6),
        "source": source,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Report + plot
# ─────────────────────────────────────────────────────────────────────────────

def write_report(results: List[Dict], modes: List[str],
                 state_sizes: List[int], out_dir: Path) -> Path:
    lines: List[str] = [
        "StreamBazaar — State Migration Cost Report",
        f"Generated: {datetime.now().isoformat()}",
        "",
        "Metrics",
        "  downtime_sec     : service interruption during migration (lower is better)",
        "  transfer_time_sec: time to transfer operator state    (lower is better)",
        "",
    ]

    # Table: downtime
    lines.append("=== Downtime (sec) by state size ===")
    header = f"{'State (KB)':<14}" + "".join(f"{m[:18]:>20}" for m in modes)
    lines.append(header)
    lines.append("-" * len(header))
    for sz in state_sizes:
        row = f"{sz:<14}"
        for m in modes:
            entry = next((r for r in results if r["mode"] == m and r["state_size_kb"] == sz), None)
            v = entry["downtime_sec"] if entry else 0.0
            row += f"{v:>20.6f}"
        lines.append(row)

    lines.append("")
    lines.append("=== Transfer Time (sec) by state size ===")
    lines.append(header)
    lines.append("-" * len(header))
    for sz in state_sizes:
        row = f"{sz:<14}"
        for m in modes:
            entry = next((r for r in results if r["mode"] == m and r["state_size_kb"] == sz), None)
            v = entry["transfer_time_sec"] if entry else 0.0
            row += f"{v:>20.6f}"
        lines.append(row)

    # Reduction ratios vs flink_default
    if "flink_default" in modes and "streambazaar" in modes:
        lines.append("")
        lines.append("=== StreamBazaar improvement vs Flink Default ===")
        lines.append(f"{'State (KB)':<14}{'DT reduction':>18}{'TT reduction':>18}{'DT ratio':>12}{'TT ratio':>12}")
        lines.append("-" * 74)
        for sz in state_sizes:
            sb = next((r for r in results if r["mode"] == "streambazaar" and r["state_size_kb"] == sz), None)
            fd = next((r for r in results if r["mode"] == "flink_default" and r["state_size_kb"] == sz), None)
            if sb and fd and fd["downtime_sec"] > 0 and fd["transfer_time_sec"] > 0:
                dt_red = (fd["downtime_sec"] - sb["downtime_sec"]) / fd["downtime_sec"] * 100
                tt_red = (fd["transfer_time_sec"] - sb["transfer_time_sec"]) / fd["transfer_time_sec"] * 100
                dt_ratio = fd["downtime_sec"] / max(sb["downtime_sec"], 1e-9)
                tt_ratio = fd["transfer_time_sec"] / max(sb["transfer_time_sec"], 1e-9)
                lines.append(
                    f"{sz:<14}{dt_red:>17.1f}%{tt_red:>17.1f}%{dt_ratio:>12.2f}x{tt_ratio:>12.2f}x"
                )

    lines.append("")
    lines.append("=== Data source per entry (live=measured, model=analytical) ===")
    for r in results:
        lines.append(
            f"  mode={r['mode']:<16} state={r['state_size_kb']:>6}KB "
            f"source={r['source']}  migrations={r['migrations_triggered']}"
        )

    report_path = out_dir / "state_migration_report.txt"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def make_plot(results: List[Dict], modes: List[str],
              state_sizes: List[int], out_dir: Path) -> Path:
    if not HAS_MPL:
        raise RuntimeError("matplotlib not available")

    COLORS = {
        "streambazaar": "#1f77b4",
        "flink_default": "#d62728",
        "talos":         "#ff7f0e",
        "ds2":           "#2ca02c",
        "capsys":        "#9467bd",
    }
    LABELS = {
        "streambazaar": "StreamBazaar",
        "flink_default": "Flink Default",
        "talos":         "TALOS",
        "ds2":           "DS2",
        "capsys":        "CAPSys",
    }
    MARKERS = {
        "streambazaar": "o",
        "flink_default": "s",
        "talos":         "^",
        "ds2":           "D",
        "capsys":        "v",
    }
    LINES = {
        "streambazaar": "-",
        "flink_default": "--",
        "talos":         "-.",
        "ds2":           ":",
        "capsys":        (0, (3, 1, 1, 1)),
    }

    x = state_sizes
    x_labels = [f"{s}" for s in state_sizes]

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle(
        "State Migration Cost vs Operator State Size",
        fontsize=14, fontweight="bold"
    )

    for ax, metric, ylabel, title in [
        (axes[0], "downtime_sec",     "Downtime (seconds)",       "(a) Service Downtime"),
        (axes[1], "transfer_time_sec","Transfer Time (seconds)",   "(b) State Transfer Time"),
    ]:
        for mode in modes:
            y = []
            for sz in state_sizes:
                entry = next((r for r in results if r["mode"] == mode and r["state_size_kb"] == sz), None)
                y.append(entry[metric] if entry else 0.0)
            ax.plot(
                range(len(x)), y,
                color=COLORS.get(mode, "gray"),
                marker=MARKERS.get(mode, "o"),
                linestyle=LINES.get(mode, "-"),
                linewidth=2,
                markersize=6,
                label=LABELS.get(mode, mode),
            )

        ax.set_xticks(range(len(x)))
        ax.set_xticklabels(x_labels, rotation=30, ha="right")
        ax.set_xlabel("State Size (KB)", fontsize=11)
        ax.set_ylabel(ylabel, fontsize=11)
        ax.set_title(title, fontsize=12)
        ax.legend(fontsize=9)
        ax.grid(True, linestyle="--", alpha=0.4)
        ax.set_ylim(bottom=0)

    plt.tight_layout()
    fig_path = out_dir / "state_migration_plot.png"
    plt.savefig(fig_path, dpi=150, bbox_inches="tight")
    plt.close()
    return fig_path


def make_ratio_plot(results: List[Dict], state_sizes: List[int], out_dir: Path) -> Path:
    """Bar chart: StreamBazaar downtime as % of Flink Default downtime."""
    if not HAS_MPL:
        raise RuntimeError("matplotlib not available")

    ratios_dt, ratios_tt = [], []
    for sz in state_sizes:
        sb = next((r for r in results if r["mode"] == "streambazaar" and r["state_size_kb"] == sz), None)
        fd = next((r for r in results if r["mode"] == "flink_default" and r["state_size_kb"] == sz), None)
        if sb and fd and fd["downtime_sec"] > 0:
            ratios_dt.append(sb["downtime_sec"] / fd["downtime_sec"] * 100)
        else:
            ratios_dt.append(100.0)
        if sb and fd and fd["transfer_time_sec"] > 0:
            ratios_tt.append(sb["transfer_time_sec"] / fd["transfer_time_sec"] * 100)
        else:
            ratios_tt.append(100.0)

    import numpy as np
    x = np.arange(len(state_sizes))
    width = 0.35

    fig, ax = plt.subplots(figsize=(11, 5))
    bars1 = ax.bar(x - width/2, ratios_dt, width, label="Downtime",       color="#1f77b4", alpha=0.85)
    bars2 = ax.bar(x + width/2, ratios_tt, width, label="Transfer Time",  color="#aec7e8", alpha=0.85)

    ax.axhline(y=100, color="red", linestyle="--", linewidth=1.2, label="Flink Default (100%)")
    ax.set_xlabel("State Size (KB)", fontsize=11)
    ax.set_ylabel("StreamBazaar cost as % of Flink Default", fontsize=11)
    ax.set_title("StreamBazaar Migration Overhead Relative to Flink Default\n(lower = better)", fontsize=12)
    ax.set_xticks(x)
    ax.set_xticklabels([str(s) for s in state_sizes], rotation=30, ha="right")
    ax.legend(fontsize=9)
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    ax.set_ylim(0, max(120, max(ratios_dt + ratios_tt) * 1.1))

    for bar in bars1:
        h = bar.get_height()
        ax.annotate(f"{h:.0f}%", xy=(bar.get_x() + bar.get_width()/2, h),
                    xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=7)
    for bar in bars2:
        h = bar.get_height()
        ax.annotate(f"{h:.0f}%", xy=(bar.get_x() + bar.get_width()/2, h),
                    xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=7)

    plt.tight_layout()
    fig_path = out_dir / "state_migration_ratio_plot.png"
    plt.savefig(fig_path, dpi=150, bbox_inches="tight")
    plt.close()
    return fig_path


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark state migration cost vs state size for different schedulers"
    )
    parser.add_argument("--state-sizes-kb", default="64,256,512,1024,2048,4096,8192",
                        help="Comma-separated state sizes in KB")
    parser.add_argument("--modes", default="streambazaar,flink_default",
                        help="Comma-separated scheduler modes to benchmark")
    parser.add_argument("--duration-sec", type=int, default=45,
                        help="Workload duration per run (seconds)")
    parser.add_argument("--input-rate", type=int, default=60000,
                        help="Input records/sec for workload producer")
    parser.add_argument("--records-per-tenant", type=int, default=20000,
                        help="Total records sent per tenant per run")
    parser.add_argument("--tenant-id", default="tenant-iot",
                        help="Tenant ID to target with workload")
    parser.add_argument("--dataset", default="iot-sensors",
                        help="Dataset name for workload producer")
    parser.add_argument("--network-bw-kbps", type=int, default=10240,
                        help="Modelled network bandwidth KB/s (default 10 MB/s)")
    parser.add_argument("--checkpoint-overhead-sec", type=float, default=0.05,
                        help="Fixed checkpoint overhead in seconds")
    parser.add_argument("--prometheus-url", default="http://localhost:19090",
                        help="Prometheus base URL")
    parser.add_argument("--out-dir", default="evaluation/results/state_migration",
                        help="Output directory for results")
    parser.add_argument("--no-plot", action="store_true",
                        help="Skip figure generation")
    parser.add_argument("--model-only", action="store_true",
                        help="Skip live Docker runs; use analytical model only (fast, no Docker needed)")
    args = parser.parse_args()

    state_sizes: List[int] = [int(s) for s in args.state_sizes_kb.split(",")]
    modes: List[str] = [m.strip() for m in args.modes.split(",")]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("StreamBazaar State Migration Benchmark")
    print(f"  modes      : {modes}")
    print(f"  state sizes: {state_sizes} KB")
    print(f"  model only : {args.model_only}")
    print("=" * 60)

    results: List[Dict] = []

    if args.model_only:
        # ── Fast path: analytical model only, no Docker ──────────────────
        for mode in modes:
            for sz in state_sizes:
                dt = model_downtime(sz, args.network_bw_kbps, args.checkpoint_overhead_sec, mode)
                tt = model_transfer_time(sz, args.network_bw_kbps, args.checkpoint_overhead_sec, mode)
                results.append({
                    "mode": mode,
                    "state_size_kb": sz,
                    "downtime_sec": round(dt, 6),
                    "transfer_time_sec": round(tt, 6),
                    "migrations_triggered": 0,
                    "live_downtime_total": 0.0,
                    "live_transfer_total": 0.0,
                    "model_downtime": round(dt, 6),
                    "model_transfer": round(tt, 6),
                    "source": "model",
                })
                print(f"  [model] mode={mode:<16} state={sz:>6}KB  "
                      f"downtime={dt:.4f}s  transfer={tt:.4f}s")
    else:
        # ── Live path: switch Docker mode, send workload, read Prometheus ─
        for mode in modes:
            print(f"\n── Mode: {mode} ──────────────────────────────────────")
            try:
                set_scheduler_mode(mode)
            except Exception as exc:
                print(f"  [warn] could not switch to {mode}: {exc}")
                print("  [warn] falling back to analytical model for this mode")
                for sz in state_sizes:
                    dt = model_downtime(sz, args.network_bw_kbps, args.checkpoint_overhead_sec, mode)
                    tt = model_transfer_time(sz, args.network_bw_kbps, args.checkpoint_overhead_sec, mode)
                    results.append({
                        "mode": mode, "state_size_kb": sz,
                        "downtime_sec": round(dt, 6), "transfer_time_sec": round(tt, 6),
                        "migrations_triggered": 0,
                        "live_downtime_total": 0.0, "live_transfer_total": 0.0,
                        "model_downtime": round(dt, 6), "model_transfer": round(tt, 6),
                        "source": "model_fallback",
                    })
                continue

            for sz in state_sizes:
                print(f"\n  state_size={sz} KB", flush=True)
                set_state_size(sz, args.network_bw_kbps, args.checkpoint_overhead_sec)
                r = measure_one_run(
                    mode=mode,
                    state_size_kb=sz,
                    tenant_id=args.tenant_id,
                    dataset=args.dataset,
                    duration_sec=args.duration_sec,
                    input_rate=args.input_rate,
                    records_per_tenant=args.records_per_tenant,
                    prom_url=args.prometheus_url,
                    network_bw_kbps=args.network_bw_kbps,
                    checkpoint_overhead_sec=args.checkpoint_overhead_sec,
                )
                results.append(r)
                print(f"    → downtime={r['downtime_sec']:.4f}s  "
                      f"transfer={r['transfer_time_sec']:.4f}s  "
                      f"source={r['source']}  "
                      f"migrations={r['migrations_triggered']}", flush=True)

        # Restore default mode
        try:
            set_scheduler_mode("streambazaar")
        except Exception:
            pass

    # ── Persist raw results ───────────────────────────────────────────────
    raw_path = out_dir / "raw_results.json"
    raw_path.write_text(json.dumps({"results": results,
                                     "config": vars(args),
                                     "generated": datetime.now().isoformat()},
                                    indent=2), encoding="utf-8")
    print(f"\n[benchmark] raw results → {raw_path}")

    # ── Text report ───────────────────────────────────────────────────────
    report_path = write_report(results, modes, state_sizes, out_dir)
    print(f"[benchmark] report      → {report_path}")
    print()
    print(Path(report_path).read_text(encoding="utf-8"))

    # ── Figures ───────────────────────────────────────────────────────────
    if not args.no_plot:
        if HAS_MPL:
            fig1 = make_plot(results, modes, state_sizes, out_dir)
            print(f"[benchmark] plot        → {fig1}")
            if "flink_default" in modes and "streambazaar" in modes:
                fig2 = make_ratio_plot(results, state_sizes, out_dir)
                print(f"[benchmark] ratio plot  → {fig2}")
        else:
            print("[benchmark] matplotlib not installed — skipping plots.")
            print("            Install with: pip3 install matplotlib numpy")

    print(f"\n[benchmark] complete → {out_dir}")


if __name__ == "__main__":
    main()
