#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from evaluation.baseline_comparison import BaselineComparison


def main() -> None:
    parser = argparse.ArgumentParser(description="Run StreamBazaar vs TALOS/DS2/CAPSys/FlinkDefault baseline comparison")
    parser.add_argument("--prom-url", default="http://localhost:19090", help="Prometheus base URL")
    parser.add_argument(
        "--tenant-ids",
        default="tenant-fraud,tenant-clickstream,tenant-ml,tenant-iot",
        help="Comma-separated tenant IDs to gather metrics for",
    )
    parser.add_argument(
        "--no-live-metrics",
        action="store_true",
        help="Skip Prometheus gathering and use built-in snapshot",
    )
    args = parser.parse_args()

    tenant_ids = [t.strip() for t in args.tenant_ids.split(",") if t.strip()]
    comparison = BaselineComparison(prometheus_url=args.prom_url)
    results = comparison.run_comparison(
        tenant_ids=tenant_ids,
        gather_live_metrics=not args.no_live_metrics,
    )

    out = Path("evaluation") / "baseline_comparison_results.json"
    out.write_text(json.dumps(results, indent=2), encoding="utf-8")

    print("Baseline comparison complete")
    print(f"Saved: {out}")
    print("Schedulers: StreamBazaar, TALOS, DS2, CAPSys, FlinkDefault")


if __name__ == "__main__":
    main()
