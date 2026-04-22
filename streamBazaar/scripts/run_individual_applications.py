#!/usr/bin/env python3
import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from datasets.workload_generators.factory import (  # noqa: E402
    DatasetRuntimeOptions,
    build_workload_generators,
    normalize_dataset_key,
)

DEFAULT_DATASETS = ["fraud", "web-analytics", "network-intrusion", "iot-sensors"]


def build_tenant_ids(datasets: list[str]) -> Dict[str, str]:
    return {dataset: f"tenant-{dataset}" for dataset in datasets}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run each application pipeline directly from real datasets and print output events"
    )
    parser.add_argument(
        "--datasets",
        default=",".join(DEFAULT_DATASETS),
        help="Comma-separated list: fraud,web-analytics,network-intrusion,iot-sensors",
    )
    parser.add_argument("--samples", type=int, default=3, help="How many events to print per application")
    parser.add_argument("--subset-lines", type=int, default=20000, help="Read cap per source dataset")
    parser.add_argument("--criteo-subset-lines", type=int, default=200000, help="Read cap for web analytics source")
    parser.add_argument(
        "--dataset-root",
        default="",
        help="Dataset root directory (default: streamBazaar/datasets)",
    )
    parser.add_argument(
        "--allow-synthetic-fallback",
        action="store_true",
        help="Allow synthetic fallback if real files are missing",
    )
    args = parser.parse_args()

    logger = logging.getLogger("streambazaar.individual-applications")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s | %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    dataset_root = Path(args.dataset_root) if args.dataset_root else BASE_DIR / "datasets"
    datasets = [normalize_dataset_key(x.strip()) for x in args.datasets.split(",") if x.strip()]
    tenant_ids = build_tenant_ids(datasets)

    options = DatasetRuntimeOptions(
        dataset_root=dataset_root,
        allow_download=False,
        enable_synthetic_fallback=bool(args.allow_synthetic_fallback),
        subset_lines=max(0, args.subset_lines),
        criteo_subset_lines=max(0, args.criteo_subset_lines),
        replay_window_compression=10.0,
    )

    generators = build_workload_generators(tenant_ids=tenant_ids, datasets=datasets, options=options, logger=logger)

    for dataset in datasets:
        print(f"\n=== APPLICATION: {dataset} ===")
        gen = generators[dataset]
        for idx in range(1, args.samples + 1):
            event = next(gen)
            preview = {
                "sample": idx,
                "dataset": dataset,
                "tenant_id": event.get("tenant_id"),
                "operator_count": event.get("operator_count"),
                "operators": event.get("operators"),
                "operator_trace": event.get("operator_trace", []),
                "output_fields": {
                    k: event.get(k)
                    for k in [
                        "predicted_fraud",
                        "model_score",
                        "analytics_label",
                        "campaign_rank",
                        "predicted_attack",
                        "alert_severity",
                        "is_anomaly",
                        "anomaly_score",
                    ]
                    if k in event
                },
            }
            print(json.dumps(preview, indent=2))


if __name__ == "__main__":
    main()
