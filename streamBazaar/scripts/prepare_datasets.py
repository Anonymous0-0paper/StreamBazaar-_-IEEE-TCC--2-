#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from datasets.download_manager import DatasetManager, DatasetManagerConfig, network_available
from datasets.workload_generators.factory import normalize_dataset_key


def parse_csv_list(raw: str) -> list[str]:
    return [x.strip() for x in raw.split(",") if x.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Preflight/download StreamBazaar evaluation datasets")
    parser.add_argument(
        "--datasets",
        default="fraud,web-analytics,network-intrusion,iot-sensors",
        help="Comma-separated datasets to prepare",
    )
    parser.add_argument(
        "--dataset-root",
        default="",
        help="Dataset root directory (default: streamBazaar/datasets)",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Only check local files; do not attempt downloads",
    )
    parser.add_argument(
        "--disable-synthetic-fallback",
        action="store_true",
        help="Mark missing datasets as failures instead of synthetic fallback",
    )
    parser.add_argument(
        "--criteo-subset-lines",
        type=int,
        default=500000,
        help="Subset lines to keep from Criteo train.txt",
    )
    parser.add_argument(
        "--required-free-gb",
        type=float,
        default=5.0,
        help="Minimum free disk in GB required before downloads",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON summary",
    )
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parents[1]
    dataset_root = Path(args.dataset_root) if args.dataset_root else base_dir / "datasets"
    datasets = [normalize_dataset_key(d) for d in parse_csv_list(args.datasets)]

    manager = DatasetManager(
        DatasetManagerConfig(
            root_dir=dataset_root,
            enable_downloads=not args.skip_download,
            enable_synthetic_fallback=not args.disable_synthetic_fallback,
            criteo_subset_lines=max(0, args.criteo_subset_lines),
            required_free_gb=max(0.1, args.required_free_gb),
        )
    )

    net_ok = network_available()
    if not net_ok and not args.skip_download:
        print("[warning] Network appears unavailable; downloads may fail.")

    statuses = manager.ensure_datasets(datasets)

    summary = {
        "dataset_root": str(dataset_root),
        "network_available": net_ok,
        "downloads_enabled": not args.skip_download,
        "synthetic_fallback_enabled": not args.disable_synthetic_fallback,
        "datasets": {},
    }

    hard_failures = []
    for key in datasets:
        st = statuses[key]
        entry = {
            "exists": st.exists,
            "validated": st.validated,
            "synthetic_fallback": st.synthetic_fallback,
            "errors": st.errors,
            "path": str(manager.dataset_path(key)),
        }
        summary["datasets"][key] = entry

        if not st.validated and not st.synthetic_fallback:
            hard_failures.append((key, st.errors))

    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        print(f"Dataset root: {summary['dataset_root']}")
        print(f"Network available: {summary['network_available']}")
        for key, entry in summary["datasets"].items():
            status_text = "VALID" if entry["validated"] else "SYNTHETIC" if entry["synthetic_fallback"] else "MISSING"
            print(f"- {key}: {status_text} @ {entry['path']}")
            for err in entry["errors"]:
                print(f"  * {err}")

    if hard_failures:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
