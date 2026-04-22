import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator

from ..dataset_loaders import BerkeleyLoader, CriteoLoader, FraudLoader, UNSWLoader
from ..download_manager import DatasetManager, DatasetManagerConfig
from .fraud_workload import FraudDetectionWorkloadGenerator
from .iot_sensor_workload import IoTSensorAnalyticsWorkloadGenerator
from .network_intrusion_workload import NetworkIntrusionWorkloadGenerator
from .web_analytics_workload import WebAnalyticsWorkloadGenerator


DATASET_ALIASES = {
    "fraud-detection": "fraud",
    "fraud": "fraud",
    "clickstream": "web-analytics",
    "web-analytics": "web-analytics",
    "criteo": "web-analytics",
    "network-intrusion": "network-intrusion",
    "unsw": "network-intrusion",
    "intrusion": "network-intrusion",
    "iot": "iot-sensors",
    "iot-sensors": "iot-sensors",
    "berkeley": "iot-sensors",
    "ml": "iot-sensors",
}


@dataclass
class DatasetRuntimeOptions:
    dataset_root: Path
    allow_download: bool
    enable_synthetic_fallback: bool
    subset_lines: int
    criteo_subset_lines: int
    replay_window_compression: float


def normalize_dataset_key(raw: str) -> str:
    key = raw.strip().lower()
    if key not in DATASET_ALIASES:
        supported = sorted(set(DATASET_ALIASES.values()))
        raise ValueError(f"Unsupported dataset '{raw}'. Supported datasets: {supported}")
    return DATASET_ALIASES[key]


def build_workload_generators(
    tenant_ids: Dict[str, str],
    datasets: list[str],
    options: DatasetRuntimeOptions,
    logger: logging.Logger,
) -> Dict[str, Iterator[Dict]]:
    normalized = [normalize_dataset_key(d) for d in datasets]
    manager = DatasetManager(
        DatasetManagerConfig(
            root_dir=options.dataset_root,
            enable_downloads=options.allow_download,
            enable_synthetic_fallback=options.enable_synthetic_fallback,
            subset_lines=options.subset_lines,
            criteo_subset_lines=options.criteo_subset_lines,
            logger_name="streambazaar.datasets",
        )
    )

    statuses = manager.ensure_datasets(normalized)
    result: Dict[str, Iterator[Dict]] = {}

    for canonical in normalized:
        tenant_id = tenant_ids[canonical]
        status = statuses[canonical]
        if not status.validated:
            if status.synthetic_fallback:
                use_synthetic = True
            else:
                detail = "; ".join(status.errors) if status.errors else "dataset validation failed"
                raise RuntimeError(
                    f"Dataset '{canonical}' is unavailable and synthetic fallback is disabled: {detail}"
                )
        else:
            use_synthetic = False
        ds_dir = manager.dataset_path(canonical)
        line_limit = options.criteo_subset_lines if canonical == "web-analytics" else options.subset_lines

        if canonical == "fraud":
            loader = FraudLoader(ds_dir, logger=logger)
            gen = FraudDetectionWorkloadGenerator(
                tenant_id=tenant_id,
                replay_window_compression=options.replay_window_compression,
                record_source=loader.records(subset_lines=line_limit, use_synthetic=use_synthetic),
            ).generate_workload()
        elif canonical == "web-analytics":
            loader = CriteoLoader(ds_dir, logger=logger)
            gen = WebAnalyticsWorkloadGenerator(
                tenant_id=tenant_id,
                replay_window_compression=options.replay_window_compression,
                record_source=loader.records(subset_lines=line_limit, use_synthetic=use_synthetic),
            ).generate_workload()
        elif canonical == "network-intrusion":
            loader = UNSWLoader(ds_dir, logger=logger)
            gen = NetworkIntrusionWorkloadGenerator(
                tenant_id=tenant_id,
                replay_window_compression=options.replay_window_compression,
                record_source=loader.records(subset_lines=line_limit, use_synthetic=use_synthetic),
            ).generate_workload()
        elif canonical == "iot-sensors":
            loader = BerkeleyLoader(ds_dir, logger=logger)
            gen = IoTSensorAnalyticsWorkloadGenerator(
                tenant_id=tenant_id,
                replay_window_compression=options.replay_window_compression,
                record_source=loader.records(subset_lines=line_limit, use_synthetic=use_synthetic),
            ).generate_workload()
        else:
            raise ValueError(f"Unknown canonical dataset key: {canonical}")

        logger.info(
            "Prepared dataset '%s' for tenant '%s' (real=%s, synthetic=%s)",
            canonical,
            tenant_id,
            status.validated,
            use_synthetic,
        )
        result[canonical] = gen

    return result
