#!/usr/bin/env python3
import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Dict, Iterator

from kafka import KafkaProducer

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from datasets.workload_generators.factory import (  # noqa: E402
    DatasetRuntimeOptions,
    build_workload_generators,
    normalize_dataset_key,
)


DEFAULT_DATASETS = "fraud,web-analytics,network-intrusion,iot-sensors"
DEFAULT_PRIORITIES = {
    "fraud": "high",
    "web-analytics": "low",
    "network-intrusion": "high",
    "iot-sensors": "medium",
}
DEFAULT_INPUT_RATES = {
    "fraud": 100000,
    "web-analytics": 500000,
    "network-intrusion": 100000,
    "iot-sensors": 100000,
}


def parse_csv_list(raw: str) -> list[str]:
    return [x.strip() for x in raw.split(",") if x.strip()]


def parse_kv_float(raw: str) -> Dict[str, float]:
    result: Dict[str, float] = {}
    if not raw:
        return result
    for part in parse_csv_list(raw):
        key, value = part.split("=", 1)
        result[key.strip()] = float(value.strip())
    return result


def parse_kv_int(raw: str) -> Dict[str, int]:
    result: Dict[str, int] = {}
    if not raw:
        return result
    for part in parse_csv_list(raw):
        key, value = part.split("=", 1)
        result[key.strip()] = int(value.strip())
    return result


def parse_kv_str(raw: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    if not raw:
        return result
    for part in parse_csv_list(raw):
        key, value = part.split("=", 1)
        result[key.strip()] = value.strip()
    return result


def build_tenant_ids(datasets: list[str], tenant_ids_raw: str) -> Dict[str, str]:
    if tenant_ids_raw:
        values = parse_csv_list(tenant_ids_raw)
        if len(values) != len(datasets):
            raise ValueError("--tenant-ids count must match --datasets count")
        return {dataset: tenant for dataset, tenant in zip(datasets, values)}
    return {dataset: f"tenant-{dataset}" for dataset in datasets}


def build_generators(
    datasets: list[str],
    tenant_ids: Dict[str, str],
    dataset_root: Path,
    allow_download: bool,
    enable_synthetic_fallback: bool,
    subset_lines: int,
    criteo_subset_lines: int,
    replay_window_compression: float,
    logger: logging.Logger,
) -> Dict[str, Iterator[Dict]]:
    options = DatasetRuntimeOptions(
        dataset_root=dataset_root,
        allow_download=allow_download,
        enable_synthetic_fallback=enable_synthetic_fallback,
        subset_lines=subset_lines,
        criteo_subset_lines=criteo_subset_lines,
        replay_window_compression=replay_window_compression,
    )
    return build_workload_generators(tenant_ids=tenant_ids, datasets=datasets, options=options, logger=logger)


def compute_state_sizes(operator_count: int, min_gb: float, max_gb: float, avg_gb: float) -> list[float]:
    if operator_count <= 0:
        return []
    if operator_count == 1:
        return [round(avg_gb, 3)]

    interior_count = max(0, operator_count - 2)
    values = [min_gb] + [avg_gb] * interior_count + [max_gb]
    return [round(max(0.1, v), 3) for v in values]


def publish_records(
    bootstrap_servers: str,
    duration_sec: int,
    datasets: list[str],
    records_per_dataset: Dict[str, int],
    input_rates: Dict[str, int],
    priorities: Dict[str, str],
    payload_bytes: Dict[str, int],
    bids_topic: str,
    input_topic_template: str,
    tenant_ids: Dict[str, str],
    dataset_root: Path,
    allow_download: bool,
    enable_synthetic_fallback: bool,
    subset_lines: int,
    criteo_subset_lines: int,
    replay_window_compression: float,
    state_size_min_gb: float,
    state_size_max_gb: float,
    state_size_avg_gb: float,
    loop_sleep_ms: int,
    dry_run: bool,
) -> None:
    logger = logging.getLogger("streambazaar.workloads")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s | %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    generators = build_generators(
        datasets=datasets,
        tenant_ids=tenant_ids,
        dataset_root=dataset_root,
        allow_download=allow_download,
        enable_synthetic_fallback=enable_synthetic_fallback,
        subset_lines=subset_lines,
        criteo_subset_lines=criteo_subset_lines,
        replay_window_compression=replay_window_compression,
        logger=logger,
    )

    if dry_run:
        print(
            json.dumps(
                {
                    "status": "dry-run-ok",
                    "datasets": datasets,
                    "tenant_ids": tenant_ids,
                    "dataset_root": str(dataset_root),
                    "synthetic_fallback_enabled": enable_synthetic_fallback,
                    "downloads_enabled": allow_download,
                },
                indent=2,
            )
        )
        return

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
        retries=3,
        acks="all",
    )

    sent = {tenant_ids[d]: 0 for d in datasets}
    bytes_sent = {tenant_ids[d]: 0 for d in datasets}
    sent_per_dataset = {d: 0 for d in datasets}
    deadline = time.time() + duration_sec
    next_emit_ts = {d: time.perf_counter() for d in datasets}

    while time.time() < deadline:
        did_publish = False
        for dataset, generator in generators.items():
            tenant_id = tenant_ids[dataset]
            if sent[tenant_id] >= records_per_dataset[dataset]:
                continue

            now_perf = time.perf_counter()
            rate = max(1, int(input_rates[dataset]))
            interval = 1.0 / rate
            if now_perf < next_emit_ts[dataset]:
                continue

            event = next(generator)
            now = time.time()
            event["tenant_id"] = tenant_id
            event["tenantId"] = tenant_id
            event["record_id"] = event.get("record_id", event.get("event_id", event.get("request_id", f"rec-{int(now*1000)}")))
            event["recordId"] = str(event["record_id"])
            event["timestamp"] = now
            event["ingest_ts_ns"] = time.time_ns()
            event["workload"] = event.get("workload", dataset.replace("-", "_"))
            event["dataset"] = dataset
            event["priority"] = priorities[dataset]
            state_sizes_gb = compute_state_sizes(
                operator_count=int(event.get("operator_count", 1)),
                min_gb=state_size_min_gb,
                max_gb=state_size_max_gb,
                avg_gb=state_size_avg_gb,
            )
            event["state_sizes_gb"] = state_sizes_gb
            event["state_size_avg_gb"] = round(sum(state_sizes_gb) / max(len(state_sizes_gb), 1), 4)
            event["payload_bytes"] = payload_bytes[dataset]

            extra_bytes = payload_bytes[dataset]
            if extra_bytes > 0:
                # Optional payload padding to control message size in benchmarks.
                event["payload"] = "x" * extra_bytes

            encoded = json.dumps(event).encode("utf-8")
            topic_in = input_topic_template.format(tenant_id=tenant_id, dataset=dataset)

            producer.send(bids_topic, key=tenant_id, value=event)
            producer.send(topic_in, key=tenant_id, value=event)
            sent[tenant_id] += 1
            sent_per_dataset[dataset] += 1
            bytes_sent[tenant_id] += len(encoded)
            next_emit_ts[dataset] = now_perf + interval
            did_publish = True

        if all(sent_per_dataset[d] >= records_per_dataset[d] for d in datasets):
            break

        if loop_sleep_ms > 0:
            time.sleep(loop_sleep_ms / 1000.0)
        elif not did_publish:
            next_due = min(next_emit_ts.values())
            sleep_s = max(0.0, min(0.005, next_due - time.perf_counter()))
            if sleep_s > 0:
                time.sleep(sleep_s)

    producer.flush(timeout=60)
    producer.close(timeout=60)
    print(
        json.dumps(
            {
                "status": "ok",
                "datasets": datasets,
                "tenant_ids": tenant_ids,
                "topics": {
                    "bids": bids_topic,
                    "input_template": input_topic_template,
                    "resolved_input_topics": {
                        d: input_topic_template.format(tenant_id=tenant_ids[d], dataset=d) for d in datasets
                    },
                },
                "input_rates_per_dataset_hz": input_rates,
                "priorities": priorities,
                "payload_bytes_per_dataset": payload_bytes,
                "records_target_per_dataset": records_per_dataset,
                "sent_per_tenant": sent,
                "sent_per_dataset": sent_per_dataset,
                "bytes_sent_per_tenant": bytes_sent,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish real or synthetic workload events to Kafka topics")
    parser.add_argument("--bootstrap-servers", default="localhost:19092")
    parser.add_argument("--duration-sec", type=int, default=20)
    parser.add_argument("--datasets", default=DEFAULT_DATASETS, help="Comma-separated dataset list")
    parser.add_argument(
        "--tenant-ids",
        default="",
        help="Optional comma-separated tenant ids aligned to --datasets order. Example: tenant-a,tenant-b",
    )
    parser.add_argument("--records-per-tenant", type=int, default=30, help="Default records target for each dataset")
    parser.add_argument(
        "--records-per-dataset",
        default="",
        help="Optional overrides. Example: fraud=80,web-analytics=120,network-intrusion=60",
    )
    parser.add_argument("--input-rate", type=int, default=1000, help="Default event rate per dataset (Hz)")
    parser.add_argument(
        "--input-rates",
        default="",
        help="Optional per-dataset rates. Example: fraud=120000,web-analytics=500000",
    )
    parser.add_argument("--payload-bytes", type=int, default=0, help="Default extra payload bytes per message")
    parser.add_argument(
        "--payload-bytes-map",
        default="",
        help="Optional per-dataset payload bytes. Example: fraud=256,web-analytics=1024",
    )
    parser.add_argument("--bids-topic", default="streamBazaar.bids")
    parser.add_argument("--input-topic-template", default="tenant.{tenant_id}.input")
    parser.add_argument("--dataset-root", default="", help="Dataset root directory (default: streamBazaar/datasets)")
    parser.add_argument("--subset-lines", type=int, default=0, help="Optional line cap per dataset for quick runs")
    parser.add_argument(
        "--criteo-subset-lines",
        type=int,
        default=500000,
        help="Subset lines for Criteo dataset handling due to large size",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip dataset download attempts and only use local files",
    )
    parser.add_argument(
        "--disable-synthetic-fallback",
        action="store_true",
        help="Fail if a dataset is unavailable instead of using synthetic fallback",
    )
    parser.add_argument(
        "--compress-time-window",
        type=float,
        default=10.0,
        help="Replay window compression factor to increase backpressure",
    )
    parser.add_argument(
        "--priorities",
        default="",
        help="Optional per-dataset priority override: fraud=high,web-analytics=low",
    )
    parser.add_argument("--state-size-min-gb", type=float, default=0.1)
    parser.add_argument("--state-size-max-gb", type=float, default=10.0)
    parser.add_argument("--state-size-avg-gb", type=float, default=1.0)
    parser.add_argument(
        "--loop-sleep-ms",
        type=int,
        default=0,
        help="Extra loop sleep to reduce burstiness; set >0 for lower publish frequency",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate dataset readiness and workload wiring without connecting to Kafka",
    )
    args = parser.parse_args()

    datasets_raw = parse_csv_list(args.datasets)
    datasets = [normalize_dataset_key(d) for d in datasets_raw]
    if not datasets:
        raise ValueError("--datasets must include at least one dataset")

    tenant_ids = build_tenant_ids(datasets=datasets, tenant_ids_raw=args.tenant_ids)

    rates_override = parse_kv_int(args.input_rates)
    records_override = parse_kv_int(args.records_per_dataset)
    payload_override = parse_kv_int(args.payload_bytes_map)

    input_rates = {d: int(rates_override.get(d, DEFAULT_INPUT_RATES.get(d, args.input_rate))) for d in datasets}
    records_per_dataset = {d: int(records_override.get(d, args.records_per_tenant)) for d in datasets}
    payload_bytes = {d: int(payload_override.get(d, args.payload_bytes)) for d in datasets}
    priorities = {d: DEFAULT_PRIORITIES.get(d, "low") for d in datasets}
    priority_override = {normalize_dataset_key(k): v for k, v in parse_kv_str(args.priorities).items()}
    priorities.update(priority_override)
    dataset_root = Path(args.dataset_root) if args.dataset_root else BASE_DIR / "datasets"

    publish_records(
        bootstrap_servers=args.bootstrap_servers,
        duration_sec=args.duration_sec,
        datasets=datasets,
        records_per_dataset=records_per_dataset,
        input_rates=input_rates,
        priorities=priorities,
        payload_bytes=payload_bytes,
        bids_topic=args.bids_topic,
        input_topic_template=args.input_topic_template,
        tenant_ids=tenant_ids,
        dataset_root=dataset_root,
        allow_download=not args.skip_download,
        enable_synthetic_fallback=not args.disable_synthetic_fallback,
        subset_lines=max(0, args.subset_lines),
        criteo_subset_lines=max(0, args.criteo_subset_lines),
        replay_window_compression=max(1.0, args.compress_time_window),
        state_size_min_gb=max(0.1, args.state_size_min_gb),
        state_size_max_gb=max(0.1, args.state_size_max_gb),
        state_size_avg_gb=max(0.1, args.state_size_avg_gb),
        loop_sleep_ms=args.loop_sleep_ms,
        dry_run=args.dry_run,
    )
