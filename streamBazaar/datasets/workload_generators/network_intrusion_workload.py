from typing import Dict, Iterator

from .base import BaseWorkloadGenerator, WorkloadMetadata, default_state_sizes


class NetworkIntrusionWorkloadGenerator(BaseWorkloadGenerator):
    def __init__(self, tenant_id: str, replay_window_compression: float, record_source: Iterator[Dict]) -> None:
        metadata = WorkloadMetadata(
            name="network_intrusion_detection",
            dataset="network-intrusion",
            operator_count=7,
            priority="high",
            operators=[
                "packet_parser",
                "flow_normalizer",
                "protocol_classifier",
                "feature_extractor",
                "windowed_detector",
                "anomaly_classifier",
                "alert_sink",
            ],
            state_sizes_gb=default_state_sizes(7),
        )
        super().__init__(tenant_id, replay_window_compression, metadata, record_source)

    def _run_pipeline(self, event: Dict) -> Dict:
        # 1) packet_parser
        stage = dict(event)
        src_bytes = int(stage.get("src_bytes", 0) or 0)
        dst_bytes = int(stage.get("dst_bytes", 0) or 0)
        stage["duration"] = float(stage.get("duration", 0.0) or 0.0)

        # 2) flow_normalizer
        stage["flow_volume"] = max(0, src_bytes + dst_bytes)
        stage["byte_ratio"] = round(src_bytes / max(dst_bytes, 1), 6)

        # 3) protocol_classifier
        stage["protocol_group"] = "trusted" if str(stage.get("proto", "")).lower() in {"tcp", "udp"} else "other"

        # 4) feature_extractor
        stage["bytes_per_second"] = round(stage["flow_volume"] / max(stage["duration"], 0.001), 6)
        stage["service_entropy_proxy"] = (len(str(stage.get("service", "-"))) % 7) / 7.0

        # 5) windowed_detector
        stage["window_attack_score"] = round(
            min(1.0, stage["byte_ratio"] / 10.0 + stage["bytes_per_second"] / 200000.0 + stage["service_entropy_proxy"]),
            6,
        )

        # 6) anomaly_classifier
        stage["predicted_attack"] = int(int(stage.get("label", 0)) == 1 or stage["window_attack_score"] >= 0.72)

        # 7) alert_sink
        stage["alert_severity"] = "critical" if stage["predicted_attack"] else "normal"
        stage["operator_trace"] = [
            "packet_parser",
            "flow_normalizer",
            "protocol_classifier",
            "feature_extractor",
            "windowed_detector",
            "anomaly_classifier",
            "alert_sink",
        ]
        return stage
