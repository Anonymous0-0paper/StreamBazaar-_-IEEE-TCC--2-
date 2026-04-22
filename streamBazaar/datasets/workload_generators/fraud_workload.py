from typing import Dict, Iterator

from .base import BaseWorkloadGenerator, WorkloadMetadata, default_state_sizes


class FraudDetectionWorkloadGenerator(BaseWorkloadGenerator):
    def __init__(self, tenant_id: str, replay_window_compression: float, record_source: Iterator[Dict]) -> None:
        metadata = WorkloadMetadata(
            name="fraud_detection",
            dataset="fraud",
            operator_count=3,
            priority="high",
            operators=["transaction_parser", "feature_extractor", "ml_inference"],
            state_sizes_gb=default_state_sizes(3),
        )
        super().__init__(tenant_id, replay_window_compression, metadata, record_source)

    def _run_pipeline(self, event: Dict) -> Dict:
        amount = float(event.get("amount", 0.0))
        card1 = int(event.get("card1", 0) or 0)
        addr1 = int(event.get("addr1", 0) or 0)
        device_type = str(event.get("device_type", "unknown") or "unknown").lower()

        # 1) transaction_parser: basic sanitization and canonical fields.
        cleaned = dict(event)
        cleaned["amount"] = round(max(0.0, amount), 4)
        cleaned["card1"] = card1
        cleaned["addr1"] = addr1
        cleaned["device_type"] = device_type

        # 2) feature_extractor: derive risk-centric transaction features.
        features = dict(cleaned)
        features["feature_amount_log"] = round((features["amount"] + 1.0) ** 0.5, 6)
        features["feature_card_addr_hash"] = (card1 % 997) * 1000 + (addr1 % 97)
        features["feature_device_risk"] = 0.35 if device_type in {"mobile", "tablet"} else 0.15
        features["feature_high_value"] = int(features["amount"] >= 500.0)

        # 3) ml_inference: lightweight fraud scoring approximation.
        score = (
            min(0.65, features["amount"] / 1500.0)
            + 0.2 * float(features["feature_high_value"])
            + float(features["feature_device_risk"])
        )
        features["model_score"] = round(min(1.0, score), 4)
        features["predicted_fraud"] = int(features["model_score"] >= 0.72)
        features["operator_trace"] = ["transaction_parser", "feature_extractor", "ml_inference"]
        return features
