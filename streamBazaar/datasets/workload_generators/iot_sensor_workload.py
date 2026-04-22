from typing import Dict, Iterator

from .base import BaseWorkloadGenerator, WorkloadMetadata, default_state_sizes


class IoTSensorAnalyticsWorkloadGenerator(BaseWorkloadGenerator):
    def __init__(self, tenant_id: str, replay_window_compression: float, record_source: Iterator[Dict]) -> None:
        metadata = WorkloadMetadata(
            name="iot_sensor_analytics",
            dataset="iot-sensors",
            operator_count=11,
            priority="medium",
            operators=[
                "raw_reader",
                "schema_validator",
                "null_cleaner",
                "outlier_filter",
                "temperature_enrichment",
                "humidity_enrichment",
                "light_transform",
                "windowed_aggregator",
                "anomaly_scoring",
                "threshold_detector",
                "notification_sink",
            ],
            state_sizes_gb=default_state_sizes(11),
        )
        super().__init__(tenant_id, replay_window_compression, metadata, record_source)

    def _run_pipeline(self, event: Dict) -> Dict:
        # 1) raw_reader
        stage = dict(event)
        temp = float(stage.get("temperature", 0.0) or 0.0)
        humidity = float(stage.get("humidity", 0.0) or 0.0)
        light = float(stage.get("light", 0.0) or 0.0)
        voltage = float(stage.get("voltage", 0.0) or 0.0)

        # 2) schema_validator
        stage["valid_schema"] = int(all(k in stage for k in ["sensor_id", "temperature"]))

        # 3) null_cleaner
        stage["temperature"] = temp
        stage["humidity"] = humidity if humidity >= 0 else 0.0
        stage["light"] = max(0.0, light)

        # 4) outlier_filter
        stage["temperature_clamped"] = min(80.0, max(-20.0, stage["temperature"]))

        # 5) temperature_enrichment
        stage["thermal_stress"] = round((stage["temperature_clamped"] - 22.0) / 10.0, 6)

        # 6) humidity_enrichment
        stage["comfort_index"] = round(0.6 * stage["temperature_clamped"] + 0.4 * stage["humidity"], 6)

        # 7) light_transform
        stage["light_log"] = round((stage["light"] + 1.0) ** 0.5, 6)

        # 8) windowed_aggregator
        stage["window_seconds"] = max(1, int(30 / self.replay_window_compression))

        # 9) anomaly_scoring
        stage["sensor_health_score"] = round(max(0.0, 1.0 - abs(voltage - 2.7)), 6)
        stage["anomaly_score"] = round(
            abs(stage["temperature_clamped"] - 24.0) * 0.05
            + abs(stage["humidity"] - 50.0) * 0.01
            + max(0.0, 0.5 - stage["sensor_health_score"]),
            6,
        )

        # 10) threshold_detector
        stage["is_anomaly"] = int(stage["anomaly_score"] > 0.8)

        # 11) notification_sink
        stage["alert_type"] = "sensor_anomaly" if stage["is_anomaly"] else "normal"
        stage["operator_trace"] = [
            "raw_reader",
            "schema_validator",
            "null_cleaner",
            "outlier_filter",
            "temperature_enrichment",
            "humidity_enrichment",
            "light_transform",
            "windowed_aggregator",
            "anomaly_scoring",
            "threshold_detector",
            "notification_sink",
        ]
        return stage
