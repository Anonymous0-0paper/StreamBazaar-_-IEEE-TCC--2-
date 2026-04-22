"""Streaming workload generators backed by real/synthetic datasets."""

from .fraud_workload import FraudDetectionWorkloadGenerator
from .iot_sensor_workload import IoTSensorAnalyticsWorkloadGenerator
from .network_intrusion_workload import NetworkIntrusionWorkloadGenerator
from .web_analytics_workload import WebAnalyticsWorkloadGenerator

__all__ = [
    "FraudDetectionWorkloadGenerator",
    "WebAnalyticsWorkloadGenerator",
    "NetworkIntrusionWorkloadGenerator",
    "IoTSensorAnalyticsWorkloadGenerator",
]
