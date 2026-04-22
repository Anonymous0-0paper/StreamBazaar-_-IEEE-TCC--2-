import asyncio
import os
import time
from dataclasses import dataclass
from typing import Dict, List

import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


@dataclass
class LatencyMeasurement:
    tenant_id: str
    operator_id: str
    timestamp: float
    latency_ms: float
    record_id: str


class MetricsCollector:
    def __init__(self) -> None:
        self.influx_client = InfluxDBClient(
            url=os.getenv("INFLUXDB_URL", "http://localhost:18086"),
            token="streamBazaar-token",
            org="evaluation",
        )
        self.prom_url = os.getenv("PROMETHEUS_URL", "http://localhost:19090")
        tenants_raw = os.getenv("TENANT_IDS", "tenant-fraud,tenant-clickstream,tenant-ml")
        self.tenants = [t.strip() for t in tenants_raw.split(",") if t.strip()]
        # Synchronous writes keep evaluation scripts deterministic.
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)

    def _prom_query(self, expr: str) -> float:
        try:
            response = requests.get(
                f"{self.prom_url.rstrip('/')}/api/v1/query",
                params={"query": expr},
                timeout=4,
            )
            response.raise_for_status()
            payload = response.json()
            result = payload.get("data", {}).get("result", [])
            if not result:
                return 0.0
            return float(result[0]["value"][1])
        except Exception:
            return 0.0

    async def collect_latency_metrics(self) -> None:
        while True:
            measurements = await self.get_latency_measurements()
            await self.store_latency_metrics(measurements)
            await asyncio.sleep(1.0)

    async def collect_throughput_metrics(self) -> None:
        while True:
            throughput_data = await self.get_throughput_data()
            await self.store_throughput_metrics(throughput_data)
            await asyncio.sleep(1.0)

    async def collect_resource_metrics(self) -> None:
        while True:
            resource_data = await self.get_resource_utilization()
            await self.store_resource_metrics(resource_data)
            await asyncio.sleep(1.0)

    async def get_latency_measurements(self) -> List[LatencyMeasurement]:
        now = time.time()
        measurements: List[LatencyMeasurement] = []
        for tenant_id in self.tenants:
            p99 = self._prom_query(f'streambazaar_latency_p99_ms{{tenant_id="{tenant_id}"}}')
            if p99 <= 0:
                continue
            measurements.append(
                LatencyMeasurement(
                    tenant_id=tenant_id,
                    operator_id="stream-coordinator",
                    timestamp=now,
                    latency_ms=p99,
                    record_id=f"real-{int(now * 1000)}-{tenant_id}",
                )
            )
        return measurements

    async def store_latency_metrics(self, measurements: List[LatencyMeasurement]) -> None:
        for m in measurements:
            point = (
                Point("operator_latency")
                .tag("tenant_id", m.tenant_id)
                .tag("operator_id", m.operator_id)
                .field("latency_ms", m.latency_ms)
                .field("record_id", m.record_id)
                .time(int(m.timestamp * 1000), WritePrecision.MS)
            )
            self.write_api.write(bucket="streamBazaar", org="evaluation", record=point)

    async def get_throughput_data(self) -> Dict[str, float]:
        data: Dict[str, float] = {}
        for tenant_id in self.tenants:
            value = self._prom_query(
                f'streambazaar_throughput_msgs_per_sec{{tenant_id="{tenant_id}",direction="total"}}'
            )
            data[tenant_id] = max(0.0, value)
        return data

    async def store_throughput_metrics(self, throughput_data: Dict[str, float]) -> None:
        for tenant_id, value in throughput_data.items():
            point = Point("throughput").tag("tenant_id", tenant_id).field("records_per_sec", value)
            self.write_api.write(bucket="streamBazaar", org="evaluation", record=point)

    async def get_resource_utilization(self) -> Dict[str, Dict[str, float]]:
        out: Dict[str, Dict[str, float]] = {}
        for tenant_id in self.tenants:
            cpu = self._prom_query(
                f'streambazaar_checkpoint_cpu_utilization_percent{{scope="tenant",tenant_id="{tenant_id}"}}'
            )
            mem = self._prom_query(
                f'streambazaar_checkpoint_memory_utilization_percent{{scope="tenant",tenant_id="{tenant_id}"}}'
            )
            net = self._prom_query(
                f'streambazaar_checkpoint_network_utilization_percent{{scope="tenant",tenant_id="{tenant_id}"}}'
            )
            out[tenant_id] = {
                "cpu_percent": max(0.0, cpu),
                "memory_percent": max(0.0, mem),
                "network_percent": max(0.0, net),
            }
        return out

    async def store_resource_metrics(self, resource_data: Dict[str, Dict[str, float]]) -> None:
        for tenant_id, vals in resource_data.items():
            point = (
                Point("resource_utilization")
                .tag("tenant_id", tenant_id)
                .field("cpu_percent", vals.get("cpu_percent", 0.0))
                .field("memory_percent", vals.get("memory_percent", 0.0))
                .field("network_percent", vals.get("network_percent", 0.0))
            )
            self.write_api.write(bucket="streamBazaar", org="evaluation", record=point)
