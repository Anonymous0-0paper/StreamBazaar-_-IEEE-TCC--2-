import os
from typing import Dict

import numpy as np
from influxdb_client import InfluxDBClient


class PercentileCalculator:
    def __init__(self) -> None:
        self.client = InfluxDBClient(
            url=os.getenv("INFLUXDB_URL", "http://localhost:18086"),
            token="streamBazaar-token",
            org="evaluation",
        )

    def calculate_latency_percentiles(self, tenant_id: str, time_window: str = "5m") -> Dict[str, float]:
        query = f'''
        from(bucket: "streamBazaar")
        |> range(start: -{time_window})
        |> filter(fn: (r) => r["_measurement"] == "operator_latency")
        |> filter(fn: (r) => r["tenant_id"] == "{tenant_id}")
        |> filter(fn: (r) => r["_field"] == "latency_ms")
        '''

        result = self.client.query_api().query(query)
        latencies = [record.get_value() for table in result for record in table.records]
        if not latencies:
            return {}

        return {
            "p50": float(np.percentile(latencies, 50)),
            "p90": float(np.percentile(latencies, 90)),
            "p95": float(np.percentile(latencies, 95)),
            "p99": float(np.percentile(latencies, 99)),
            "p99_9": float(np.percentile(latencies, 99.9)),
            "mean": float(np.mean(latencies)),
            "max": float(np.max(latencies)),
        }

    def calculate_throughput_metrics(self, tenant_id: str, time_window: str = "5m") -> Dict[str, float]:
        query = f'''
        from(bucket: "streamBazaar")
        |> range(start: -{time_window})
        |> filter(fn: (r) => r["_measurement"] == "throughput")
        |> filter(fn: (r) => r["tenant_id"] == "{tenant_id}")
        |> filter(fn: (r) => r["_field"] == "records_per_sec")
        '''
        result = self.client.query_api().query(query)
        throughputs = [record.get_value() for table in result for record in table.records]
        if not throughputs:
            return {}

        return {
            "avg_throughput": float(np.mean(throughputs)),
            "max_throughput": float(np.max(throughputs)),
            "min_throughput": float(np.min(throughputs)),
            "throughput_variance": float(np.var(throughputs)),
        }

    def calculate_resource_metrics(self, tenant_id: str, time_window: str = "5m") -> Dict[str, float]:
        cpu_query = f'''
        from(bucket: "streamBazaar")
        |> range(start: -{time_window})
        |> filter(fn: (r) => r["_measurement"] == "resource_utilization")
        |> filter(fn: (r) => r["tenant_id"] == "{tenant_id}")
        |> filter(fn: (r) => r["_field"] == "cpu_percent")
        '''
        mem_query = f'''
        from(bucket: "streamBazaar")
        |> range(start: -{time_window})
        |> filter(fn: (r) => r["_measurement"] == "resource_utilization")
        |> filter(fn: (r) => r["tenant_id"] == "{tenant_id}")
        |> filter(fn: (r) => r["_field"] == "memory_percent")
        '''
        net_query = f'''
        from(bucket: "streamBazaar")
        |> range(start: -{time_window})
        |> filter(fn: (r) => r["_measurement"] == "resource_utilization")
        |> filter(fn: (r) => r["tenant_id"] == "{tenant_id}")
        |> filter(fn: (r) => r["_field"] == "network_percent")
        '''

        cpu_result = self.client.query_api().query(cpu_query)
        mem_result = self.client.query_api().query(mem_query)
        net_result = self.client.query_api().query(net_query)

        cpu_values = [record.get_value() for table in cpu_result for record in table.records]
        mem_values = [record.get_value() for table in mem_result for record in table.records]
        net_values = [record.get_value() for table in net_result for record in table.records]

        if not cpu_values and not mem_values and not net_values:
            return {}

        cpu_mean = float(np.mean(cpu_values)) if cpu_values else 0.0
        mem_mean = float(np.mean(mem_values)) if mem_values else 0.0
        net_mean = float(np.mean(net_values)) if net_values else 0.0
        cpu_peak = float(np.max(cpu_values)) if cpu_values else 0.0
        mem_peak = float(np.max(mem_values)) if mem_values else 0.0
        net_peak = float(np.max(net_values)) if net_values else 0.0

        return {
            "cpu_percent": cpu_mean,
            "memory_percent": mem_mean,
            "network_percent": net_mean,
            "cpu_peak": cpu_peak,
            "memory_peak": mem_peak,
            "network_peak": net_peak,
        }
