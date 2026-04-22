from __future__ import annotations

from typing import Dict, Mapping, Optional

from baselines.comparison_system import BaselineComparisonSystem, BaselineSnapshot
from evaluation.export_prometheus_csv import BASE_QUERIES, build_tenant_queries, instant_query


class BaselineComparison:
    """End-to-end comparison wrapper for StreamBazaar vs TALOS/DS2/Flink Default."""

    def __init__(self, prometheus_url: str = "http://localhost:19090") -> None:
        self.system = BaselineComparisonSystem()
        self.prometheus_url = prometheus_url

    def gather_all_metrics(self, tenant_ids: list[str]) -> Dict[str, float]:
        """Collect complete metric set used by CSV exporter for baseline reporting."""

        query_map: Dict[str, str] = dict(BASE_QUERIES)
        query_map.update(build_tenant_queries(tenant_ids))

        # Add extra counters often used for audit/debug analysis.
        query_map.update(
            {
                "messages_in_total": "streambazaar_messages_in_total",
                "messages_out_total": "streambazaar_messages_out_total",
                "clearing_cycles_total": "streambazaar_clearing_cycles_total",
                "tenant_backlog_total": "sum(streambazaar_tenant_backlog)",
                "tenant_bid_avg": "avg(streambazaar_tenant_last_bid)",
            }
        )

        return {
            metric_name: instant_query(self.prometheus_url, promql)
            for metric_name, promql in query_map.items()
        }

    @staticmethod
    def _build_snapshot_from_live_metrics(
        metrics: Mapping[str, float],
        tenant_ids: list[str],
    ) -> BaselineSnapshot:
        tenant_tp: Dict[str, float] = {}
        for tenant in tenant_ids:
            safe = tenant.replace("-", "_")
            tenant_tp[tenant] = float(metrics.get(f"throughput_{safe}_total", 0.0))

        total_tp = sum(tenant_tp.values())
        avg_p99 = 0.0
        if tenant_ids:
            avg_p99 = sum(float(metrics.get(f"latency_{t.replace('-', '_')}_p99_ms", 0.0)) for t in tenant_ids) / len(tenant_ids)

        operator_metrics = {
            "source": {
                "is_source": 1.0,
                "parallelism": 2,
                "lag_change_rate": max(0.0, float(metrics.get("tenant_backlog_total", 0.0)) / max(total_tp, 1e-9)),
                "in_pool_usage": float(metrics.get("checkpoint_cpu_cluster", 0.0)) / 100.0,
                "out_pool_usage": float(metrics.get("checkpoint_network_cluster", 0.0)) / 100.0,
                "backpressure_ms": float(metrics.get("tlvr_cluster", 0.0)) * 1000.0,
                "busy_time_ms": 800.0,
                "idle_time_ms": max(0.0, 1000.0 - float(metrics.get("checkpoint_cpu_cluster", 0.0)) * 10.0),
                "actual_processing_time": 0.002,
                "backpressure_wait_time": 0.0005,
                "input_rate": max(total_tp, 1.0),
                "required_throughput": max(total_tp * 1.1, 1.0),
                "utilization": float(metrics.get("checkpoint_cpu_cluster", 0.0)) / 100.0,
                "p99_latency_ms": max(avg_p99, 1.0),
            },
            "map": {
                "is_source": 0.0,
                "parallelism": 3,
                "relative_lag_change_rate": max(0.0, float(metrics.get("tenant_backlog_total", 0.0)) / max(total_tp, 1e-9)) * 0.5,
                "in_pool_usage": float(metrics.get("checkpoint_memory_cluster", 0.0)) / 100.0,
                "out_pool_usage": float(metrics.get("checkpoint_network_cluster", 0.0)) / 100.0,
                "backpressure_ms": float(metrics.get("tlvr_cluster", 0.0)) * 800.0,
                "busy_time_ms": 700.0,
                "idle_time_ms": 220.0,
                "actual_processing_time": 0.0018,
                "backpressure_wait_time": 0.0004,
                "input_rate": max(total_tp * 0.95, 1.0),
                "required_throughput": max(total_tp * 1.05, 1.0),
                "utilization": float(metrics.get("checkpoint_memory_cluster", 0.0)) / 100.0,
                "p99_latency_ms": max(avg_p99 * 0.9, 1.0),
            },
            "sink": {
                "is_source": 0.0,
                "parallelism": 2,
                "relative_lag_change_rate": -0.05,
                "in_pool_usage": 0.25,
                "out_pool_usage": 0.2,
                "backpressure_ms": float(metrics.get("tlvr_cluster", 0.0)) * 400.0,
                "busy_time_ms": 350.0,
                "idle_time_ms": 750.0,
                "actual_processing_time": 0.0012,
                "backpressure_wait_time": 0.0001,
                "input_rate": max(total_tp * 0.9, 1.0),
                "required_throughput": max(total_tp * 0.9, 1.0),
                "utilization": 0.3,
                "p99_latency_ms": max(avg_p99 * 0.8, 1.0),
            },
        }

        required_throughput = {
            op: float(vals.get("required_throughput", 0.0)) for op, vals in operator_metrics.items()
        }

        return BaselineSnapshot(
            operator_metrics=operator_metrics,
            required_throughput=required_throughput,
            tenant_throughputs=tenant_tp,
        )

    def run_comparison(
        self,
        operator_metrics: Optional[Mapping[str, Mapping[str, float]]] = None,
        required_throughput: Optional[Mapping[str, float]] = None,
        tenant_throughputs: Optional[Mapping[str, float]] = None,
        tenant_ids: Optional[list[str]] = None,
        gather_live_metrics: bool = True,
    ) -> Dict[str, object]:
        """Run baseline comparison on a snapshot.

        If no inputs are provided, a representative sample snapshot is used.
        """

        if tenant_ids is None:
            tenant_ids = ["tenant-fraud", "tenant-clickstream", "tenant-ml"]

        gathered_metrics: Dict[str, float] = {}
        if gather_live_metrics:
            gathered_metrics = self.gather_all_metrics(tenant_ids)

        if operator_metrics is None and gathered_metrics:
            snapshot = self._build_snapshot_from_live_metrics(gathered_metrics, tenant_ids)
            return self.system.compare(snapshot, gathered_metrics)

        if operator_metrics is None:
            operator_metrics = {
                "source": {
                    "is_source": 1.0,
                    "parallelism": 2,
                    "lag_change_rate": 0.28,
                    "in_pool_usage": 0.71,
                    "out_pool_usage": 0.25,
                    "backpressure_ms": 620,
                    "busy_time_ms": 820,
                    "idle_time_ms": 120,
                    "actual_processing_time": 0.0022,
                    "backpressure_wait_time": 0.0005,
                    "input_rate": 9000,
                    "required_throughput": 11000,
                    "utilization": 0.82,
                    "p99_latency_ms": 210,
                },
                "map": {
                    "is_source": 0.0,
                    "parallelism": 3,
                    "relative_lag_change_rate": 0.15,
                    "in_pool_usage": 0.64,
                    "out_pool_usage": 0.31,
                    "backpressure_ms": 510,
                    "busy_time_ms": 790,
                    "idle_time_ms": 190,
                    "actual_processing_time": 0.0018,
                    "backpressure_wait_time": 0.0004,
                    "input_rate": 12000,
                    "required_throughput": 14000,
                    "utilization": 0.74,
                    "p99_latency_ms": 185,
                },
                "sink": {
                    "is_source": 0.0,
                    "parallelism": 2,
                    "relative_lag_change_rate": -0.08,
                    "in_pool_usage": 0.22,
                    "out_pool_usage": 0.18,
                    "backpressure_ms": 120,
                    "busy_time_ms": 320,
                    "idle_time_ms": 780,
                    "actual_processing_time": 0.0013,
                    "backpressure_wait_time": 0.0001,
                    "input_rate": 8700,
                    "required_throughput": 8600,
                    "utilization": 0.28,
                    "p99_latency_ms": 130,
                },
            }

        if required_throughput is None:
            required_throughput = {
                op: float(metrics.get("required_throughput", 0.0))
                for op, metrics in operator_metrics.items()
            }

        if tenant_throughputs is None:
            tenant_throughputs = {
                "tenant-fraud": 4200.0,
                "tenant-clickstream": 4600.0,
                "tenant-iot": 3500.0,
            }

        snapshot = BaselineSnapshot(
            operator_metrics={k: dict(v) for k, v in operator_metrics.items()},
            required_throughput=dict(required_throughput),
            tenant_throughputs=dict(tenant_throughputs),
        )
        return self.system.compare(snapshot, gathered_metrics)
