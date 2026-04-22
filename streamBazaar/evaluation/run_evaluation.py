import argparse
import asyncio
import importlib.util
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import os
import requests


class EvaluationRunner:
    def __init__(self) -> None:
        self.tenant_ids = ["tenant-fraud", "tenant-clickstream", "tenant-ml"]
        self.prometheus_url = os.getenv("PROMETHEUS_URL", "http://localhost:19090")
        self.metrics_collector_cls = self._load_symbol(
            Path("evaluation/metrics-collector/collector.py"),
            "MetricsCollector",
        )
        self.percentile_calculator_cls = self._load_symbol(
            Path("evaluation/latency-tracker/percentile_calculator.py"),
            "PercentileCalculator",
        )
        self.resource_monitor_cls = self._load_symbol(
            Path("evaluation/metrics-collector/resource_monitor.py"),
            "ResourceMonitor",
        )

    @staticmethod
    def _load_symbol(module_path: Path, symbol_name: str) -> Any:
        spec = importlib.util.spec_from_file_location(symbol_name, module_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Unable to load module from {module_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, symbol_name)

    async def _seed_metrics(self, collector: Any) -> None:
        # Ensure at least one sample exists, so very short runs still produce non-null reports.
        lat = await collector.get_latency_measurements()
        await collector.store_latency_metrics(lat)
        thr = await collector.get_throughput_data()
        await collector.store_throughput_metrics(thr)
        res = await collector.get_resource_utilization()
        await collector.store_resource_metrics(res)

    async def run_evaluation(self, duration_minutes: float = 60) -> None:
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)

        collector = self.metrics_collector_cls()
        percentile_calc = self.percentile_calculator_cls()
        resource_monitor = self.resource_monitor_cls()

        await self._seed_metrics(collector)
        tasks = [
            asyncio.create_task(collector.collect_latency_metrics()),
            asyncio.create_task(collector.collect_throughput_metrics()),
            asyncio.create_task(collector.collect_resource_metrics()),
        ]

        print(f"[evaluation] started at {start_time.isoformat()} for {duration_minutes} minutes")
        try:
            while datetime.now() < end_time:
                await asyncio.sleep(10)
                print(f"[evaluation] heartbeat {datetime.now().isoformat()}")
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        await self.generate_final_report(start_time, end_time, percentile_calc, resource_monitor)

    async def generate_final_report(self, start_time: datetime, end_time: datetime, percentile_calc: Any, resource_monitor: Any) -> None:
        report = {
            "timestamp": datetime.now().isoformat(),
            "window": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
            },
            "tenants": {},
            "cluster_metrics": {},
            "advanced_kpis": {},
        }

        for tenant_id in self.tenant_ids:
            latency = percentile_calc.calculate_latency_percentiles(tenant_id, time_window="30m")
            throughput = percentile_calc.calculate_throughput_metrics(tenant_id, time_window="30m")
            resource_usage = percentile_calc.calculate_resource_metrics(tenant_id, time_window="30m")
            report["tenants"][tenant_id] = {
                "latency": latency,
                "throughput": throughput,
                "resource_usage": resource_usage,
            }

        report["cluster_metrics"] = resource_monitor.get_cluster_resources()
        report["advanced_kpis"] = self.collect_advanced_kpis()

        output_file = Path(f"evaluation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        output_file.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"[evaluation] final report written to {output_file}")

    def _prom_query(self, expr: str) -> float:
        try:
            response = requests.get(
                f"{self.prometheus_url.rstrip('/')}/api/v1/query",
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

    def collect_advanced_kpis(self) -> dict[str, float]:
        return {
            "resource_utilization_efficiency": self._prom_query(
                'streambazaar_resource_utilization_efficiency{scope="cluster",tenant_id="all"}'
            ),
            "tail_latency_violation_rate": self._prom_query(
                'streambazaar_tail_latency_violation_rate{scope="cluster",tenant_id="all"}'
            ),
            "economic_efficiency_index": self._prom_query("streambazaar_economic_efficiency_index"),
            "fairness_performance_product": self._prom_query("streambazaar_fairness_performance_product"),
            "migration_impact_score": self._prom_query("streambazaar_migration_impact_score"),
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run StreamBazaar evaluation loop")
    parser.add_argument("--duration", type=float, default=60, help="Evaluation duration in minutes (supports fractional values)")
    args = parser.parse_args()

    runner = EvaluationRunner()
    asyncio.run(runner.run_evaluation(duration_minutes=args.duration))
