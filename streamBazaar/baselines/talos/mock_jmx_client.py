from __future__ import annotations

from typing import Any, Dict, List


class MockFlinkJmxClient:
    """Simple mock to test TALOS formula functions without a live cluster."""

    def __init__(self, task_metrics: Dict[str, Dict[str, Any]], source_partitions: Dict[str, List[Dict[str, float]]]):
        self._task_metrics = task_metrics
        self._source_partitions = source_partitions

    def get_task_metric(self, task_id: str, name: str, default: Any = None) -> Any:
        return self._task_metrics.get(task_id, {}).get(name, default)

    def get_source_partitions(self, task_id: str) -> List[Dict[str, float]]:
        return self._source_partitions.get(task_id, [])
