import time
from dataclasses import dataclass
from typing import Dict, Iterator, List


@dataclass
class WorkloadMetadata:
    name: str
    dataset: str
    operator_count: int
    priority: str
    operators: List[str]
    state_sizes_gb: List[float]


class BaseWorkloadGenerator:
    def __init__(
        self,
        tenant_id: str,
        replay_window_compression: float,
        metadata: WorkloadMetadata,
        record_source: Iterator[Dict],
    ) -> None:
        self.tenant_id = tenant_id
        self.replay_window_compression = max(1.0, replay_window_compression)
        self.metadata = metadata
        self.record_source = record_source

    def generate_workload(self) -> Iterator[Dict]:
        for event in self.record_source:
            transformed = self._run_pipeline(event)
            transformed["tenant_id"] = self.tenant_id
            transformed["tenantId"] = self.tenant_id
            transformed["workload"] = self.metadata.name
            transformed["dataset"] = self.metadata.dataset
            transformed["priority"] = self.metadata.priority
            transformed["operator_count"] = self.metadata.operator_count
            transformed["operators"] = self.metadata.operators
            transformed["state_sizes_gb"] = self.metadata.state_sizes_gb
            ingest_ts_ns = int(transformed.get("ingest_ts_ns", time.time_ns()))
            transformed["ingest_ts_ns"] = ingest_ts_ns
            transformed["timestamp"] = transformed.get("timestamp", ingest_ts_ns / 1_000_000_000.0)
            transformed["event_time"] = transformed.get("event_time", transformed["timestamp"])
            yield transformed

    def _run_pipeline(self, event: Dict) -> Dict:
        raise NotImplementedError


def default_state_sizes(operator_count: int) -> List[float]:
    baseline = [0.1, 0.25, 0.5, 0.75, 1.0, 1.0, 1.25, 1.5, 2.0, 3.0, 10.0]
    if operator_count <= len(baseline):
        values = baseline[:operator_count]
    else:
        values = baseline + [1.0] * (operator_count - len(baseline))
    return values
