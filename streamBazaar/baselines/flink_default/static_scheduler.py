from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping

from .slot_allocator import allocate_slots_statically


@dataclass
class StaticScheduleResult:
    parallelism_config: Dict[str, int]
    assignments: Dict[str, object]


class FlinkDefaultScheduler:
    def __init__(self, fixed_parallelism_config: Mapping[str, int]):
        self.parallelism_config = dict(fixed_parallelism_config)

    def schedule_job(self, job_graph: Mapping[str, object]) -> StaticScheduleResult:
        """Static Flink scheduling with no dynamic scaling."""

        cluster_config = job_graph.get("cluster_config", {})
        operators = job_graph.get("operators", {})

        subtasks = []
        for operator_id, operator in operators.items():
            p = int(self.parallelism_config.get(operator_id, operator.get("parallelism", 1)))
            for i in range(p):
                subtasks.append(
                    {
                        "subtask_id": f"{operator_id}-{i}",
                        "job_id": job_graph.get("job_id", "job"),
                        "operator_id": operator_id,
                    }
                )

        job = {"subtasks": subtasks}
        assignments = allocate_slots_statically(job, cluster_config)

        return StaticScheduleResult(
            parallelism_config=dict(self.parallelism_config),
            assignments=assignments,
        )
