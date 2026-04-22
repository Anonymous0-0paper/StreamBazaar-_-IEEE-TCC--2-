from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Dict, Mapping

from .processing_estimator import estimate_true_processing_time, identify_bottlenecks


@dataclass
class DS2Decision:
    operator_id: str
    from_parallelism: int
    to_parallelism: int
    reason: str


class DS2Scheduler:
    def __init__(self, max_scaling_steps: int = 3, stability_period: int = 120):
        self.max_scaling_steps = max_scaling_steps
        self.stability_period = stability_period
        self.last_scaling_time: Dict[str, float] = {}

    def _resource_cap(self, operator_id: str, current_metrics: Mapping[str, Mapping[str, float]]) -> int:
        return int(current_metrics.get(operator_id, {}).get("max_parallelism", 1024))

    def three_step_scaling(
        self,
        dataflow_graph: Mapping[str, object],
        current_metrics: Mapping[str, Mapping[str, float]],
    ) -> Dict[str, DS2Decision]:
        """DS2 three-step scaling logic.

        Step1: model true processing time and capacity
        Step2: detect bottlenecks and required parallelism
        Step3: apply scaling with max 3-step increases and conservative scale-down
        """

        now = time.time()
        required_throughput = dataflow_graph.get("required_throughput", {})
        decisions: Dict[str, DS2Decision] = {}

        modeled_ops: Dict[str, Dict[str, float]] = {}
        for operator_id, metrics in current_metrics.items():
            perf = estimate_true_processing_time(metrics)
            modeled = dict(metrics)
            modeled.update(perf)
            modeled_ops[operator_id] = modeled

        bottlenecks = identify_bottlenecks(modeled_ops, required_throughput)

        for operator_id, details in bottlenecks.items():
            metrics = modeled_ops[operator_id]
            current_p = max(1, int(metrics.get("parallelism", 1)))
            required_p = int(details["required_parallelism"])
            max_increase = min(required_p - current_p, self.max_scaling_steps)
            proposed = current_p + max(0, max_increase)

            capacity_limit = self._resource_cap(operator_id, current_metrics)
            target_p = min(proposed, capacity_limit)

            if target_p > current_p:
                decisions[operator_id] = DS2Decision(
                    operator_id=operator_id,
                    from_parallelism=current_p,
                    to_parallelism=target_p,
                    reason="capacity bottleneck under required throughput",
                )
                self.last_scaling_time[operator_id] = now

        for operator_id, metrics in modeled_ops.items():
            if operator_id in decisions:
                continue

            current_p = max(1, int(metrics.get("parallelism", 1)))
            utilization = float(metrics.get("utilization", 1.0))
            last_change = self.last_scaling_time.get(operator_id, 0.0)
            stable = (now - last_change) >= self.stability_period

            if current_p > 1 and utilization < 0.3 and stable:
                decisions[operator_id] = DS2Decision(
                    operator_id=operator_id,
                    from_parallelism=current_p,
                    to_parallelism=current_p - 1,
                    reason="underutilized and stable",
                )
                self.last_scaling_time[operator_id] = now

        return decisions
