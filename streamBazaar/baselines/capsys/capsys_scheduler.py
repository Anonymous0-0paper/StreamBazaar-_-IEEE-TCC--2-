from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping


@dataclass
class CAPSysDecision:
    task_id: str
    action: str
    from_parallelism: int
    to_parallelism: int
    contention_score: float


class CAPSysScheduler:
    """Contention-aware placement and scaling heuristic."""

    def __init__(self, contention_threshold: float = 0.75, max_step: int = 2) -> None:
        self.contention_threshold = contention_threshold
        self.max_step = max(1, max_step)

    def scale_tasks(self, flink_metrics: Mapping[str, Mapping[str, float]]) -> Dict[str, CAPSysDecision]:
        decisions: Dict[str, CAPSysDecision] = {}

        for task_id, metrics in flink_metrics.items():
            current_parallelism = max(1, int(metrics.get("parallelism", 1)))
            in_pool = float(metrics.get("in_pool_usage", 0.0))
            out_pool = float(metrics.get("out_pool_usage", 0.0))
            backpressure = float(metrics.get("backpressure_ms", 0.0))
            lag_growth = float(metrics.get("relative_lag_change_rate", metrics.get("lag_change_rate", 0.0)))

            contention_score = (0.35 * in_pool) + (0.25 * out_pool) + (0.25 * min(1.0, backpressure / 1000.0)) + (0.15 * max(0.0, lag_growth))

            if contention_score >= self.contention_threshold:
                to_parallelism = current_parallelism + self.max_step
                decisions[task_id] = CAPSysDecision(
                    task_id=task_id,
                    action="rebalance_up",
                    from_parallelism=current_parallelism,
                    to_parallelism=to_parallelism,
                    contention_score=contention_score,
                )
            elif contention_score < (self.contention_threshold * 0.4) and current_parallelism > 1:
                to_parallelism = max(1, current_parallelism - 1)
                decisions[task_id] = CAPSysDecision(
                    task_id=task_id,
                    action="rebalance_down",
                    from_parallelism=current_parallelism,
                    to_parallelism=to_parallelism,
                    contention_score=contention_score,
                )

        return decisions
