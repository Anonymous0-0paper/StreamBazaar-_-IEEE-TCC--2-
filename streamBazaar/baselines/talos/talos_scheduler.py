from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Dict, Mapping

from .bottleneck_detector import is_backpressured, is_bottleneck


@dataclass
class ScalingDecision:
    task_id: str
    action: str
    from_parallelism: int
    to_parallelism: int
    reason: str


class TALOSScheduler:
    """Task-level TALOS scheduler baseline (reactive autoscaling)."""

    def __init__(self, cooldown_period: int = 90, idle_threshold: int = 500):
        self.cooldown_period = cooldown_period
        self.idle_threshold = idle_threshold
        self.last_scaling_time: Dict[str, float] = {}

    def _in_cooldown(self, task_id: str, now: float) -> bool:
        last = self.last_scaling_time.get(task_id)
        if last is None:
            return False
        return (now - last) < self.cooldown_period

    def scale_tasks(self, flink_metrics: Mapping[str, Mapping[str, float]]) -> Dict[str, ScalingDecision]:
        """Apply TALOS Algorithm 1 for each task.

        Expected per-task fields:
        - is_source, parallelism, total_lag/lag_change_rate/relative_lag_change_rate
        - throughput, in_pool_usage, out_pool_usage, backpressure_ms, idle_time_ms
        """

        now = time.time()
        decisions: Dict[str, ScalingDecision] = {}

        for task_id, metrics in flink_metrics.items():
            if self._in_cooldown(task_id, now):
                continue

            current_parallelism = max(1, int(metrics.get("parallelism", 1)))
            is_source = bool(metrics.get("is_source", False))

            if is_source and is_backpressured(metrics):
                continue

            lag_change_rate = float(
                metrics.get(
                    "lag_change_rate",
                    metrics.get("relative_lag_change_rate", 0.0),
                )
            )
            idle_time = float(metrics.get("idle_time_ms", 0.0))
            busy_time = float(metrics.get("busy_time_ms", 0.0))
            busy_threshold = float(metrics.get("busy_threshold_ms", 500.0))

            if lag_change_rate > 0 and busy_time >= busy_threshold and is_bottleneck(metrics):
                target_parallelism = int(math.ceil(current_parallelism * (lag_change_rate + 1.0)))
                if target_parallelism > current_parallelism:
                    decisions[task_id] = ScalingDecision(
                        task_id=task_id,
                        action="scale_up",
                        from_parallelism=current_parallelism,
                        to_parallelism=target_parallelism,
                        reason="lag growth with bottleneck signature",
                    )
                    self.last_scaling_time[task_id] = now
                continue

            if lag_change_rate < 0 and idle_time >= self.idle_threshold and current_parallelism > 1:
                target_parallelism = current_parallelism - 1
                decisions[task_id] = ScalingDecision(
                    task_id=task_id,
                    action="scale_down",
                    from_parallelism=current_parallelism,
                    to_parallelism=target_parallelism,
                    reason="negative lag trend and sustained idleness",
                )
                self.last_scaling_time[task_id] = now

        return decisions
