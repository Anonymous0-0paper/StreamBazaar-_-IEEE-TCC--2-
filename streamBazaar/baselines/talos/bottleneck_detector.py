from __future__ import annotations

from typing import Mapping


TaskMetrics = Mapping[str, float]


def is_bottleneck(task_metrics: TaskMetrics) -> bool:
    """TALOS bottleneck condition.

    True iff: (0.5 < inPoolUsage <= 1) AND (0.1 < outPoolUsage <= 0.5)
              AND (backpressure > 500)
    """

    in_pool = float(task_metrics.get("in_pool_usage", 0.0))
    out_pool = float(task_metrics.get("out_pool_usage", 0.0))
    backpressure = float(task_metrics.get("backpressure_ms", 0.0))
    return (0.5 < in_pool <= 1.0) and (0.1 < out_pool <= 0.5) and (backpressure > 500.0)


def is_backpressured(task_metrics: TaskMetrics) -> bool:
    """Return True when both input and output buffers are highly occupied."""

    in_pool = float(task_metrics.get("in_pool_usage", 0.0))
    out_pool = float(task_metrics.get("out_pool_usage", 0.0))
    return in_pool > 0.5 and out_pool > 0.5


def classify_backpressure_state(task_metrics: TaskMetrics) -> str:
    """Classify TALOS buffer-state interpretation.

    - both_full: backpressured by downstream
    - input_full_output_low: current task is bottleneck
    - input_low_output_full: being backpressured, not bottleneck
    - healthy: both low
    """

    in_pool = float(task_metrics.get("in_pool_usage", 0.0))
    out_pool = float(task_metrics.get("out_pool_usage", 0.0))

    if in_pool > 0.5 and out_pool > 0.5:
        return "both_full"
    if in_pool > 0.5 and out_pool <= 0.5:
        return "input_full_output_low"
    if in_pool <= 0.5 and out_pool > 0.5:
        return "input_low_output_full"
    return "healthy"
