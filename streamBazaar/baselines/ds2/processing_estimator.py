from __future__ import annotations

import math
from typing import Dict, Iterable, List, Mapping


def estimate_true_processing_time(operator_metrics: Mapping[str, float]) -> Dict[str, float]:
    """Estimate DS2 performance model terms.

    trueProcessingTime = actualProcessingTime - backpressureWaitTime
    processingCapacity = 1 / trueProcessingTime
    estimatedOutputRate = min(inputRate, processingCapacity)
    """

    actual_processing_time = float(operator_metrics.get("actual_processing_time", 0.0))
    backpressure_wait_time = float(operator_metrics.get("backpressure_wait_time", 0.0))
    input_rate = float(operator_metrics.get("input_rate", 0.0))

    true_processing_time = max(actual_processing_time - backpressure_wait_time, 1e-9)
    processing_capacity = 1.0 / true_processing_time
    estimated_output_rate = min(input_rate, processing_capacity)

    return {
        "true_processing_time": true_processing_time,
        "processing_capacity": processing_capacity,
        "estimated_output_rate": estimated_output_rate,
    }


def identify_bottlenecks(
    operators: Mapping[str, Mapping[str, float]],
    required_throughput: Mapping[str, float],
) -> Dict[str, Dict[str, float]]:
    """Identify DS2 bottlenecks and required parallelism.

    Bottleneck if: processingCapacity * parallelism < requiredThroughput
    requiredParallelism = ceil(requiredThroughput / capacity)
    """

    bottlenecks: Dict[str, Dict[str, float]] = {}

    for operator_id, metrics in operators.items():
        perf = estimate_true_processing_time(metrics)
        capacity = perf["processing_capacity"]
        parallelism = max(1, int(metrics.get("parallelism", 1)))
        req = float(required_throughput.get(operator_id, metrics.get("required_throughput", 0.0)))

        if capacity * parallelism < req:
            required_parallelism = int(math.ceil(req / max(capacity, 1e-9)))
            bottlenecks[operator_id] = {
                "capacity": capacity,
                "parallelism": float(parallelism),
                "required_throughput": req,
                "required_parallelism": float(required_parallelism),
            }

    return bottlenecks
