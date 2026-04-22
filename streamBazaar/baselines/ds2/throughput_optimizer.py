from __future__ import annotations

import math
from typing import Dict, List, Mapping

from .processing_estimator import estimate_true_processing_time


def optimize_for_throughput(dataflow_graph: Mapping[str, object]) -> Dict[str, int]:
    """Compute conservative throughput-oriented DS2 targets with 3-step cap."""

    operators = dataflow_graph.get("operators", {})
    required_throughput = dataflow_graph.get("required_throughput", {})
    max_step = int(dataflow_graph.get("max_step", 3))

    targets: Dict[str, int] = {}
    for operator_id, metrics in operators.items():
        perf = estimate_true_processing_time(metrics)
        capacity = max(perf["processing_capacity"], 1e-9)
        current_parallelism = max(1, int(metrics.get("parallelism", 1)))
        req = float(required_throughput.get(operator_id, metrics.get("required_throughput", 0.0)))

        raw_target = int(math.ceil(req / capacity)) if req > 0 else current_parallelism

        if raw_target > current_parallelism:
            targets[operator_id] = current_parallelism + min(max_step, raw_target - current_parallelism)
        elif raw_target < current_parallelism and float(metrics.get("utilization", 1.0)) < 0.3:
            targets[operator_id] = max(1, current_parallelism - 1)
        else:
            targets[operator_id] = current_parallelism

    return targets
