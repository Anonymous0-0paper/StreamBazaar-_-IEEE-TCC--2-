from __future__ import annotations

from typing import Any, Dict, Iterable


def _safe_avg(values: Iterable[float]) -> float:
    items = [float(v) for v in values]
    if not items:
        return 0.0
    return sum(items) / len(items)


def _safe_div(num: float, den: float) -> float:
    if den == 0:
        return 0.0
    return num / den


def calculate_intermediate_task_metrics(task_id: str, flink_jmx_client: Any) -> Dict[str, float]:
    """Compute TALOS intermediate-task equations (Eq.1-5).

    Eq.1 queuedRecordsIn  = (bufferSize * inPoolUsage) / avg(bytesPerRecord)
    Eq.2 queuedRecordsOut = (bufferSize * outPoolUsage) / avg(bytesPerRecord)
    Eq.3 totalLag         = sum(bufferSize * inPoolUsage) / avg(bytesPerRecord)
    Eq.4 throughput       = avg(recordsInPerSec) / avg(busyTimeMs)
    Eq.5 relativeLagChangeRate = deriv(totalLag) / throughput
    """

    buffer_size = float(flink_jmx_client.get_task_metric(task_id, "buffer_size_bytes", 0.0))
    in_pool_usage = float(flink_jmx_client.get_task_metric(task_id, "in_pool_usage", 0.0))
    out_pool_usage = float(flink_jmx_client.get_task_metric(task_id, "out_pool_usage", 0.0))

    bytes_per_record_samples = flink_jmx_client.get_task_metric(task_id, "bytes_per_record_samples", [1.0])
    records_in_per_sec_samples = flink_jmx_client.get_task_metric(task_id, "records_in_per_sec_samples", [0.0])
    busy_time_ms_samples = flink_jmx_client.get_task_metric(task_id, "busy_time_ms_samples", [1.0])

    avg_bytes_per_record = max(_safe_avg(bytes_per_record_samples), 1e-9)
    avg_records_in_per_sec = _safe_avg(records_in_per_sec_samples)
    avg_busy_time_ms = max(_safe_avg(busy_time_ms_samples), 1e-9)

    queued_records_in = _safe_div(buffer_size * in_pool_usage, avg_bytes_per_record)
    queued_records_out = _safe_div(buffer_size * out_pool_usage, avg_bytes_per_record)

    in_pool_samples = flink_jmx_client.get_task_metric(task_id, "in_pool_usage_per_subtask", [in_pool_usage])
    total_lag_numerator = sum(buffer_size * float(pool_usage) for pool_usage in in_pool_samples)
    total_lag = _safe_div(total_lag_numerator, avg_bytes_per_record)

    throughput = _safe_div(avg_records_in_per_sec, avg_busy_time_ms)

    lag_derivative = flink_jmx_client.get_task_metric(task_id, "lag_derivative", None)
    if lag_derivative is None:
        previous_lag = float(flink_jmx_client.get_task_metric(task_id, "previous_total_lag", total_lag))
        elapsed_sec = max(float(flink_jmx_client.get_task_metric(task_id, "elapsed_sec", 1.0)), 1e-9)
        lag_derivative = _safe_div((total_lag - previous_lag), elapsed_sec)

    relative_lag_change_rate = _safe_div(float(lag_derivative), throughput)

    return {
        "queued_records_in": queued_records_in,
        "queued_records_out": queued_records_out,
        "total_lag": total_lag,
        "throughput": throughput,
        "relative_lag_change_rate": relative_lag_change_rate,
        "in_pool_usage": in_pool_usage,
        "out_pool_usage": out_pool_usage,
        "backpressure_ms": float(flink_jmx_client.get_task_metric(task_id, "backpressure_ms", 0.0)),
        "busy_time_ms": avg_busy_time_ms,
        "idle_time_ms": float(flink_jmx_client.get_task_metric(task_id, "idle_time_ms", 0.0)),
    }


def calculate_source_task_metrics(task_id: str, flink_jmx_client: Any) -> Dict[str, float]:
    """Compute TALOS source-task equations (Eq.6-8).

    Eq.6 totalLag     = sum(record_lag_max_i)
    Eq.7 throughput   = sum(records_consumed_rate_i)
    Eq.8 lagChangeRate = deriv(totalLag) / throughput
    """

    kafka_partitions = flink_jmx_client.get_source_partitions(task_id)
    lag_values = [float(p.get("record_lag_max", 0.0)) for p in kafka_partitions]
    consumed_rates = [float(p.get("records_consumed_rate", 0.0)) for p in kafka_partitions]

    total_lag = sum(lag_values)
    throughput = sum(consumed_rates)

    lag_derivative = flink_jmx_client.get_task_metric(task_id, "lag_derivative", None)
    if lag_derivative is None:
        previous_lag = float(flink_jmx_client.get_task_metric(task_id, "previous_total_lag", total_lag))
        elapsed_sec = max(float(flink_jmx_client.get_task_metric(task_id, "elapsed_sec", 1.0)), 1e-9)
        lag_derivative = _safe_div((total_lag - previous_lag), elapsed_sec)

    lag_change_rate = _safe_div(float(lag_derivative), throughput)

    return {
        "total_lag": total_lag,
        "throughput": throughput,
        "lag_change_rate": lag_change_rate,
        "in_pool_usage": float(flink_jmx_client.get_task_metric(task_id, "in_pool_usage", 0.0)),
        "out_pool_usage": float(flink_jmx_client.get_task_metric(task_id, "out_pool_usage", 0.0)),
        "backpressure_ms": float(flink_jmx_client.get_task_metric(task_id, "backpressure_ms", 0.0)),
        "busy_time_ms": float(flink_jmx_client.get_task_metric(task_id, "busy_time_ms", 0.0)),
        "idle_time_ms": float(flink_jmx_client.get_task_metric(task_id, "idle_time_ms", 0.0)),
    }
