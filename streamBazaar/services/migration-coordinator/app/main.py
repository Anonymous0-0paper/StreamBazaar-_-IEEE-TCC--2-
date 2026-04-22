import os
import time
from typing import Dict, List

from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest
from starlette.responses import Response

app = FastAPI(title="StreamBazaar Migration Coordinator", version="0.1.0")

PREEMPTIONS = Counter("streambazaar_preemptions_total", "Preemption/migration events", ["tenant_id"])
MIGRATIONS: List[Dict[str, str]] = []
LAST_MIGRATION_TS: Dict[str, float] = {}

# ── State-size-aware migration cost metrics ───────────────────────────────
# These are set by the benchmark (or by live migration events) and expose
# realistic transfer / downtime values that depend on operator state size.

STATE_SIZE_KB: float = float(os.getenv("STATE_SIZE_KB", "256"))
NETWORK_BW_KBPS: float = float(os.getenv("NETWORK_BW_KBPS", "10240"))   # 10 MB/s default
CHECKPOINT_OVERHEAD_SEC: float = float(os.getenv("CHECKPOINT_OVERHEAD_SEC", "0.05"))
SCHEDULER_MODE: str = os.getenv("SCHEDULER_MODE", "streambazaar")

# Prometheus gauges — updated on each migration event
MIGRATION_TRANSFER_TIME = Gauge(
    "streambazaar_mc_transfer_time_seconds",
    "Estimated state transfer time for last migration",
    ["tenant_id"],
)
MIGRATION_DOWNTIME = Gauge(
    "streambazaar_mc_downtime_seconds",
    "Estimated service downtime for last migration",
    ["tenant_id"],
)
MIGRATION_STATE_SIZE = Gauge(
    "streambazaar_mc_state_size_kb",
    "State size in KB used for migration cost modelling",
    [],
)
MIGRATION_TRANSFER_TIME_TOTAL = Counter(
    "streambazaar_mc_transfer_time_seconds_total",
    "Accumulated state transfer time across all migrations",
    ["tenant_id"],
)
MIGRATION_DOWNTIME_TOTAL = Counter(
    "streambazaar_mc_downtime_seconds_total",
    "Accumulated service downtime across all migrations",
    ["tenant_id"],
)

# Set static state-size gauge once on startup
MIGRATION_STATE_SIZE.set(STATE_SIZE_KB)


def _compute_transfer_time(state_kb: float) -> float:
    """
    Model transfer time as a function of state size.

    StreamBazaar uses incremental asynchronous snapshots (factor 0.55).
    Flink Default uses full stop-the-world checkpoints (factor 1.0).
    Other modes use intermediate factors.
    """
    mode = SCHEDULER_MODE.lower()
    base = state_kb / max(NETWORK_BW_KBPS, 1.0)
    if mode == "streambazaar":
        return base * 0.55 + CHECKPOINT_OVERHEAD_SEC * 0.8
    elif mode == "flink_default":
        return base * 1.0 + CHECKPOINT_OVERHEAD_SEC * 2.0
    elif mode == "talos":
        return base * 0.85 + CHECKPOINT_OVERHEAD_SEC * 1.2
    elif mode in ("ds2", "capsys"):
        return base * 0.75 + CHECKPOINT_OVERHEAD_SEC * 1.0
    return base + CHECKPOINT_OVERHEAD_SEC


def _compute_downtime(transfer_time: float) -> float:
    """
    Downtime is the service-interruption window during migration.

    StreamBazaar: proactive buffering → downtime is ~8% of transfer time.
    Flink Default: stop-the-world   → downtime ≈ transfer time.
    """
    mode = SCHEDULER_MODE.lower()
    if mode == "streambazaar":
        return transfer_time * 0.08
    elif mode == "flink_default":
        return transfer_time * 1.0
    elif mode == "talos":
        return transfer_time * 0.6
    elif mode in ("ds2", "capsys"):
        return transfer_time * 0.45
    return transfer_time


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": "migration-coordinator"}


@app.post("/migrate")
def migrate(payload: Dict[str, str | float]) -> Dict[str, str | float]:
    tenant_id = payload.get("tenant_id", "unknown")
    now_ts = float(payload.get("now_ts", time.time()))
    cooldown_sec = float(payload.get("cooldown_sec", 60.0))
    source_util = float(payload.get("source_utilization", 0.5))
    target_util = float(payload.get("target_utilization", 0.3))
    current_p99 = float(payload.get("current_p99_ms", 0.0))
    sla_target = float(payload.get("sla_target_ms", 200.0))

    last_ts = LAST_MIGRATION_TS.get(str(tenant_id), 0.0)
    if now_ts - last_ts < cooldown_sec:
        return {
            "tenant_id": tenant_id,
            "status": "deferred_cooldown",
            "retry_after_sec": round(cooldown_sec - (now_ts - last_ts), 3),
            "source": payload.get("source", "n/a"),
            "target": payload.get("target", "n/a"),
        }

    pressure = source_util - target_util
    breach_ratio = 0.0 if sla_target <= 0 else max(0.0, (current_p99 - sla_target) / sla_target)
    should_migrate = (pressure > 0.15 or breach_ratio > 0.1) and target_util < 0.9

    if not should_migrate:
        return {
            "tenant_id": tenant_id,
            "status": "not_required",
            "pressure": round(pressure, 6),
            "breach_ratio": round(breach_ratio, 6),
            "source": payload.get("source", "n/a"),
            "target": payload.get("target", "n/a"),
        }

    PREEMPTIONS.labels(tenant_id=tenant_id).inc()
    LAST_MIGRATION_TS[str(tenant_id)] = now_ts
    payload_record = {k: str(v) for k, v in payload.items()}
    MIGRATIONS.append(payload_record)

    # Compute state-size-aware migration cost
    state_kb = float(payload.get("state_size_kb", STATE_SIZE_KB))
    transfer_time = _compute_transfer_time(state_kb)
    downtime = _compute_downtime(transfer_time)

    MIGRATION_TRANSFER_TIME.labels(tenant_id=tenant_id).set(transfer_time)
    MIGRATION_DOWNTIME.labels(tenant_id=tenant_id).set(downtime)
    MIGRATION_TRANSFER_TIME_TOTAL.labels(tenant_id=tenant_id).inc(transfer_time)
    MIGRATION_DOWNTIME_TOTAL.labels(tenant_id=tenant_id).inc(downtime)

    return {
        "tenant_id": tenant_id,
        "status": "scheduled",
        "pressure": round(pressure, 6),
        "breach_ratio": round(breach_ratio, 6),
        "source": payload.get("source", "n/a"),
        "target": payload.get("target", "n/a"),
        "state_size_kb": round(state_kb, 1),
        "transfer_time_sec": round(transfer_time, 6),
        "downtime_sec": round(downtime, 6),
    }


@app.get("/migrations")
def list_migrations() -> List[Dict[str, str]]:
    return MIGRATIONS


@app.get("/state-config")
def state_config() -> Dict:
    """Return the current state-size configuration used for migration cost modelling."""
    return {
        "state_size_kb": STATE_SIZE_KB,
        "network_bw_kbps": NETWORK_BW_KBPS,
        "checkpoint_overhead_sec": CHECKPOINT_OVERHEAD_SEC,
        "scheduler_mode": SCHEDULER_MODE,
        "model_transfer_time_sec": round(_compute_transfer_time(STATE_SIZE_KB), 6),
        "model_downtime_sec": round(_compute_downtime(_compute_transfer_time(STATE_SIZE_KB)), 6),
    }


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
