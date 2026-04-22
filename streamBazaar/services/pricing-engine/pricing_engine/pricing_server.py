import os
from typing import Dict

import numpy as np
import redis as redis_lib
from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest
from starlette.responses import Response

app = FastAPI(title="StreamBazaar Pricing Engine", version="0.1.0")

CURRENT_PRICE = Gauge("streambazaar_current_price", "Current calculated bid floor", ["tenant_id"])
PRICE_RAW = Gauge("streambazaar_current_price_raw", "Raw unsmoothed bid floor", ["tenant_id"])

# Redis for shared price history — allows multiple pricing-engine instances
# (one per node) to keep smoothing state consistent across restarts.
# Falls back to local dict in single-node mode.
_REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
_redis: redis_lib.Redis | None = None
_local_prices: Dict[str, float] = {}
PRICES_KEY = "streambazaar:prices"


def _get_redis() -> redis_lib.Redis | None:
    global _redis
    if _redis is None:
        try:
            _redis = redis_lib.from_url(_REDIS_URL, decode_responses=True, socket_connect_timeout=1)
            _redis.ping()
        except Exception:
            _redis = None
    return _redis


def _get_prev_price(tenant_id: str, default: float) -> float:
    r = _get_redis()
    if r:
        try:
            v = r.hget(PRICES_KEY, tenant_id)
            return float(v) if v is not None else default
        except Exception:
            pass
    return _local_prices.get(tenant_id, default)


def _set_price(tenant_id: str, price: float) -> None:
    r = _get_redis()
    if r:
        try:
            r.hset(PRICES_KEY, tenant_id, str(price))
            return
        except Exception:
            pass
    _local_prices[tenant_id] = price


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": "pricing-engine"}


@app.post("/price")
def calculate_price(payload: Dict[str, float | str]) -> Dict[str, float | str]:
    tenant_id = str(payload.get("tenant_id", "unknown"))
    utilization = float(payload.get("utilization", 0.5))
    sla_pressure = float(payload.get("sla_pressure", 0.5))
    queue_backlog = float(payload.get("queue_backlog", 0.0))
    credit_balance = float(payload.get("credit_balance", 50000.0))
    capacity = float(payload.get("capacity", 100000.0))
    demand = float(payload.get("demand", 0.0))
    # Paper parameters (Table II): θ=0.7, κ=1.5, η=0.3, μ=0.5, U_target=0.8
    kappa = float(payload.get("kappa", 1.5))
    eta = float(payload.get("eta", 0.3))
    mu = float(payload.get("mu", 0.5))
    u_target = float(payload.get("u_target", 0.8))
    smoothing = float(payload.get("smoothing", 0.7))

    utilization = float(np.clip(utilization, 0.0, 1.0))
    demand_ratio = float(np.clip(demand / max(capacity, 1.0), 0.0, 2.0))

    # Eq. 5: piecewise adjustment function f(U, D)
    # Above target: quadratic pressure term + demand ratio
    # Below target: linear discount scaled by idle capacity
    if utilization > u_target:
        adj = 1.0 + kappa * (utilization - u_target) ** 2 + eta * demand_ratio
    else:
        adj = 1.0 - mu * (u_target - utilization) * (1.0 - demand_ratio)

    # Spot price: base 0.2 scaled by SLA urgency (proxy for clearing price)
    # and adjusted by credit balance (spending incentive term from Eq. 1 efficiency metric)
    balance_norm = float(np.clip(credit_balance / 100000.0, 0.0, 1.0))
    p_spot = float(np.clip(0.2 + 0.6 * sla_pressure + 0.25 * (queue_backlog / max(capacity, 1.0)) + 0.35 * (1.0 - balance_norm), 0.1, 5.0))

    # Eq. 4: p^t = θ·p^(t-1) + (1-θ)·p_spot·f(U,D)
    prev = _get_prev_price(tenant_id, p_spot)
    raw_price = float(np.clip(p_spot * adj, 0.1, 5.0))
    bid_floor = float(np.clip(smoothing * prev + (1.0 - smoothing) * raw_price, 0.1, 5.0))
    _set_price(tenant_id, bid_floor)

    PRICE_RAW.labels(tenant_id=tenant_id).set(raw_price)
    CURRENT_PRICE.labels(tenant_id=tenant_id).set(bid_floor)

    return {
        "tenant_id": tenant_id,
        "bid_floor": bid_floor,
        "raw_price": raw_price,
        "utilization": utilization,
        "sla_pressure": sla_pressure,
        "adjustment_factor": round(adj, 6),
        "demand_ratio": round(demand_ratio, 6),
        "balance_norm": round(balance_norm, 6),
        "currency": "virtual_credit",
    }


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
