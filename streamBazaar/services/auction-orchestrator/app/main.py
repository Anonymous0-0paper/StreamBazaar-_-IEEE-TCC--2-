import json
import os
import time
from typing import Dict, List

import redis as redis_lib
from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from starlette.responses import Response

app = FastAPI(title="StreamBazaar Auction Orchestrator", version="0.1.0")

BID_COUNTER = Counter("streambazaar_bids_total", "Total accepted bids", ["tenant_id"])
AUCTION_LATENCY = Histogram("streambazaar_auction_latency_ms", "Auction processing latency")
AUCTION_REVENUE = Gauge("streambazaar_auction_revenue", "Latest auction revenue")
WINNING_BID = Gauge("streambazaar_winning_bid", "Latest winner bid", ["tenant_id"])

TENANT_PRIORITY: Dict[str, float] = {
    "tenant-fraud": 1.3,
    "tenant-clickstream": 1.0,
    "tenant-ml": 1.5,
}

# Redis — shared bid state so multiple stream-coordinator nodes can all submit bids
# to a single clearinghouse. Falls back to in-process dict when Redis is unavailable
# (single-node mode).
_REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
_redis: redis_lib.Redis | None = None
_local_bids: Dict[str, float] = {}          # fallback for single-node
_local_clearing: Dict[str, object] = {}     # fallback for single-node

BIDS_KEY = "streambazaar:bids"
CLEARING_KEY = "streambazaar:last_clearing"
LOCK_KEY = "streambazaar:auction_lock"


def _get_redis() -> redis_lib.Redis | None:
    global _redis
    if _redis is None:
        try:
            _redis = redis_lib.from_url(_REDIS_URL, decode_responses=True, socket_connect_timeout=1)
            _redis.ping()
        except Exception:
            _redis = None
    return _redis


def _set_bid(tenant_id: str, bid_price: float) -> None:
    r = _get_redis()
    if r:
        try:
            r.hset(BIDS_KEY, tenant_id, str(bid_price))
            return
        except Exception:
            pass
    _local_bids[tenant_id] = bid_price


def _get_all_bids() -> Dict[str, float]:
    r = _get_redis()
    if r:
        try:
            raw = r.hgetall(BIDS_KEY)
            return {k: float(v) for k, v in raw.items()}
        except Exception:
            pass
    return dict(_local_bids)


def _save_clearing(result: Dict[str, object]) -> None:
    r = _get_redis()
    if r:
        try:
            r.set(CLEARING_KEY, json.dumps(result), ex=60)
            return
        except Exception:
            pass
    _local_clearing.clear()
    _local_clearing.update(result)


def _load_clearing() -> Dict[str, object]:
    r = _get_redis()
    if r:
        try:
            raw = r.get(CLEARING_KEY)
            if raw:
                return json.loads(raw)
        except Exception:
            pass
    return dict(_local_clearing)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": "auction-orchestrator"}


@app.post("/bid")
def submit_bid(payload: Dict[str, str | float]) -> Dict[str, str | float]:
    start = time.perf_counter()
    tenant_id = str(payload.get("tenant_id", "unknown"))
    bid_price = float(payload.get("bid_price", 0.0))
    _set_bid(tenant_id, bid_price)
    BID_COUNTER.labels(tenant_id=tenant_id).inc()
    AUCTION_LATENCY.observe((time.perf_counter() - start) * 1000.0)
    return {
        "tenant_id": tenant_id,
        "accepted_bid": bid_price,
        "allocation_token": f"alloc-{tenant_id}-{int(time.time())}",
    }


@app.post("/auction/clear")
def clear_auction(payload: Dict[str, object]) -> Dict[str, object]:
    start = time.perf_counter()
    resource_units = int(payload.get("resource_units", 10))
    min_price = float(payload.get("min_price", 0.1))
    sla_urgency = payload.get("sla_urgency", {})
    # Per-tenant requested bundle sizes for greedy knapsack (Algorithm 1)
    requested_units_map = payload.get("requested_units", {})

    if resource_units <= 0:
        return {"winners": [], "clearing_price": min_price, "revenue": 0.0}

    # Distributed lock: in multi-node mode, only one coordinator runs the clearing
    # per interval. Others that fail to acquire simply re-use the last result.
    r = _get_redis()
    lock = None
    if r:
        try:
            lock = r.lock(LOCK_KEY, timeout=2.0, blocking_timeout=0.0)
            acquired = lock.acquire(blocking=False)
            if not acquired:
                # Another node is clearing right now — return last known result
                return _load_clearing() or {"winners": [], "clearing_price": min_price, "revenue": 0.0}
        except Exception:
            lock = None

    try:
        all_bids = _get_all_bids()

        candidates: List[Dict[str, float | str]] = []
        for tenant_id, bid in all_bids.items():
            if bid < min_price:
                continue
            priority = float(TENANT_PRIORITY.get(tenant_id, 1.0))
            urgency = float(sla_urgency.get(tenant_id, 0.0)) if isinstance(sla_urgency, dict) else 0.0
            # Efficiency score: bid × priority × (1 + urgency) — maps to paper e_i = bid_density × φ × spending_incentive
            score = bid * priority * (1.0 + urgency)
            req_units = int(requested_units_map.get(tenant_id, 1)) if isinstance(requested_units_map, dict) else 1
            candidates.append(
                {
                    "tenant_id": tenant_id,
                    "bid": bid,
                    "priority": priority,
                    "urgency": urgency,
                    "score": score,
                    "requested_units": max(1, req_units),
                }
            )

        candidates.sort(key=lambda item: float(item["score"]), reverse=True)
        if not candidates:
            result: Dict[str, object] = {"winners": [], "clearing_price": min_price, "revenue": 0.0}
            _save_clearing(result)
            return result

        # Algorithm 1 (paper): greedy knapsack by efficiency score.
        # Each tenant requests a bundle; accept if requested units fit in remaining capacity.
        allocated = []
        remaining_units = resource_units
        for item in candidates:
            requested = int(item.get("requested_units", 1))
            requested = max(1, requested)
            if requested <= remaining_units:
                allocated.append({**item, "allocated_units": requested})
                remaining_units -= requested
            else:
                if remaining_units > 0:
                    allocated.append({**item, "allocated_units": remaining_units})
                    remaining_units = 0
            if remaining_units <= 0:
                break

        # Second-price clearing: winner pays the highest losing bid (paper Eq. 3)
        allocated_ids = {str(a["tenant_id"]) for a in allocated}
        losing_bids = [float(item["bid"]) for item in candidates if str(item["tenant_id"]) not in allocated_ids]
        clearing_price = max(min_price, max(losing_bids)) if losing_bids else max(min_price, float(candidates[0]["bid"]))

        revenue = 0.0
        winners: List[Dict[str, float | str | int]] = []
        for item in allocated:
            cost = float(item["allocated_units"]) * clearing_price
            revenue += cost
            tenant_id = str(item["tenant_id"])
            WINNING_BID.labels(tenant_id=tenant_id).set(float(item["bid"]))
            winners.append(
                {
                    "tenant_id": tenant_id,
                    "bid": round(float(item["bid"]), 6),
                    "score": round(float(item["score"]), 6),
                    "allocated_units": int(item["allocated_units"]),
                    "price_per_unit": round(clearing_price, 6),
                    "total_cost": round(cost, 6),
                }
            )

        AUCTION_REVENUE.set(revenue)
        AUCTION_LATENCY.observe((time.perf_counter() - start) * 1000.0)

        result = {
            "timestamp": int(time.time()),
            "resource_units": resource_units,
            "min_price": min_price,
            "clearing_price": clearing_price,
            "revenue": revenue,
            "winners": winners,
        }
        _save_clearing(result)
        return result
    finally:
        if lock:
            try:
                lock.release()
            except Exception:
                pass


@app.get("/latest-bids")
def latest_bids() -> Dict[str, float]:
    return _get_all_bids()


@app.get("/auction/last")
def auction_last() -> Dict[str, object]:
    return _load_clearing()


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
