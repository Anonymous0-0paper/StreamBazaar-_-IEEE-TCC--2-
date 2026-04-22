from typing import Dict, List

from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from starlette.responses import Response

app = FastAPI(title="StreamBazaar Resource Allocator", version="0.1.0")

ALLOCATIONS = Counter("streambazaar_allocations_total", "Allocation decisions made", ["tenant_id"])


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "service": "resource-allocator"}


def _effective_weight(tenant: Dict[str, float | str]) -> float:
    priority_weight = float(tenant.get("priority_weight", 1.0))
    sla_gap = float(tenant.get("sla_gap", 0.0))
    credits = float(tenant.get("virtual_currency_balance", 50000.0))
    credit_factor = 0.8 + min(max(credits, 0.0), 200000.0) / 200000.0
    return max(0.05, priority_weight * (1.0 + sla_gap) * credit_factor)


def _weighted_fair_allocate(total_slots: int, tenants: List[Dict[str, float | str]]) -> List[Dict[str, float | str]]:
    if total_slots <= 0 or not tenants:
        return []

    reqs = [max(0, int(float(t.get("requested_slots", 0)))) for t in tenants]
    weights = [_effective_weight(t) for t in tenants]
    weight_sum = sum(weights)
    if weight_sum <= 0:
        weight_sum = float(len(tenants))
        weights = [1.0] * len(tenants)

    alloc = [0] * len(tenants)
    # Stage 1: proportional base shares.
    for i in range(len(tenants)):
        fair_share = int((weights[i] / weight_sum) * total_slots)
        alloc[i] = min(reqs[i], fair_share)

    remaining = total_slots - sum(alloc)
    # Stage 2: water-filling by highest normalized deficit.
    while remaining > 0:
        deficits = []
        for i in range(len(tenants)):
            pending = reqs[i] - alloc[i]
            if pending <= 0:
                continue
            deficits.append((pending / max(weights[i], 1e-6), i))
        if not deficits:
            break
        _, idx = max(deficits)
        alloc[idx] += 1
        remaining -= 1

    result: List[Dict[str, float | str]] = []
    for i, tenant in enumerate(tenants):
        tenant_id = str(tenant.get("tenant_id", f"tenant-{i}"))
        ALLOCATIONS.labels(tenant_id=tenant_id).inc()
        result.append(
            {
                "tenant_id": tenant_id,
                "requested_slots": reqs[i],
                "granted_slots": alloc[i],
                "effective_weight": round(weights[i], 6),
            }
        )
    return result


@app.post("/allocate")
def allocate(payload: Dict[str, object]) -> Dict[str, object]:
    if "tenants" in payload:
        tenants = payload.get("tenants")
        if isinstance(tenants, list):
            total_slots = int(float(payload.get("total_slots", 100)))
            allocations = _weighted_fair_allocate(total_slots=total_slots, tenants=tenants)
            return {
                "mode": "batch_weighted_fair",
                "total_slots": total_slots,
                "allocations": allocations,
            }

    # Backward-compatible single-tenant path.
    tenant_id = str(payload.get("tenant_id", "unknown"))
    requested_slots = float(payload.get("requested_slots", 1))
    utilization = float(payload.get("cluster_utilization", 0.6))
    sla_gap = float(payload.get("sla_gap", 0.0))
    priority = float(payload.get("priority_weight", 1.0))
    pressure = max(0.1, min(1.0, 1.0 - 0.5 * utilization + 0.2 * sla_gap + 0.1 * (priority - 1.0)))
    granted = max(1.0, round(requested_slots * pressure, 2))
    ALLOCATIONS.labels(tenant_id=tenant_id).inc()
    return {
        "tenant_id": tenant_id,
        "granted_slots": granted,
        "pressure_factor": round(pressure, 6),
    }


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
