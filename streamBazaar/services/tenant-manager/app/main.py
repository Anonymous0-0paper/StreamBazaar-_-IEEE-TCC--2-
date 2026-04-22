import os
from typing import Dict, List

from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from starlette.responses import Response

app = FastAPI(title="StreamBazaar Tenant Manager", version="0.1.0")

TENANT_REGISTRATIONS = Counter("streambazaar_tenants_registered_total", "Total tenant registrations")
TENANTS: List[Dict[str, str | float]] = []


@app.get("/health")
def health() -> Dict[str, str]:
    return {
        "status": "ok",
        "service": "tenant-manager",
        "db_host": os.getenv("POSTGRES_HOST", "postgres"),
    }


@app.post("/tenants")
def register_tenant(payload: Dict[str, str | float]) -> Dict[str, str | float]:
    TENANTS.append(payload)
    TENANT_REGISTRATIONS.inc()
    return payload


@app.get("/tenants")
def list_tenants() -> List[Dict[str, str | float]]:
    return TENANTS


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
