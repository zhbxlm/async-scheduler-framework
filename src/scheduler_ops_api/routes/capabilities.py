"""capabilities routes — /ops/v1/capabilities
aligned with docs/deepwiki-reference/API 参考.md + 调度与资源管理.md
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from scheduler_ops_api.auth import authenticate

router = APIRouter(prefix="/ops/v1/capabilities", tags=["capabilities"])


def _registry(request: Request):
    reg = getattr(request.app.state, "capability_registry", None)
    if reg is None:
        raise HTTPException(status_code=503, detail="CapabilityRegistry not initialised")
    return reg


@router.get("/", summary="List all capabilities")
async def list_capabilities(
    request: Request,
    tenant_id: str = "",
    _auth: dict = Depends(authenticate),
) -> list[dict]:
    reg = _registry(request)
    tenant = _auth.get("tenant_id", tenant_id or "default")
    ids = await reg.list(tenant)
    caps = []
    for cid in ids:
        cap = await reg.get(tenant, cid)
        if cap:
            caps.append(cap)
    return caps


@router.get("/{capability_id}", summary="Get capability detail")
async def get_capability(
    capability_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> dict:
    reg = _registry(request)
    tenant = _auth.get("tenant_id", "default")
    cap = await reg.get(tenant, capability_id)
    if cap is None:
        raise HTTPException(status_code=404, detail=f"Capability {capability_id!r} not found")
    return cap


@router.post("/", status_code=201, summary="Register capability")
async def register_capability(
    request: Request,
    body: dict,
    _auth: dict = Depends(authenticate),
) -> dict:
    reg = _registry(request)
    tenant = _auth.get("tenant_id", "default")
    cap_id = body.get("capability_id") or body.get("name")
    if not cap_id:
        raise HTTPException(status_code=422, detail="capability_id or name required")
    await reg.set(tenant, cap_id, body)
    return {"capability_id": cap_id, "registered": True}


@router.put("/{capability_id}", summary="Update capability")
async def update_capability(
    capability_id: str,
    request: Request,
    body: dict,
    _auth: dict = Depends(authenticate),
) -> dict:
    reg = _registry(request)
    tenant = _auth.get("tenant_id", "default")
    existing = await reg.get(tenant, capability_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Capability {capability_id!r} not found")
    merged = {**existing, **body, "capability_id": capability_id}
    await reg.set(tenant, capability_id, merged)
    return merged


@router.delete("/{capability_id}", status_code=204, summary="Unregister capability")
async def delete_capability(
    capability_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> None:
    reg = _registry(request)
    tenant = _auth.get("tenant_id", "default")
    await reg.delete(tenant, capability_id)


@router.get("/{capability_id}/health", summary="Get capability health")
async def get_capability_health(
    capability_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> dict:
    reg = _registry(request)
    tenant = _auth.get("tenant_id", "default")
    cap = await reg.get(tenant, capability_id)
    if cap is None:
        raise HTTPException(status_code=404, detail=f"Capability {capability_id!r} not found")
    health = cap.get("health_status", "unknown")
    return {"capability_id": capability_id, "health_status": health}
