"""tenants routes — /ops/v1/tenants
aligned with docs/deepwiki-reference/API 参考.md + 配额与多租户.md
"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request

from src.api.auth import authenticate
from src.services.access_policy import TenantAccessPolicy

router = APIRouter(prefix="/ops/v1/tenants", tags=["tenants"])


def _registry(request: Request):
    reg = getattr(request.app.state, "tenant_registry", None)
    if reg is None:
        raise HTTPException(status_code=503, detail="TenantRegistry not initialised")
    return reg


@router.get("/", summary="List tenants (super-admin only)")
async def list_tenants(
    request: Request,
    _auth: dict = Depends(authenticate),
) -> list[dict]:
    TenantAccessPolicy.require_super_admin(_auth)
    reg = _registry(request)
    tenant_ids = await reg.list_all_tenants()
    tenants = []
    for tid in tenant_ids:
        t = await reg.get_tenant(tid)
        if t:
            tenants.append(t)
    return tenants


@router.get("/{tenant_id}", summary="Get tenant detail")
async def get_tenant(
    tenant_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> dict:
    TenantAccessPolicy.require_same_tenant_or_super_admin(_auth, tenant_id)
    reg = _registry(request)
    t = await reg.get_tenant(tenant_id)
    if t is None:
        raise HTTPException(status_code=404, detail=f"Tenant {tenant_id!r} not found")
    return t


@router.post("/", status_code=201, summary="Register tenant (super-admin only)")
async def register_tenant(
    request: Request,
    body: dict,
    _auth: dict = Depends(authenticate),
) -> dict:
    TenantAccessPolicy.require_super_admin(_auth)
    reg = _registry(request)
    now_iso = datetime.now(tz=timezone.utc).isoformat()
    import secrets
    tenant_id = body.get("tenant_id") or f"tenant-{secrets.token_hex(6)}"
    record = {
        "tenant_id": tenant_id,
        "tenant_name": body.get("tenant_name", ""),
        "status": "active",
        "quota": body.get("quota", {}),
        "description": body.get("description", ""),
        "created_at": now_iso,
        "updated_at": now_iso,
    }
    await reg.register(record)
    return record


@router.put("/{tenant_id}", summary="Update tenant")
async def update_tenant(
    tenant_id: str,
    request: Request,
    body: dict,
    _auth: dict = Depends(authenticate),
) -> dict:
    TenantAccessPolicy.require_super_admin(_auth)
    reg = _registry(request)
    existing = await reg.get_tenant(tenant_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Tenant {tenant_id!r} not found")
    now_iso = datetime.now(tz=timezone.utc).isoformat()
    merged = {**existing, **body, "tenant_id": tenant_id, "updated_at": now_iso}
    await reg.register(merged)
    return merged


@router.delete("/{tenant_id}", status_code=204, summary="Unregister tenant (super-admin only)")
async def delete_tenant(
    tenant_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> None:
    TenantAccessPolicy.require_super_admin(_auth)
    reg = _registry(request)
    await reg.unregister(tenant_id)


@router.post("/{tenant_id}/api-keys", status_code=201, summary="Generate API key for tenant")
async def generate_api_key(
    tenant_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> dict:
    TenantAccessPolicy.require_same_tenant_or_super_admin(_auth, tenant_id)
    reg = _registry(request)
    api_key = await reg.generate_api_key(tenant_id)
    return {"tenant_id": tenant_id, "api_key": api_key, "note": "Store this key; it will not be shown again."}


@router.get("/{tenant_id}/usage", summary="Get tenant resource usage")
async def get_tenant_usage(
    tenant_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> dict:
    TenantAccessPolicy.require_same_tenant_or_super_admin(_auth, tenant_id)
    reg = _registry(request)
    usage = await reg.get_usage(tenant_id)
    return {"tenant_id": tenant_id, **usage}
