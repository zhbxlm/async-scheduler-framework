"""schedules routes — /ops/v1/schedules
aligned with docs/deepwiki-reference/API 参考.md + Cron 调度.md
"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request

from src.api.auth import authenticate
from src.services.access_policy import resolve_tenant_id

router = APIRouter(prefix="/ops/v1/schedules", tags=["schedules"])


def _registry(request: Request):
    reg = getattr(request.app.state, "schedule_registry", None)
    if reg is None:
        raise HTTPException(status_code=503, detail="ScheduleRegistry not initialised")
    return reg


@router.get("/", summary="List all cron schedules")
async def list_schedules(
    request: Request,
    _auth: dict = Depends(authenticate),
) -> list[dict]:
    reg = _registry(request)
    tenant = resolve_tenant_id(_auth)
    ids = await reg.list(tenant)
    schedules = []
    for sid in ids:
        s = await reg.get(tenant, sid)
        if s:
            schedules.append(s)
    return schedules


@router.get("/{schedule_id}", summary="Get schedule detail")
async def get_schedule(
    schedule_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> dict:
    reg = _registry(request)
    tenant = resolve_tenant_id(_auth)
    s = await reg.get(tenant, schedule_id)
    if s is None:
        raise HTTPException(status_code=404, detail=f"Schedule {schedule_id!r} not found")
    return s


@router.post("/", status_code=201, summary="Create cron schedule")
async def create_schedule(
    request: Request,
    body: dict,
    _auth: dict = Depends(authenticate),
) -> dict:
    import uuid
    reg = _registry(request)
    tenant = resolve_tenant_id(_auth)
    cron_expr = body.get("cron_expr")
    if not cron_expr:
        raise HTTPException(status_code=422, detail="cron_expr required")
    schedule_id = body.get("schedule_id") or str(uuid.uuid4())
    now_iso = datetime.now(tz=timezone.utc).isoformat()
    record = {
        **body,
        "schedule_id": schedule_id,
        "tenant_id": tenant,
        "enabled": body.get("enabled", True),
        "created_at": now_iso,
        "updated_at": now_iso,
        "last_triggered_at": None,
        "next_fire_at": None,
    }
    await reg.set(tenant, schedule_id, record)
    return record


@router.put("/{schedule_id}", summary="Update schedule")
async def update_schedule(
    schedule_id: str,
    request: Request,
    body: dict,
    _auth: dict = Depends(authenticate),
) -> dict:
    reg = _registry(request)
    tenant = resolve_tenant_id(_auth)
    existing = await reg.get(tenant, schedule_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Schedule {schedule_id!r} not found")
    now_iso = datetime.now(tz=timezone.utc).isoformat()
    merged = {**existing, **body, "schedule_id": schedule_id, "updated_at": now_iso}
    await reg.set(tenant, schedule_id, merged)
    return merged


@router.post("/{schedule_id}/toggle", summary="Enable or disable schedule")
async def toggle_schedule(
    schedule_id: str,
    request: Request,
    body: dict,
    _auth: dict = Depends(authenticate),
) -> dict:
    reg = _registry(request)
    tenant = resolve_tenant_id(_auth)
    enabled = bool(body.get("enabled", True))
    ok = await reg.toggle(tenant, schedule_id, enabled)
    if not ok:
        raise HTTPException(status_code=404, detail=f"Schedule {schedule_id!r} not found")
    return {"schedule_id": schedule_id, "enabled": enabled}


@router.delete("/{schedule_id}", status_code=204, summary="Delete schedule")
async def delete_schedule(
    schedule_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
) -> None:
    reg = _registry(request)
    tenant = resolve_tenant_id(_auth)
    await reg.delete(tenant, schedule_id)
