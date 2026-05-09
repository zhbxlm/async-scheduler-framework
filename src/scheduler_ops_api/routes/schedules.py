"""schedules routes — /ops/v1/schedules
aligned with docs/deepwiki-reference/API 参考.md + Cron 调度.md
"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends

from scheduler_ops_api.auth import authenticate
from scheduler_ops_api.dependencies import ScheduleRegistryDep
from src.services.access_policy import resolve_tenant_id
from src.services.resource_application import ScheduleService

router = APIRouter(prefix="/ops/v1/schedules", tags=["schedules"])


@router.get("/", summary="List all cron schedules")
async def list_schedules(
    registry: ScheduleRegistryDep,
    _auth: dict = Depends(authenticate),
) -> list[dict]:
    tenant = resolve_tenant_id(_auth)
    return await ScheduleService(registry).list_schedules(tenant)


@router.get("/{schedule_id}", summary="Get schedule detail")
async def get_schedule(
    schedule_id: str,
    registry: ScheduleRegistryDep,
    _auth: dict = Depends(authenticate),
) -> dict:
    tenant = resolve_tenant_id(_auth)
    return await ScheduleService(registry).get_schedule(tenant, schedule_id)


@router.post("/", status_code=201, summary="Create cron schedule")
async def create_schedule(
    body: dict,
    registry: ScheduleRegistryDep,
    _auth: dict = Depends(authenticate),
) -> dict:
    tenant = resolve_tenant_id(_auth)
    return await ScheduleService(registry).create_schedule(tenant, body)


@router.put("/{schedule_id}", summary="Update schedule")
async def update_schedule(
    schedule_id: str,
    body: dict,
    registry: ScheduleRegistryDep,
    _auth: dict = Depends(authenticate),
) -> dict:
    tenant = resolve_tenant_id(_auth)
    return await ScheduleService(registry).update_schedule(tenant, schedule_id, body)


@router.post("/{schedule_id}/toggle", summary="Enable or disable schedule")
async def toggle_schedule(
    schedule_id: str,
    body: dict,
    registry: ScheduleRegistryDep,
    _auth: dict = Depends(authenticate),
) -> dict:
    tenant = resolve_tenant_id(_auth)
    return await ScheduleService(registry).toggle_schedule(tenant, schedule_id, body)


@router.delete("/{schedule_id}", status_code=204, summary="Delete schedule")
async def delete_schedule(
    schedule_id: str,
    registry: ScheduleRegistryDep,
    _auth: dict = Depends(authenticate),
) -> None:
    tenant = resolve_tenant_id(_auth)
    await ScheduleService(registry).delete_schedule(tenant, schedule_id)
