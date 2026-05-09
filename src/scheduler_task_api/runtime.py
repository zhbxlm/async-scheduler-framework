"""task-api package-owned runtime helpers.

These helpers are canonical for the packaged task-api runtime and replace
monorepo-only ownership of task-api bootstrap details.
"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI


def mount_container_state(app: FastAPI, container: Any, names: tuple[str, ...]) -> None:
    for name in names:
        setattr(app.state, name, getattr(container, name, None))


async def start_container_lifecycle(container: Any) -> None:
    manager = getattr(container, "lifecycle_manager", None)
    if manager is not None:
        await manager.start_all()


async def stop_container_lifecycle(container: Any) -> None:
    manager = getattr(container, "lifecycle_manager", None)
    if manager is not None:
        await manager.stop_all()


def cache_auth_settings(app: FastAPI, settings: Any) -> None:
    app.state.auth_settings = {
        "super_admin_key": settings.tenant.super_admin_api_key,
        "multi_tenant_enabled": settings.tenant.multi_tenant_enabled,
        "tenant_id_header": settings.tenant.tenant_id_header,
    }
