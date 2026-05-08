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


async def stop_container_lifecycle(container: Any, *, drain_timeout: float | None = None) -> None:
    manager = getattr(container, "lifecycle_manager", None)
    if manager is None:
        return
    if drain_timeout is None:
        await manager.stop_all()
    else:
        await manager.stop_all(drain_timeout=drain_timeout)


def cache_auth_settings(app: FastAPI, settings: Any) -> None:
    app.state.auth_settings = {
        "super_admin_key": settings.tenant.super_admin_api_key,
        "multi_tenant_enabled": settings.tenant.multi_tenant_enabled,
        "tenant_id_header": settings.tenant.tenant_id_header,
    }


def register_control_plane_resources(container: Any, settings: Any) -> list[str]:
    from src.common.lifecycle import (
        CallbackDispatcherResource,
        CompensationServiceResource,
        CronSchedulerResource,
        TaskReconcilerResource,
    )

    manager = getattr(container, "lifecycle_manager", None)
    if manager is None:
        raise RuntimeError("LifecycleManager is not initialised in ServiceContainer")

    registered: list[str] = []

    if settings.background.reconcile.enabled and getattr(container, "task_reconciler", None):
        manager.register_resource(TaskReconcilerResource(container.task_reconciler))
        registered.append("task_reconciler")
    if settings.background.cron.enabled and getattr(container, "cron_scheduler", None):
        manager.register_resource(CronSchedulerResource(container.cron_scheduler))
        registered.append("cron_scheduler")
    if getattr(container, "compensation_service", None):
        manager.register_resource(CompensationServiceResource(container.compensation_service))
        registered.append("compensation_service")
    if getattr(container, "callback_dispatcher", None):
        manager.register_resource(CallbackDispatcherResource(container.callback_dispatcher))
        registered.append("callback_dispatcher")

    return registered
