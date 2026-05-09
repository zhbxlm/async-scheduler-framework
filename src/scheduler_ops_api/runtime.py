"""ops-api runtime shim — lifecycle helpers for container startup/shutdown."""
from __future__ import annotations
from typing import Any
from fastapi import FastAPI


def mount_container_state(app: FastAPI, container: Any, names: tuple[str, ...]) -> None:
    """Mount container attributes onto app.state for dependency injection."""
    for name in names:
        val = getattr(container, name, None)
        setattr(app.state, name, val)


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
