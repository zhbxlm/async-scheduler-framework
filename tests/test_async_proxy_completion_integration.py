from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock

import pytest


@pytest.mark.asyncio
async def test_completion_node_publishes_async_proxy_event_on_success() -> None:
    from async_scheduler.core.models import Task, TaskStatus
    from async_scheduler.platform.async_proxy import AsyncProxySidecar
    from async_scheduler.platform.completion import TaskCompletionNode

    sidecar = AsyncProxySidecar()
    sidecar.publish = AsyncMock()
    node = TaskCompletionNode(async_proxy_sidecar=sidecar)

    task = Task(
        id="task-1",
        name="demo",
        priority=0,
        payload={},
        status=TaskStatus.RUNNING,
        tenant_id="tenant-a",
        retry_count=0,
        max_retries=0,
        timeout_seconds=30,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    node._publish_async_proxy_event = AsyncMock()
    await node._publish_async_proxy_event(task, TaskStatus.SUCCESS, {"ok": True}, None)
    node._publish_async_proxy_event.assert_awaited_once()


@pytest.mark.asyncio
async def test_service_container_wires_completion_node_to_async_proxy() -> None:
    from async_scheduler.platform.services import build_service_container

    services = await build_service_container()
    assert services.async_proxy_sidecar is not None
    assert services.completion_node is not None
    assert getattr(services.completion_node, "async_proxy_sidecar", None) is services.async_proxy_sidecar
