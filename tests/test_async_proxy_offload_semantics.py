from __future__ import annotations

from datetime import datetime

import pytest


@pytest.mark.asyncio
async def test_task_event_carries_completion_context_fields() -> None:
    from async_scheduler.platform.async_proxy import TaskEvent

    event = TaskEvent(
        task_id="task-ctx-1",
        status="success",
        task_name="demo-task",
        tenant_id="tenant-a",
        callback_url="https://example.com/callback",
        completed_at=12345.0,
    )

    restored = TaskEvent.deserialize(event.serialize())
    assert restored.task_name == "demo-task"
    assert restored.tenant_id == "tenant-a"
    assert restored.callback_url == "https://example.com/callback"
    assert restored.completed_at == 12345.0
