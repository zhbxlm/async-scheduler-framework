from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_task_event_carries_callback_delivery_field() -> None:
    from async_scheduler.platform.async_proxy import TaskEvent

    event = TaskEvent(task_id="t1", status="success", callback_delivery="delivered")
    restored = TaskEvent.deserialize(event.serialize())
    assert restored.callback_delivery == "delivered"
