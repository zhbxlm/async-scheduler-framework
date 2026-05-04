from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_task_event_completion_kind_roundtrip() -> None:
    from async_scheduler.platform.async_proxy import TaskEvent

    event = TaskEvent(task_id="t1", status="failed", completion_kind="lease_lost")
    restored = TaskEvent.deserialize(event.serialize())
    assert restored.completion_kind == "lease_lost"
