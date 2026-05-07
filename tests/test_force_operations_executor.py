from __future__ import annotations

import pytest

from src.services.force_operations import ForceOperationService


@pytest.mark.asyncio
async def test_force_operation_calls_executor():
    svc = ForceOperationService()
    executed = []

    async def executor():
        executed.append(True)
        return {"evicted": True, "key": "task_lock:t1"}

    result = await svc.execute_force_operation(
        operation="force_lease_eviction",
        actor="admin_user",
        actor_role="admin",
        reason="stale lock",
        target_type="task",
        target_id="t1",
        executor=executor,
    )
    assert result["ok"] is True
    assert result["side_effect"]["evicted"] is True
    assert len(executed) == 1


@pytest.mark.asyncio
async def test_force_operation_denied_does_not_call_executor():
    svc = ForceOperationService()
    executed = []

    async def executor():
        executed.append(True)
        return {"evicted": True}

    result = await svc.execute_force_operation(
        operation="force_lease_eviction",
        actor="alice",
        actor_role="operator",
        reason="stale lock",
        target_type="task",
        target_id="t1",
        executor=executor,
    )
    assert result["ok"] is False
    assert len(executed) == 0
