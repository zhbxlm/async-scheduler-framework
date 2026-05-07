from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.force_operations import ForceOperationService


@pytest.mark.asyncio
async def test_force_operation_denied_insufficient_role():
    svc = ForceOperationService()
    result = await svc.execute_force_operation(
        operation="force_replay_on_active_lease",
        actor="alice",
        actor_role="operator",
        reason="manual",
        target_type="task",
        target_id="t1",
    )
    assert result["ok"] is False
    assert result["error"] == "insufficient role"


@pytest.mark.asyncio
async def test_force_operation_denied_no_reason_for_high_risk():
    svc = ForceOperationService()
    result = await svc.execute_force_operation(
        operation="force_replay_on_active_lease",
        actor="admin_user",
        actor_role="admin",
        reason=None,
        target_type="task",
        target_id="t1",
    )
    assert result["ok"] is False
    assert result["error"] == "reason required for high-risk force operation"


@pytest.mark.asyncio
async def test_force_operation_allowed_for_admin_with_reason():
    svc = ForceOperationService(session_factory=None)
    result = await svc.execute_force_operation(
        operation="force_replay_on_active_lease",
        actor="admin_user",
        actor_role="admin",
        reason="lease expired but lock remains",
        target_type="task",
        target_id="t1",
    )
    assert result["ok"] is True
    assert result["is_high_risk"] is True


@pytest.mark.asyncio
async def test_ordinary_operation_allowed_for_operator():
    svc = ForceOperationService(session_factory=None)
    result = await svc.execute_force_operation(
        operation="ordinary_replay",
        actor="alice",
        actor_role="operator",
        reason="retry",
        target_type="task",
        target_id="t1",
    )
    assert result["ok"] is True
    assert result["is_high_risk"] is False
