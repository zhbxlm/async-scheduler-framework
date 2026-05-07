from __future__ import annotations

import pytest

from src.services.callback_replay_policy import CallbackReplayPolicyService


@pytest.mark.asyncio
async def test_callback_replay_requires_reason():
    svc = CallbackReplayPolicyService()
    result = await svc.check_callback_replay_allowed(1, reason=None)
    assert result["allowed"] is False
    assert "replay reason is required" in result["reasons"]


@pytest.mark.asyncio
async def test_callback_replay_denied_if_already_pending():
    svc = CallbackReplayPolicyService()
    result = await svc.check_callback_replay_allowed(1, reason="retry", current_status="pending")
    assert result["allowed"] is False
    assert "callback is already pending delivery" in result["reasons"]


@pytest.mark.asyncio
async def test_callback_replay_delivered_requires_admin():
    svc = CallbackReplayPolicyService()
    result = await svc.check_callback_replay_allowed(1, reason="force", current_status="delivered", actor_role="operator")
    assert result["allowed"] is False
    assert "replay of already-delivered callback requires admin role" in result["reasons"]


@pytest.mark.asyncio
async def test_callback_replay_delivered_allowed_for_admin():
    svc = CallbackReplayPolicyService()
    result = await svc.check_callback_replay_allowed(1, reason="force", current_status="delivered", actor_role="admin")
    assert result["allowed"] is True


@pytest.mark.asyncio
async def test_callback_replay_allowed_dead_letter_with_reason():
    svc = CallbackReplayPolicyService()
    result = await svc.check_callback_replay_allowed(1, reason="retry after fix", current_status="dead_letter", actor_role="operator")
    assert result["allowed"] is True
