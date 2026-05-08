from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.models.callback_outbox import CallbackDeliveryStatus
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


@pytest.mark.asyncio
async def test_callback_replay_loads_pending_status_from_db_and_denies():
    row = SimpleNamespace(id=1, delivery_status=CallbackDeliveryStatus.PENDING)
    session = AsyncMock()
    session.get = AsyncMock(return_value=row)

    class Factory:
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc, tb):
            return False

    def db_factory():
        return Factory()

    svc = CallbackReplayPolicyService(session_factory=db_factory)
    result = await svc.check_callback_replay_allowed(1, reason="retry", actor_role="operator")
    assert result["allowed"] is False
    assert "callback is already pending delivery" in result["reasons"]


@pytest.mark.asyncio
async def test_callback_replay_loads_delivered_status_from_db_and_requires_admin():
    row = SimpleNamespace(id=1, delivery_status=CallbackDeliveryStatus.DELIVERED)
    session = AsyncMock()
    session.get = AsyncMock(return_value=row)

    class Factory:
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc, tb):
            return False

    def db_factory():
        return Factory()

    svc = CallbackReplayPolicyService(session_factory=db_factory)
    result = await svc.check_callback_replay_allowed(1, reason="force", actor_role="operator")
    assert result["allowed"] is False
    assert "replay of already-delivered callback requires admin role" in result["reasons"]


@pytest.mark.asyncio
async def test_callback_replay_loads_dead_letter_status_from_db_and_allows_operator():
    row = SimpleNamespace(id=1, delivery_status=CallbackDeliveryStatus.DEAD_LETTER)
    session = AsyncMock()
    session.get = AsyncMock(return_value=row)

    class Factory:
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc, tb):
            return False

    def db_factory():
        return Factory()

    svc = CallbackReplayPolicyService(session_factory=db_factory)
    result = await svc.check_callback_replay_allowed(1, reason="retry after fix", actor_role="operator")
    assert result["allowed"] is True


@pytest.mark.asyncio
async def test_callback_replay_denied_when_outbox_record_missing():
    session = AsyncMock()
    session.get = AsyncMock(return_value=None)

    class Factory:
        async def __aenter__(self):
            return session
        async def __aexit__(self, exc_type, exc, tb):
            return False

    def db_factory():
        return Factory()

    svc = CallbackReplayPolicyService(session_factory=db_factory)
    result = await svc.check_callback_replay_allowed(999, reason="retry", actor_role="operator")
    assert result["allowed"] is False
    assert "callback outbox record not found" in result["reasons"]
