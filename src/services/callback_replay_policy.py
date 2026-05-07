from __future__ import annotations

import inspect
from typing import Any

from src.services.governance import GovernanceService


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


class CallbackReplayPolicyService:
    """Policy checks specific to callback replay, distinct from task replay."""

    def __init__(self, *, session_factory: Any | None = None) -> None:
        self._db = session_factory
        self._governance = GovernanceService(session_factory=session_factory)

    async def check_callback_replay_allowed(
        self,
        outbox_id: int,
        *,
        reason: str | None = None,
        actor_role: str = "operator",
        current_status: str | None = None,
    ) -> dict:
        reasons: list[str] = []
        allowed = True

        if not reason:
            allowed = False
            reasons.append("replay reason is required")

        if current_status == "pending":
            allowed = False
            reasons.append("callback is already pending delivery")

        if current_status == "delivered":
            if actor_role != "admin":
                allowed = False
                reasons.append("replay of already-delivered callback requires admin role")
            else:
                reasons.append("admin override for replay of already-delivered callback")

        required_roles = self._governance.allowed_role_categories(
            "force_replay_on_active_lease" if current_status == "delivered" else "ordinary_replay"
        )
        if actor_role not in required_roles and actor_role != "admin":
            allowed = False
            reasons.append(f"operation requires role in {required_roles}")

        return {
            "allowed": allowed,
            "outbox_id": outbox_id,
            "reasons": reasons or ["allowed"],
        }
