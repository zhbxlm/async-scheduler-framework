from __future__ import annotations

from typing import Any

from src.services.governance import GovernanceService


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

        # Reason is always required
        if not reason:
            allowed = False
            reasons.append("replay reason is required")

        # Cannot replay something already queued for delivery
        if current_status == "pending":
            allowed = False
            reasons.append("callback is already pending delivery")

        # Replaying already-delivered callbacks requires admin
        if current_status == "delivered":
            if actor_role == "admin":
                reasons.append("admin override for replay of already-delivered callback")
            else:
                allowed = False
                reasons.append("replay of already-delivered callback requires admin role")

        # For all other statuses (dead_letter, failed): operator+ is fine
        # No additional role check needed beyond the delivered guard above

        return {
            "allowed": allowed,
            "outbox_id": outbox_id,
            "reasons": reasons or ["allowed"],
        }
