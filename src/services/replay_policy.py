from __future__ import annotations

import inspect
from typing import Any

from src.services.governance import GovernanceService


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


class ReplayPolicyService:
    def __init__(self, *, redis_client: Any | None = None, session_factory: Any | None = None) -> None:
        self._redis = redis_client
        self._db = session_factory
        self._governance = GovernanceService(session_factory=session_factory)

    async def check_task_replay_allowed(self, task_id: str, *, reason: str | None = None, actor_role: str = "operator") -> dict:
        reasons: list[str] = []
        allowed = True

        if not reason:
            allowed = False
            reasons.append("replay reason is required")

        if self._redis is not None:
            lock_key = f"task_lock:{task_id}"
            lock_exists = await _maybe_await(self._redis.exists(lock_key))
            if lock_exists:
                allowed = False
                reasons.append("active execution lease exists")
                if actor_role == "admin":
                    allowed = True
                    reasons.append("admin override for force replay on active lease")

        return {
            "allowed": allowed,
            "task_id": task_id,
            "reasons": reasons or ["allowed"],
        }
