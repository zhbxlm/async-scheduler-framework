from __future__ import annotations

import inspect
from typing import Any


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


class ReplayPolicyService:
    def __init__(self, *, redis_client: Any | None = None, session_factory: Any | None = None) -> None:
        self._redis = redis_client
        self._db = session_factory

    async def check_task_replay_allowed(self, task_id: str, *, reason: str | None = None) -> dict:
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

        return {
            "allowed": allowed,
            "task_id": task_id,
            "reasons": reasons or ["allowed"],
        }