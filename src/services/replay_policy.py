from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select

from src.common.db_utils import maybe_await
from src.common.metrics import REPLAY_REQUESTS_TOTAL
from src.common.service_logger import log_service_event
from src.models.operator_action import OperatorActionRecord
from src.services.governance import GovernanceService


class ReplayPolicyService:
    def __init__(
        self,
        *,
        redis_client: Any | None = None,
        session_factory: Any | None = None,
        max_replays: int = 3,
        window_seconds: int = 300,
    ) -> None:
        self._redis = redis_client
        self._db = session_factory
        self._governance = GovernanceService(session_factory=session_factory)
        self._max_replays = max_replays
        self._window_seconds = window_seconds

    async def _count_recent_replays(self, task_id: str) -> int:
        if self._db is None:
            return 0
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self._window_seconds)
        async with self._db() as session:
            stmt = (
                select(func.count())
                .select_from(OperatorActionRecord)
                .where(OperatorActionRecord.target_id == task_id)
                .where(OperatorActionRecord.action_type == "task_replay_requested")
                .where(OperatorActionRecord.created_at >= cutoff)
            )
            result = await maybe_await(session.execute(stmt))
            return int(result.scalar() or 0)

    async def check_task_replay_allowed(
        self,
        task_id: str,
        *,
        reason: str | None = None,
        actor_role: str = "operator",
    ) -> dict:
        reasons: list[str] = []
        allowed = True

        if not reason:
            allowed = False
            reasons.append("replay reason is required")

        if self._redis is not None:
            lock_key = f"task_lock:{task_id}"
            lock_exists = await maybe_await(self._redis.exists(lock_key))
            if lock_exists:
                allowed = False
                reasons.append("active execution lease exists")
                if actor_role == "admin":
                    allowed = True
                    reasons.append("admin override for force replay on active lease")

        recent = await self._count_recent_replays(task_id)
        if recent >= self._max_replays:
            allowed = False
            reasons.append(
                f"replay rate limit exceeded: {recent}/{self._max_replays} replays in last {self._window_seconds}s"
            )

        result = {
            "allowed": allowed,
            "task_id": task_id,
            "reasons": reasons or ["allowed"],
        }
        try:
            REPLAY_REQUESTS_TOTAL.labels(actor_role=actor_role, allowed=str(allowed)).inc()
            log_service_event("ReplayPolicyService", "check_task_replay_allowed", task_id=task_id, actor_role=actor_role, outcome="allowed" if allowed else "denied")
        except Exception:
            pass
        return result
