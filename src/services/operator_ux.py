from __future__ import annotations

from typing import Any

from sqlalchemy import select

from src.common.db_utils import maybe_await
from src.models.callback_outbox import CallbackDeliveryStatus, CallbackOutboxRecord
from src.models.operator_action import OperatorActionRecord
from src.models.task_run import TaskRunRecord


class OperatorUXService:
    """Aggregated operator UX queries — broader than dashboard summary."""

    def __init__(self, session_factory: Any | None = None) -> None:
        self._db = session_factory

    async def recent_operator_actions(self, *, limit: int = 20, action_type: str | None = None) -> list[dict]:
        if self._db is None:
            return []
        async with self._db() as session:
            stmt = select(OperatorActionRecord).order_by(OperatorActionRecord.created_at.desc()).limit(limit)
            if action_type:
                stmt = stmt.where(OperatorActionRecord.action_type == action_type)
            result = await maybe_await(session.execute(stmt))
            rows = list(result.scalars().all())
        return [
            {
                "id": r.id,
                "actor": r.actor,
                "action_type": r.action_type,
                "target_type": r.target_type,
                "target_id": r.target_id,
                "reason": r.reason,
                "created_at": r.created_at.isoformat() if getattr(r, "created_at", None) else None,
            }
            for r in rows
        ]

    async def stale_task_queue(self, *, stale_status: str = "running", limit: int = 50) -> list[dict]:
        if self._db is None:
            return []
        async with self._db() as session:
            stmt = (
                select(TaskRunRecord)
                .where(TaskRunRecord.status == stale_status)
                .order_by(TaskRunRecord.created_at.asc())
                .limit(limit)
            )
            result = await maybe_await(session.execute(stmt))
            rows = list(result.scalars().all())
        return [
            {
                "task_id": r.task_id,
                "run_key": r.run_key,
                "status": r.status,
                "created_at": r.created_at.isoformat() if getattr(r, "created_at", None) else None,
            }
            for r in rows
        ]

    async def replay_queue(self, *, limit: int = 50) -> list[dict]:
        return await self.stale_task_queue(stale_status="replay_requested", limit=limit)

    async def dead_letter_backlog(self, *, limit: int = 50) -> list[dict]:
        if self._db is None:
            return []
        async with self._db() as session:
            stmt = (
                select(CallbackOutboxRecord)
                .where(CallbackOutboxRecord.delivery_status == CallbackDeliveryStatus.DEAD_LETTER)
                .order_by(CallbackOutboxRecord.created_at.asc())
                .limit(limit)
            )
            result = await maybe_await(session.execute(stmt))
            rows = list(result.scalars().all())
        return [
            {
                "id": r.id,
                "task_id": r.task_id,
                "delivery_status": getattr(r.delivery_status, "value", r.delivery_status),
                "acknowledged_by": getattr(r, "acknowledged_by", None),
                "created_at": r.created_at.isoformat() if getattr(r, "created_at", None) else None,
            }
            for r in rows
        ]
