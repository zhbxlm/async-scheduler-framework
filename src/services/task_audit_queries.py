from __future__ import annotations

from src.common.db_utils import maybe_await

from typing import Any

from sqlalchemy import select, func

from src.models.task_event import TaskEventRecord
from src.models.callback_outbox import CallbackOutboxRecord, CallbackDeliveryStatus




class TaskAuditQueryService:
    def __init__(self, session_factory: Any | None = None) -> None:
        self._db = session_factory

    async def get_task_timeline(self, task_id: str, limit: int = 100) -> list[TaskEventRecord]:
        if self._db is None:
            return []
        async with self._db() as session:
            result = await maybe_await(
                session.execute(
                    select(TaskEventRecord)
                    .where(TaskEventRecord.task_id == task_id)
                    .order_by(TaskEventRecord.created_at.asc())
                    .limit(limit)
                )
            )
            return list(result.scalars().all())

    async def get_callback_summary(self) -> dict[str, int]:
        if self._db is None:
            return {"pending": 0, "delivered": 0, "failed": 0, "dead_letter": 0, "total": 0}
        async with self._db() as session:
            rows = await maybe_await(
                session.execute(
                    select(CallbackOutboxRecord.delivery_status, func.count())
                    .group_by(CallbackOutboxRecord.delivery_status)
                )
            )
            summary = {"pending": 0, "delivered": 0, "failed": 0, "dead_letter": 0, "total": 0}
            for status, count in rows.fetchall():
                key = getattr(status, "value", status)
                summary[key] = int(count)
                summary["total"] += int(count)
            return summary
