from __future__ import annotations

import inspect
from typing import Any

from sqlalchemy import select, func

from src.models.operator_action import OperatorActionRecord
from src.models.task_event import TaskEventRecord
from src.models.callback_outbox import CallbackOutboxRecord, CallbackDeliveryStatus


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


class OperatorDashboardService:
    def __init__(self, session_factory: Any | None = None) -> None:
        self._db = session_factory

    async def summary(self) -> dict[str, Any]:
        if self._db is None:
            return {"callback": {}, "operator_actions": 0, "recent_timeline_events": 0}
        async with self._db() as session:
            cb_rows = await _maybe_await(
                session.execute(select(CallbackOutboxRecord.delivery_status, func.count()).group_by(CallbackOutboxRecord.delivery_status))
            )
            action_rows = await _maybe_await(session.execute(select(func.count()).select_from(OperatorActionRecord)))
            event_rows = await _maybe_await(session.execute(select(func.count()).select_from(TaskEventRecord)))
            callback = {"pending": 0, "delivered": 0, "failed": 0, "dead_letter": 0, "total": 0}
            for status, count in cb_rows.fetchall():
                key = getattr(status, "value", status)
                callback[key] = int(count)
                callback["total"] += int(count)
            operator_actions = int(action_rows.scalar() or 0)
            timeline_events = int(event_rows.scalar() or 0)
            return {
                "callback": callback,
                "operator_actions": operator_actions,
                "recent_timeline_events": timeline_events,
            }
