from __future__ import annotations

import inspect
import json
from typing import Any

from src.models.task_event import TaskEventRecord


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


class TaskTimelineService:
    def __init__(self, session_factory: Any | None = None) -> None:
        self._db = session_factory

    async def emit(self, *, task_id: str, event_type: str, payload: dict | None = None, run_key: str | None = None) -> None:
        if self._db is None:
            return
        async with self._db() as session:
            session.add(TaskEventRecord(
                task_id=task_id,
                run_key=run_key,
                event_type=event_type,
                event_payload=json.dumps(payload or {}, ensure_ascii=False),
            ))
            await _maybe_await(session.commit())
