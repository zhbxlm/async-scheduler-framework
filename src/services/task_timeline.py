from __future__ import annotations


import json
from typing import Any

from src.common.db_utils import maybe_await
from src.models.task_event import TaskEventRecord




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
            await maybe_await(session.commit())
