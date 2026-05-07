from __future__ import annotations

from src.common.db_utils import maybe_await

import json
from typing import Any

from src.models.operator_action import OperatorActionRecord
from src.services.task_timeline import TaskTimelineService
from src.common.metrics import OPERATOR_ACTIONS_TOTAL
from src.common.service_logger import log_service_event




class OperatorActionService:
    def __init__(self, session_factory: Any | None = None) -> None:
        self._db = session_factory
        self._timeline = TaskTimelineService(session_factory) if session_factory else None

    async def record(
        self,
        *,
        actor: str,
        action_type: str,
        target_type: str,
        target_id: str,
        reason: str | None = None,
        payload: dict | None = None,
        task_id: str | None = None,
    ) -> int | None:
        if self._db is None:
            return None
        async with self._db() as session:
            row = OperatorActionRecord(
                actor=actor,
                action_type=action_type,
                target_type=target_type,
                target_id=target_id,
                reason=reason,
                payload_json=json.dumps(payload or {}, ensure_ascii=False),
            )
            session.add(row)
            await maybe_await(session.commit())
        if self._timeline and task_id:
            await self._timeline.emit(
                task_id=task_id,
                event_type=f"operator_{action_type}",
                payload={"actor": actor, "reason": reason or "", **(payload or {})},
            )
        try:
            OPERATOR_ACTIONS_TOTAL.labels(action_type=action_type).inc()
            log_service_event("OperatorActionService", "record", task_id=task_id, actor=actor, action_type=action_type, outcome="ok")
        except Exception:
            pass
        return getattr(row, "id", None)
