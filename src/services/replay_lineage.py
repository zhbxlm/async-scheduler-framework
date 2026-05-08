from __future__ import annotations

from typing import Any

from src.services.operator_actions import OperatorActionService
from src.services.run_tracking import RunTrackingService
from src.services.task_timeline import TaskTimelineService


class ReplayLineageService:
    def __init__(self, session_factory: Any | None = None) -> None:
        self._db = session_factory
        self._runs = RunTrackingService(session_factory) if session_factory else None
        self._ops = OperatorActionService(session_factory) if session_factory else None
        self._timeline = TaskTimelineService(session_factory) if session_factory else None

    async def replay_task(
        self,
        *,
        task_id: str,
        actor: str,
        reason: str | None = None,
        from_run_key: str | None = None,
    ) -> dict:
        if self._db is None:
            return {"ok": False, "task_id": task_id}
        new_run_key = await self._runs.create_task_run(
            task_id=task_id,
            status="replay_requested",
        ) if self._runs else None
        if self._ops:
            await self._ops.record(
                actor=actor,
                action_type="task_replay_requested",
                target_type="task",
                target_id=task_id,
                reason=reason,
                payload={"from_run_key": from_run_key or "", "new_run_key": new_run_key or ""},
                task_id=task_id,
            )
        if self._timeline:
            await self._timeline.emit(
                task_id=task_id,
                run_key=new_run_key,
                event_type="task_replay_requested",
                payload={"actor": actor, "reason": reason or "", "from_run_key": from_run_key or "", "new_run_key": new_run_key or ""},
            )
        return {"ok": True, "task_id": task_id, "new_run_key": new_run_key, "from_run_key": from_run_key}
