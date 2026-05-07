from __future__ import annotations

from src.common.db_utils import maybe_await

from typing import Any

from sqlalchemy import select

from src.models.task_run import TaskRunRecord




class ReplayChainQueryService:
    """Query the replay lineage chain for a task: ordered list of runs by replay ancestry."""

    def __init__(self, session_factory: Any | None = None) -> None:
        self._db = session_factory

    async def get_replay_chain(self, task_id: str, limit: int = 50) -> list[dict]:
        if self._db is None:
            return []
        async with self._db() as session:
            stmt = (
                select(TaskRunRecord)
                .where(TaskRunRecord.task_id == task_id)
                .order_by(TaskRunRecord.created_at.asc())
                .limit(limit)
            )
            result = await maybe_await(session.execute(stmt))
            rows = list(result.scalars().all())
        return [
            {
                "run_key": r.run_key,
                "status": r.status,
                "attempt": r.attempt,
                "trigger_source": getattr(r, "trigger_source", None),
                "created_at": r.created_at.isoformat() if getattr(r, "created_at", None) else None,
            }
            for r in rows
        ]
