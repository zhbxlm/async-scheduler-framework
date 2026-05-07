from __future__ import annotations

from src.common.db_utils import maybe_await

from typing import Any

from sqlalchemy import select

from src.models.dag_run import DagRunRecord
from src.models.task_event import TaskEventRecord
from src.models.task_run import TaskRunRecord




class RunCentricQueryService:
    def __init__(self, session_factory: Any | None = None) -> None:
        self._db = session_factory

    async def list_task_runs(self, task_id: str, limit: int = 100) -> list[TaskRunRecord]:
        if self._db is None:
            return []
        async with self._db() as session:
            stmt = (
                select(TaskRunRecord)
                .where(TaskRunRecord.task_id == task_id)
                .order_by(TaskRunRecord.created_at.desc())
                .limit(limit)
            )
            result = await maybe_await(session.execute(stmt))
            return list(result.scalars().all())

    async def list_task_run_events(self, task_id: str, run_key: str | None = None, limit: int = 100) -> list[TaskEventRecord]:
        if self._db is None:
            return []
        async with self._db() as session:
            stmt = (
                select(TaskEventRecord)
                .where(TaskEventRecord.task_id == task_id)
                .order_by(TaskEventRecord.created_at.desc())
                .limit(limit)
            )
            if run_key:
                stmt = stmt.where(TaskEventRecord.run_key == run_key)
            result = await maybe_await(session.execute(stmt))
            return list(result.scalars().all())
    
    async def list_dag_runs(self, dag_id: str, limit: int = 100) -> list[DagRunRecord]:
        if self._db is None:
            return []
        async with self._db() as session:
            stmt = (
                select(DagRunRecord)
                .where(DagRunRecord.dag_id == dag_id)
                .order_by(DagRunRecord.created_at.desc())
                .limit(limit)
            )
            result = await maybe_await(session.execute(stmt))
            return list(result.scalars().all())
