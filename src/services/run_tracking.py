from __future__ import annotations

import uuid
from typing import Any

from src.common.db_utils import maybe_await
from src.models.dag_run import DagRunRecord
from src.models.task_run import TaskRunRecord


class RunTrackingService:
    def __init__(self, session_factory: Any | None = None) -> None:
        self._db = session_factory

    async def create_task_run(
        self,
        *,
        task_id: str,
        attempt: int = 0,
        status: str = "created",
        worker_id: str | None = None,
        lease_id: str | None = None,
        run_key: str | None = None,
    ) -> str | None:
        if self._db is None:
            return None
        run_key = run_key or f"taskrun-{task_id[:8]}-{uuid.uuid4().hex[:8]}"
        async with self._db() as session:
            session.add(TaskRunRecord(
                task_id=task_id,
                run_key=run_key,
                attempt=attempt,
                status=status,
                worker_id=worker_id,
                lease_id=lease_id,
            ))
            await maybe_await(session.commit())
        return run_key

    async def create_dag_run(
        self,
        *,
        dag_id: str,
        tenant_id: str = "",
        status: str = "created",
        trigger_source: str | None = None,
        input_json: str | None = None,
        run_key: str | None = None,
    ) -> str | None:
        if self._db is None:
            return None
        run_key = run_key or f"dagrun-{dag_id[:8]}-{uuid.uuid4().hex[:8]}"
        async with self._db() as session:
            session.add(DagRunRecord(
                dag_id=dag_id,
                run_key=run_key,
                tenant_id=tenant_id,
                status=status,
                trigger_source=trigger_source,
                input_json=input_json,
            ))
            await maybe_await(session.commit())
        return run_key
