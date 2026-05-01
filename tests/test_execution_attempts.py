from __future__ import annotations

from datetime import datetime

import pytest
import pytest_asyncio

from async_scheduler.core.models import (
    ExecutionAttemptCreate,
    ExecutionAttemptStatus,
    TaskCreate,
)
from async_scheduler.persistence import ExecutionAttemptRepository, TaskRepository


@pytest.mark.asyncio
class TestExecutionAttempts:
    @pytest_asyncio.fixture
    async def db_session(self):
        from async_scheduler.persistence import get_session_no_context, init_db

        await init_db()

        async with await get_session_no_context() as session:
            yield session

    async def test_create_attempt(self, db_session) -> None:
        task = await TaskRepository.create(
            db_session,
            TaskCreate(name="attempt-task", payload={"x": 1}),
        )

        attempt = await ExecutionAttemptRepository.create(
            db_session,
            ExecutionAttemptCreate(
                task_id=task.id,
                worker_id="worker-a",
                retry_index=0,
                lease_token="lease-1",
            ),
        )

        assert attempt.id is not None
        assert attempt.task_id == task.id
        assert attempt.worker_id == "worker-a"
        assert attempt.status == ExecutionAttemptStatus.CLAIMED
        assert attempt.retry_index == 0
        assert attempt.lease_token == "lease-1"
        assert attempt.started_at is None
        assert attempt.completed_at is None

    async def test_update_attempt_started_and_heartbeat(self, db_session) -> None:
        task = await TaskRepository.create(
            db_session,
            TaskCreate(name="start-task", payload={}),
        )
        attempt = await ExecutionAttemptRepository.create(
            db_session,
            ExecutionAttemptCreate(
                task_id=task.id,
                worker_id="worker-b",
                retry_index=1,
                lease_token="lease-2",
            ),
        )

        started_at = datetime.utcnow()
        heartbeat_at = datetime.utcnow()
        updated = await ExecutionAttemptRepository.update(
            db_session,
            attempt.id,
            status=ExecutionAttemptStatus.RUNNING,
            started_at=started_at,
            last_heartbeat_at=heartbeat_at,
        )

        assert updated is not None
        assert updated.status == ExecutionAttemptStatus.RUNNING
        assert updated.started_at == started_at
        assert updated.last_heartbeat_at == heartbeat_at

    async def test_finalize_success(self, db_session) -> None:
        task = await TaskRepository.create(
            db_session,
            TaskCreate(name="success-task", payload={}),
        )
        attempt = await ExecutionAttemptRepository.create(
            db_session,
            ExecutionAttemptCreate(
                task_id=task.id,
                worker_id="worker-c",
                retry_index=0,
                lease_token="lease-3",
            ),
        )

        completed_at = datetime.utcnow()
        finalized = await ExecutionAttemptRepository.finalize(
            db_session,
            attempt.id,
            status=ExecutionAttemptStatus.SUCCEEDED,
            completed_at=completed_at,
            result_payload={"ok": True},
        )

        assert finalized is not None
        assert finalized.status == ExecutionAttemptStatus.SUCCEEDED
        assert finalized.completed_at == completed_at
        assert finalized.result_payload == {"ok": True}
        assert finalized.error_message is None

    async def test_finalize_failure(self, db_session) -> None:
        task = await TaskRepository.create(
            db_session,
            TaskCreate(name="failure-task", payload={}),
        )
        attempt = await ExecutionAttemptRepository.create(
            db_session,
            ExecutionAttemptCreate(
                task_id=task.id,
                worker_id="worker-d",
                retry_index=2,
                lease_token="lease-4",
            ),
        )

        finalized = await ExecutionAttemptRepository.finalize(
            db_session,
            attempt.id,
            status=ExecutionAttemptStatus.FAILED,
            error_message="boom",
        )

        assert finalized is not None
        assert finalized.status == ExecutionAttemptStatus.FAILED
        assert finalized.error_message == "boom"
        assert finalized.completed_at is not None

    async def test_get_latest_for_task(self, db_session) -> None:
        task = await TaskRepository.create(
            db_session,
            TaskCreate(name="latest-task", payload={}),
        )

        first = await ExecutionAttemptRepository.create(
            db_session,
            ExecutionAttemptCreate(
                task_id=task.id,
                worker_id="worker-a",
                retry_index=0,
                lease_token="lease-5",
            ),
        )
        second = await ExecutionAttemptRepository.create(
            db_session,
            ExecutionAttemptCreate(
                task_id=task.id,
                worker_id="worker-b",
                retry_index=1,
                lease_token="lease-6",
            ),
        )

        latest = await ExecutionAttemptRepository.get_latest_for_task(db_session, task.id)

        assert latest is not None
        assert latest.id == second.id
        assert latest.id != first.id
