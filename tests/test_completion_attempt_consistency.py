from __future__ import annotations

from datetime import datetime, timedelta

import pytest
import pytest_asyncio

from async_scheduler.core.models import ExecutionAttemptCreate, ExecutionAttemptStatus, TaskCreate, TaskStatus
from async_scheduler.persistence import (
    ExecutionAttemptRepository,
    TaskRepository,
    drop_db,
    get_session,
    get_session_no_context,
    init_db,
)
from async_scheduler.platform.completion import TaskCompletionNode


@pytest.mark.asyncio
class TestCompletionAttemptConsistency:
    @pytest_asyncio.fixture(autouse=True)
    async def init_database(self):
        await drop_db()
        await init_db()
        yield

    async def test_finalize_can_converge_latest_attempt_to_succeeded(self) -> None:
        node = TaskCompletionNode()

        async with get_session() as session:
            task = await TaskRepository.create(session, TaskCreate(name="attempt-success", payload={}))
            await TaskRepository.update(
                session,
                task.id,
                status=TaskStatus.RUNNING,
                started_at=datetime.utcnow() - timedelta(seconds=1),
            )
            attempt = await ExecutionAttemptRepository.create(
                session,
                ExecutionAttemptCreate(
                    task_id=task.id,
                    worker_id="worker-a",
                    retry_index=0,
                    lease_token="lease-a",
                ),
            )
            await ExecutionAttemptRepository.update(
                session,
                attempt.id,
                status=ExecutionAttemptStatus.RUNNING,
                started_at=datetime.utcnow() - timedelta(seconds=1),
            )

        await node.finalize(task, TaskStatus.SUCCESS, result={"ok": True}, finalize_latest_attempt=True)

        async with await get_session_no_context() as session:
            latest_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

        assert latest_attempt is not None
        assert latest_attempt.status == ExecutionAttemptStatus.SUCCEEDED
        assert latest_attempt.result_payload == {"ok": True}

    async def test_finalize_does_not_override_terminal_attempt(self) -> None:
        node = TaskCompletionNode()

        async with get_session() as session:
            task = await TaskRepository.create(session, TaskCreate(name="attempt-terminal", payload={}))
            await TaskRepository.update(session, task.id, status=TaskStatus.RUNNING)
            attempt = await ExecutionAttemptRepository.create(
                session,
                ExecutionAttemptCreate(
                    task_id=task.id,
                    worker_id="worker-b",
                    retry_index=0,
                    lease_token="lease-b",
                ),
            )
            await ExecutionAttemptRepository.finalize(
                session,
                attempt.id,
                status=ExecutionAttemptStatus.ABANDONED,
                error_message="lost lease",
            )

        await node.finalize(task, TaskStatus.SUCCESS, result={"ok": True}, finalize_latest_attempt=True)

        async with await get_session_no_context() as session:
            latest_attempt = await ExecutionAttemptRepository.get_latest_for_task(session, task.id)

        assert latest_attempt is not None
        assert latest_attempt.status == ExecutionAttemptStatus.ABANDONED
        assert latest_attempt.error_message == "lost lease"
