from __future__ import annotations

import pytest
import pytest_asyncio

from async_scheduler.backends.redis import RedisCompletionDedupBackend
from async_scheduler.core.models import TaskCreate, TaskStatus
from async_scheduler.persistence import drop_db, TaskRepository, get_session, get_session_no_context, init_db
from async_scheduler.platform.completion import TaskCompletionNode


class RecordingDispatcher:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def dispatch(self, url: str, payload: dict) -> None:
        self.calls.append((url, payload))


@pytest.mark.asyncio
class TestCompletionIdempotency:
    @pytest_asyncio.fixture(autouse=True)
    async def init_database(self):
        await drop_db()
        await init_db()
        yield

    async def test_duplicate_success_completion_only_processes_once(self) -> None:
        dispatcher = RecordingDispatcher()
        dedup = RedisCompletionDedupBackend(redis_url="redis://localhost:6379/0")
        node = TaskCompletionNode(callback_dispatcher=dispatcher, dedup_backend=dedup)
        completions: list[str] = []
        node.register_completion_handler(TaskStatus.SUCCESS, lambda task: completions.append(task.id))

        async with get_session() as session:
            task = await TaskRepository.create(
                session,
                TaskCreate(name="success-idempotent", payload={}, callback_url="https://callback/success"),
            )

        await node.finalize(task, TaskStatus.SUCCESS, result={"ok": True})
        await node.finalize(task, TaskStatus.SUCCESS, result={"ok": True})

        async with await get_session_no_context() as session:
            stored = await TaskRepository.get(session, task.id)

        assert stored is not None
        assert stored.status == TaskStatus.SUCCESS
        assert len(dispatcher.calls) == 1
        assert completions == [task.id]

    async def test_duplicate_failure_completion_only_processes_once(self) -> None:
        dispatcher = RecordingDispatcher()
        dedup = RedisCompletionDedupBackend(redis_url="redis://localhost:6379/0")
        node = TaskCompletionNode(callback_dispatcher=dispatcher, dedup_backend=dedup)
        completions: list[str] = []
        node.register_completion_handler(TaskStatus.FAILED, lambda task: completions.append(task.id))

        async with get_session() as session:
            task = await TaskRepository.create(
                session,
                TaskCreate(name="failure-idempotent", payload={}, callback_url="https://callback/failure"),
            )

        await node.finalize(task, TaskStatus.FAILED, error_message="boom")
        await node.finalize(task, TaskStatus.FAILED, error_message="boom")

        assert len(dispatcher.calls) == 1
        assert completions == [task.id]

    async def test_repeated_callback_delivery_remains_convergent(self) -> None:
        dispatcher = RecordingDispatcher()
        dedup = RedisCompletionDedupBackend(redis_url="redis://localhost:6379/0")
        node = TaskCompletionNode(callback_dispatcher=dispatcher, dedup_backend=dedup)

        async with get_session() as session:
            task = await TaskRepository.create(
                session,
                TaskCreate(name="convergent", payload={}, callback_url="https://callback/convergent"),
            )

        first = await node.finalize(task, TaskStatus.SUCCESS, result={"value": 1})
        second = await node.finalize(task, TaskStatus.SUCCESS, result={"value": 999})

        assert first is not None
        assert second is not None
        assert len(dispatcher.calls) == 1
        assert dispatcher.calls[0][1]["result"] == {"value": 1}

    async def test_two_completion_nodes_share_dedup_backend_semantics(self) -> None:
        dispatcher_a = RecordingDispatcher()
        dispatcher_b = RecordingDispatcher()
        dedup = RedisCompletionDedupBackend(redis_url="redis://localhost:6379/0")
        node_a = TaskCompletionNode(callback_dispatcher=dispatcher_a, dedup_backend=dedup)
        node_b = TaskCompletionNode(callback_dispatcher=dispatcher_b, dedup_backend=dedup)

        async with get_session() as session:
            task = await TaskRepository.create(
                session,
                TaskCreate(name="shared-dedup", payload={}, callback_url="https://callback/shared"),
            )

        await node_a.finalize(task, TaskStatus.SUCCESS, result={"from": "a"})
        await node_b.finalize(task, TaskStatus.SUCCESS, result={"from": "b"})

        assert len(dispatcher_a.calls) + len(dispatcher_b.calls) == 1
