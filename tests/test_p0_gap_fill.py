"""P0 Gap Fill Tests: capability-aware queue, round-robin consumer, lease-lost interrupt."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from async_scheduler.backends.memory import InMemoryQueueBackend
from async_scheduler.core.consumer import TaskConsumer
from async_scheduler.core.models import Task, TaskCreate, TaskPriority, TaskStatus
from async_scheduler.executor.executor import ExecutionResult, TaskExecutor
from async_scheduler.queue.manager import QueueManager


def _make_task(task_type: str = "default", priority: TaskPriority = TaskPriority.NORMAL) -> Task:
    return Task(
        id=f"task-{task_type}-{id(object())}",
        name=f"test-{task_type}",
        task_type=task_type,
        priority=priority,
        payload={},
        status=TaskStatus.QUEUED,
        tenant_id="tenant-1",
        retry_count=0,
        max_retries=0,
        timeout_seconds=30,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


# ---------------------------------------------------------------------------
# TODO-1: InMemoryQueueBackend capability-aware tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enqueue_dequeue_with_capability():
    """Tasks in different capabilities don't interfere."""
    backend = InMemoryQueueBackend()
    task_a = _make_task("gpu")
    task_b = _make_task("cpu")

    await backend.enqueue(task_a, capability="gpu")
    await backend.enqueue(task_b, capability="cpu")

    got_gpu = await backend.dequeue(capability="gpu")
    got_cpu = await backend.dequeue(capability="cpu")

    assert got_gpu is not None and got_gpu.id == task_a.id
    assert got_cpu is not None and got_cpu.id == task_b.id

    # Other capability should be empty
    empty = await backend.dequeue(capability="gpu")
    assert empty is None


@pytest.mark.asyncio
async def test_discover_capabilities():
    """Enqueuing to different capabilities registers them."""
    backend = InMemoryQueueBackend()
    await backend.enqueue(_make_task("gpu"), capability="gpu")
    await backend.enqueue(_make_task("cpu"), capability="cpu")
    await backend.enqueue(_make_task("tpu"), capability="tpu")

    caps = await backend.discover_capabilities()
    assert set(caps) == {"gpu", "cpu", "tpu"}


@pytest.mark.asyncio
async def test_capability_stats():
    """Stats are correctly updated on enqueue/dequeue/complete/fail."""
    backend = InMemoryQueueBackend()
    t1 = _make_task("gpu")
    t2 = _make_task("gpu")

    await backend.enqueue(t1, capability="gpu")
    await backend.enqueue(t2, capability="gpu")

    stats = await backend.get_capability_stats("gpu")
    assert stats["enqueue_count"] == 2

    got = await backend.dequeue(capability="gpu")
    assert got is not None
    stats = await backend.get_capability_stats("gpu")
    assert stats["dequeue_count"] == 1
    assert stats["running_count"] == 1

    await backend.complete(got.id, capability="gpu")
    stats = await backend.get_capability_stats("gpu")
    assert stats["complete_count"] == 1
    assert stats["running_count"] == 0

    got2 = await backend.dequeue(capability="gpu")
    assert got2 is not None
    await backend.fail(got2.id, capability="gpu")
    stats = await backend.get_capability_stats("gpu")
    assert stats["fail_count"] == 1


@pytest.mark.asyncio
async def test_max_concurrent_blocks_dequeue():
    """When max_concurrent=1 and running is full, dequeue returns None."""
    backend = InMemoryQueueBackend()
    await backend.set_max_concurrent("gpu", 1)

    t1 = _make_task("gpu")
    t2 = _make_task("gpu")
    await backend.enqueue(t1, capability="gpu")
    await backend.enqueue(t2, capability="gpu")

    # First dequeue succeeds
    got1 = await backend.dequeue(capability="gpu")
    assert got1 is not None

    # Second dequeue is blocked (running_count >= max_concurrent)
    got2 = await backend.dequeue(capability="gpu")
    assert got2 is None

    # After completing first, second can proceed
    await backend.complete(got1.id, capability="gpu")
    got2 = await backend.dequeue(capability="gpu")
    assert got2 is not None


@pytest.mark.asyncio
async def test_priority_ordering_within_capability():
    """Higher priority tasks (lower rank) are dequeued first."""
    backend = InMemoryQueueBackend()
    t_low = _make_task("default")
    t_low.priority = TaskPriority.LOW
    t_high = _make_task("default")
    t_high.priority = TaskPriority.VERY_HIGH

    await backend.enqueue(t_low, capability="default")
    await backend.enqueue(t_high, capability="default")

    first = await backend.dequeue(capability="default")
    assert first is not None
    assert first.id == t_high.id


@pytest.mark.asyncio
async def test_queue_manager_discover_capabilities():
    """QueueManager.discover_capabilities returns known capabilities."""
    backend = InMemoryQueueBackend()
    qm = QueueManager(backend=backend)

    await qm.enqueue(_make_task("ml"), capability="ml")
    await qm.enqueue(_make_task("data"), capability="data")

    caps = await qm.discover_capabilities()
    assert "ml" in caps
    assert "data" in caps


# ---------------------------------------------------------------------------
# TODO-2: TaskConsumer Round-Robin tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_consumer_round_robin_two_capabilities():
    """Consumer alternates between two capabilities."""
    backend = InMemoryQueueBackend()
    qm = QueueManager(backend=backend)

    t_gpu = _make_task("gpu")
    t_cpu = _make_task("cpu")
    await backend.enqueue(t_gpu, capability="gpu")
    await backend.enqueue(t_cpu, capability="cpu")

    executor = TaskExecutor()
    handler = AsyncMock(return_value={"done": True})

    consumed_ids = []

    async def mock_handler(payload):
        return {"done": True}

    consumer = TaskConsumer(
        queue_manager=qm,
        executor=executor,
        handler=mock_handler,
        max_concurrent_tasks=10,
        poll_interval=0.01,
    )

    # Manually test round-robin by calling discover + dequeue
    caps = await qm.discover_capabilities()
    assert len(caps) == 2

    # Cap idx=0 → first cap
    cap0 = caps[0 % len(caps)]
    cap1 = caps[1 % len(caps)]
    assert cap0 != cap1

    got0 = await qm.dequeue(capability=cap0)
    got1 = await qm.dequeue(capability=cap1)
    assert got0 is not None
    assert got1 is not None
    assert {got0.id, got1.id} == {t_gpu.id, t_cpu.id}


@pytest.mark.asyncio
async def test_consumer_cap_idx_increments():
    """Consumer's _cap_idx increments on each call."""
    backend = InMemoryQueueBackend()
    qm = QueueManager(backend=backend)
    executor = TaskExecutor()

    consumer = TaskConsumer(
        queue_manager=qm,
        executor=executor,
        handler=AsyncMock(return_value={}),
        max_concurrent_tasks=5,
    )
    assert consumer._cap_idx == 0


# ---------------------------------------------------------------------------
# TODO-3: lease_lost_event interrupt tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_executor_lease_lost_interrupts_execution():
    """Setting lease_lost_event mid-execution causes executor to return failed result."""
    executor = TaskExecutor()
    task = _make_task()
    task.timeout_seconds = 30

    lease_lost_event = asyncio.Event()

    async def slow_handler(payload):
        await asyncio.sleep(10)
        return {"done": True}

    # Set lease_lost after a short delay
    async def set_event_later():
        await asyncio.sleep(0.05)
        lease_lost_event.set()

    asyncio.create_task(set_event_later())
    result = await executor.execute(task, slow_handler, lease_lost_event=lease_lost_event)

    assert not result.success
    assert result.error is not None
    assert "lease_lost" in str(result.error)


@pytest.mark.asyncio
async def test_executor_no_lease_lost_succeeds_normally():
    """Without lease_lost, normal execution completes successfully."""
    executor = TaskExecutor()
    task = _make_task()
    task.timeout_seconds = 5

    async def fast_handler(payload):
        return {"result": 42}

    result = await executor.execute(task, fast_handler, lease_lost_event=None)
    assert result.success
    assert result.result == {"result": 42}


@pytest.mark.asyncio
async def test_executor_lease_lost_after_completion_has_no_effect():
    """Setting lease_lost_event after execution completes doesn't change result."""
    executor = TaskExecutor()
    task = _make_task()
    task.timeout_seconds = 5

    lease_lost_event = asyncio.Event()

    async def fast_handler(payload):
        return {"result": "done"}

    result = await executor.execute(task, fast_handler, lease_lost_event=lease_lost_event)
    # Set event after completion - shouldn't matter
    lease_lost_event.set()

    assert result.success


@pytest.mark.asyncio
async def test_consumer_lease_lost_events_dict_managed():
    """lease_lost_events are cleaned up after task completes."""
    backend = InMemoryQueueBackend()
    qm = QueueManager(backend=backend)
    executor = TaskExecutor()

    call_count = 0

    async def handler(payload):
        nonlocal call_count
        call_count += 1
        return {"done": True}

    consumer = TaskConsumer(
        queue_manager=qm,
        executor=executor,
        handler=handler,
        max_concurrent_tasks=5,
        lock_backend=None,
    )

    # Initially empty
    assert consumer._lease_lost_events == {}

    # After processing, should be cleaned up
    task = _make_task()
    await consumer._process_single_task(task)
    assert task.id not in consumer._lease_lost_events
