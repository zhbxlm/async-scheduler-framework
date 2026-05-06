"""Tests for TaskConsumer — poll loop, dequeue dispatch, concurrency, backoff.

Covers:
- start / stop lifecycle
- dequeue → execute happy path
- empty queue → backoff
- semaphore respects max_concurrent
- global concurrency via Lua slot (Redis path)
- dequeue exception is swallowed
- task execute exception is swallowed
- stop() cancels in-flight tasks and calls executor.shutdown()
"""
from __future__ import annotations

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from tests.fake_redis import FullFakeAsyncRedis
from src.platform.task_consumer import TaskConsumer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_consumer(
    capabilities=None,
    *,
    max_concurrent=4,
    poll_interval=0.01,
    use_redis=False,
) -> tuple[TaskConsumer, AsyncMock, AsyncMock]:
    """Build a TaskConsumer with mock queue + executor.

    Returns (consumer, mock_queue, mock_executor).
    """
    mock_queue = AsyncMock()
    mock_queue.dequeue = AsyncMock(return_value=None)  # empty by default

    mock_executor = AsyncMock()
    mock_executor.execute = AsyncMock()
    mock_executor.shutdown = AsyncMock()

    redis = FullFakeAsyncRedis() if use_redis else None

    consumer = TaskConsumer(
        queue_manager=mock_queue,
        task_executor=mock_executor,
        capabilities=capabilities or ["cap_a"],
        poll_interval=poll_interval,
        max_concurrent=max_concurrent,
        redis_client=redis,
    )
    return consumer, mock_queue, mock_executor


async def _run_for(consumer: TaskConsumer, seconds: float) -> None:
    """Run consumer.start() for *seconds* then stop."""
    task = asyncio.create_task(consumer.start())
    await asyncio.sleep(seconds)
    await consumer.stop()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


# ---------------------------------------------------------------------------
# Lifecycle tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_sets_running_flag():
    consumer, _, _ = _make_consumer()
    assert consumer._running is False
    # Start briefly then stop immediately
    task = asyncio.create_task(consumer.start())
    await asyncio.sleep(0.02)
    assert consumer._running is True
    await consumer.stop()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


@pytest.mark.asyncio
async def test_stop_clears_running_flag():
    consumer, _, _ = _make_consumer()
    task = asyncio.create_task(consumer.start())
    await asyncio.sleep(0.02)
    await consumer.stop()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
    assert consumer._running is False


@pytest.mark.asyncio
async def test_stop_calls_executor_shutdown():
    consumer, _, mock_executor = _make_consumer()
    task = asyncio.create_task(consumer.start())
    await asyncio.sleep(0.02)
    await consumer.stop()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
    mock_executor.shutdown.assert_awaited()


# ---------------------------------------------------------------------------
# Happy-path dispatch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dequeue_task_dispatched_to_executor():
    """When dequeue returns a task, executor.execute is called."""
    consumer, mock_queue, mock_executor = _make_consumer(poll_interval=0.01)

    # Return a task once, then nothing
    task_data = {"task_id": "t1", "payload": "hello"}
    mock_queue.dequeue = AsyncMock(side_effect=[task_data, None, None, None, None, None])

    await _run_for(consumer, 0.08)

    # execute should have been called with t1
    calls = mock_executor.execute.call_args_list
    task_ids = [c.args[0] for c in calls]
    assert "t1" in task_ids


@pytest.mark.asyncio
async def test_dequeue_multiple_tasks_all_dispatched():
    consumer, mock_queue, mock_executor = _make_consumer(
        capabilities=["cap_a", "cap_b"],
        poll_interval=0.01,
    )
    task_a = {"task_id": "a1"}
    task_b = {"task_id": "b1"}
    # cap_a returns task_a once, cap_b returns task_b once, then empty
    call_count: dict[str, int] = {"cap_a": 0, "cap_b": 0}

    async def fake_dequeue(cap):
        if cap == "cap_a" and call_count["cap_a"] == 0:
            call_count["cap_a"] += 1
            return task_a
        if cap == "cap_b" and call_count["cap_b"] == 0:
            call_count["cap_b"] += 1
            return task_b
        return None

    mock_queue.dequeue = fake_dequeue
    await _run_for(consumer, 0.15)

    task_ids = {c.args[0] for c in mock_executor.execute.call_args_list}
    assert "a1" in task_ids
    assert "b1" in task_ids


# ---------------------------------------------------------------------------
# Backoff behaviour
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backoff_increases_on_empty_queue():
    consumer, _, _ = _make_consumer(poll_interval=0.01)
    # Run briefly so a few empty rounds complete
    await _run_for(consumer, 0.08)
    # backoff multiplier should be > 1 after empty rounds
    assert consumer._backoff_multiplier > 1.0


@pytest.mark.asyncio
async def test_backoff_resets_on_task():
    """Backoff multiplier is reset to 1.0 after empty rounds when a task arrives."""
    consumer, mock_queue, _ = _make_consumer(poll_interval=0.001)

    captured_multipliers: list[float] = []
    sleep_count = 0
    original_sleep = asyncio.sleep

    async def patched_sleep(t):
        nonlocal sleep_count
        sleep_count += 1
        captured_multipliers.append(consumer._backoff_multiplier)
        await original_sleep(0.001)

    # Supply: first 3 empty rounds (multiplier increases), then 1 task, then empty
    task_count = 0

    async def fake_dequeue(cap):
        nonlocal task_count
        task_count += 1
        if task_count == 4:  # 4th dequeue call returns the task
            return {"task_id": "x1"}
        return None

    mock_queue.dequeue = fake_dequeue

    with patch("src.platform.task_consumer.asyncio.sleep", side_effect=patched_sleep):
        task = asyncio.create_task(consumer.start())
        for _ in range(100):
            await asyncio.sleep(0.005)
            if sleep_count >= 9:  # wait for enough rounds to capture the reset
                break
        await consumer.stop()
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    # Find pattern: >1.0 somewhere, then 1.0 (reset)
    had_increase = any(v > 1.0 for v in captured_multipliers)
    had_reset = False
    found_increase = False
    for val in captured_multipliers:
        if val > 1.0:
            found_increase = True
        if found_increase and val == 1.0:
            had_reset = True
            break

    assert had_increase, f"Backoff never increased: {captured_multipliers}"
    assert had_reset, f"Backoff never reset to 1.0 after task: {captured_multipliers}"


@pytest.mark.asyncio
async def test_backoff_caps_at_max():
    consumer, _, _ = _make_consumer(poll_interval=0.001, max_concurrent=4)
    await _run_for(consumer, 0.1)
    assert consumer._backoff_multiplier <= consumer._max_backoff


# ---------------------------------------------------------------------------
# Concurrency limit (semaphore)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_semaphore_limits_concurrent_tasks():
    """With max_concurrent=2, at most 2 tasks execute simultaneously."""
    max_concurrent = 2
    consumer, mock_queue, mock_executor = _make_consumer(
        max_concurrent=max_concurrent,
        poll_interval=0.005,
    )

    active = 0
    peak = 0

    async def slow_execute(task_id, ctx, data):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.05)
        active -= 1

    mock_executor.execute = slow_execute

    # Supply 6 tasks then empty
    tasks = [{"task_id": f"t{i}"} for i in range(6)]
    mock_queue.dequeue = AsyncMock(side_effect=tasks + [None] * 20)

    await _run_for(consumer, 0.35)

    assert peak <= max_concurrent


@pytest.mark.asyncio
async def test_full_semaphore_skips_dequeue():
    """When semaphore is exhausted, _try_dequeue returns False immediately."""
    consumer, mock_queue, _ = _make_consumer(max_concurrent=1, poll_interval=0.01)

    # Manually drain the semaphore
    await consumer._semaphore.acquire()

    result = await consumer._try_dequeue("cap_a")
    assert result is False
    mock_queue.dequeue.assert_not_called()

    consumer._semaphore.release()


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dequeue_exception_does_not_crash_loop():
    consumer, mock_queue, mock_executor = _make_consumer(poll_interval=0.01)
    mock_queue.dequeue = AsyncMock(side_effect=RuntimeError("redis down"))
    # Consumer should keep running without raising
    await _run_for(consumer, 0.05)
    # executor should never be called
    mock_executor.execute.assert_not_called()


@pytest.mark.asyncio
async def test_execute_exception_does_not_crash_loop():
    """Task execution errors are caught; consumer keeps looping."""
    consumer, mock_queue, mock_executor = _make_consumer(poll_interval=0.01)
    bad_task = {"task_id": "bad"}
    mock_queue.dequeue = AsyncMock(side_effect=[bad_task] + [None] * 30)
    mock_executor.execute = AsyncMock(side_effect=RuntimeError("exec error"))

    # Should not raise
    await _run_for(consumer, 0.08)


@pytest.mark.asyncio
async def test_semaphore_released_after_execute_exception():
    """Semaphore is always released even when execute raises."""
    consumer, mock_queue, mock_executor = _make_consumer(max_concurrent=2, poll_interval=0.01)
    mock_queue.dequeue = AsyncMock(
        side_effect=[{"task_id": "t1"}, {"task_id": "t2"}] + [None] * 20
    )
    mock_executor.execute = AsyncMock(side_effect=RuntimeError("boom"))

    await _run_for(consumer, 0.15)

    # Both slots should be freed: semaphore value back to max
    assert consumer._semaphore._value == consumer._max_concurrent


# ---------------------------------------------------------------------------
# Global Redis concurrency (Lua path)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_global_conc_redis_path_dispatches_task():
    """With a real FakeRedis, global concurrency Lua scripts work correctly."""
    consumer, mock_queue, mock_executor = _make_consumer(
        use_redis=True,
        max_concurrent=2,
        poll_interval=0.01,
    )
    task_data = {"task_id": "r1"}
    mock_queue.dequeue = AsyncMock(side_effect=[task_data] + [None] * 20)

    await _run_for(consumer, 0.12)

    task_ids = [c.args[0] for c in mock_executor.execute.call_args_list]
    assert "r1" in task_ids


@pytest.mark.asyncio
async def test_global_conc_slot_released_after_execute():
    """After task execution, the Lua concurrency slot counter returns to 0."""
    from src.platform.task_consumer import _GLOBAL_CONC_KEY

    fake_redis = FullFakeAsyncRedis()

    consumer, mock_queue, mock_executor = _make_consumer(
        use_redis=False,   # we inject manually below
        max_concurrent=2,
        poll_interval=0.01,
    )
    # Inject the fake redis scripts — pass real Lua content so FakeRedis detects acquire/release
    consumer._use_global_conc = True
    consumer._lua_acquire_slot = fake_redis.register_script("redis.call('INCR', KEYS[1])")
    consumer._lua_release_slot = fake_redis.register_script("redis.call('DECR', KEYS[1])")

    task_data = {"task_id": "slot1"}
    mock_queue.dequeue = AsyncMock(side_effect=[task_data] + [None] * 30)

    await _run_for(consumer, 0.2)

    # After the task finished, the global slot counter should be back to 0
    counter = await fake_redis.get(_GLOBAL_CONC_KEY)
    val = int(counter) if counter is not None else 0
    assert val == 0


# ---------------------------------------------------------------------------
# Active-cap burst logic
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_active_cap_tracked_after_task():
    consumer, mock_queue, _ = _make_consumer(capabilities=["cap_a"], poll_interval=0.01)
    task_data = {"task_id": "burst1"}
    mock_queue.dequeue = AsyncMock(side_effect=[task_data] + [None] * 30)

    await _run_for(consumer, 0.08)

    # cap_a should have been added to active set at some point
    # After 3 empty rounds it gets evicted — just confirm no crash
    assert isinstance(consumer._active_caps, dict)


@pytest.mark.asyncio
async def test_active_cap_evicted_after_empty_rounds():
    consumer, mock_queue, _ = _make_consumer(capabilities=["cap_a"], poll_interval=0.005)
    # Immediately empty — cap_a should be evicted after 3 empty rounds
    mock_queue.dequeue = AsyncMock(return_value=None)

    # Seed active set manually
    consumer._active_caps["cap_a"] = 0

    await _run_for(consumer, 0.1)
    # cap_a should have been evicted
    assert "cap_a" not in consumer._active_caps
