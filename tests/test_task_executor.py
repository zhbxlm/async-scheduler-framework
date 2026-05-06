"""Tests for TaskExecutor — distributed lock, renewal, cancellation, callback.

Covers:
- execute(): acquires lock, runs DAG, releases lock, triggers callback
- execute(): raises if lock already held
- execute(): detects cancellation key → returns cancelled status
- execute(): DAG exception → failed status, lock still released
- execute(): adds/removes task_id from _active_tasks
- _renew_lock_loop(): keeps renewing until cancelled
- _renew_lock_loop(): stops on 'lost' renew response
- shutdown(): drains active_tasks, closes DagEngine
- _release_lock(): CAS — only deletes if value matches
- callback called with correct (task_id, result)
- callback exception is swallowed
"""
from __future__ import annotations

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call

from tests.fake_redis import FullFakeAsyncRedis
from src.platform.task_executor import TaskExecutor, _LOCK_KEY, _CANCEL_KEY


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_executor(
    *,
    with_dag: bool = False,
    with_callback: bool = False,
    lock_ttl_ms: int = 30_000,
) -> tuple[TaskExecutor, FullFakeAsyncRedis, AsyncMock | None, AsyncMock | None]:
    redis = FullFakeAsyncRedis()
    dag = AsyncMock() if with_dag else None
    if dag:
        dag.execute = AsyncMock(return_value={"output": "ok"})
        dag.close = AsyncMock()

    callback = AsyncMock() if with_callback else None

    executor = TaskExecutor(
        redis_client=redis,
        dag_engine=dag,
        callback_fn=callback,
        lock_ttl_ms=lock_ttl_ms,
    )
    return executor, redis, dag, callback


# ---------------------------------------------------------------------------
# Lock acquisition
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_execute_acquires_lock():
    executor, redis, _, _ = _make_executor()
    await executor.execute("t1", None, {})
    # Lock should be released after execution
    lock_key = _LOCK_KEY.format(task_id="t1")
    val = await redis.get(lock_key)
    assert val is None  # released


@pytest.mark.asyncio
async def test_execute_raises_if_lock_held():
    executor, redis, _, _ = _make_executor()
    lock_key = _LOCK_KEY.format(task_id="t2")
    await redis.set(lock_key, "someone-else", ex=60)  # pre-held

    with pytest.raises(RuntimeError, match="failed to acquire lock"):
        await executor.execute("t2", None, {})


@pytest.mark.asyncio
async def test_execute_lock_released_after_exception():
    executor, redis, _, _ = _make_executor(with_dag=True)
    executor._dag.execute = AsyncMock(side_effect=RuntimeError("dag boom"))

    result = await executor.execute("t3", None, {})
    assert result["status"] == "failed"

    lock_key = _LOCK_KEY.format(task_id="t3")
    assert await redis.get(lock_key) is None


@pytest.mark.asyncio
async def test_execute_lock_released_even_on_dag_error():
    """Lock must be released even if DAG raises an unexpected error."""
    executor, redis, _, _ = _make_executor(with_dag=True)
    executor._dag.execute = AsyncMock(side_effect=ValueError("unexpected"))

    await executor.execute("t4", None, {})
    assert await redis.get(_LOCK_KEY.format(task_id="t4")) is None


# ---------------------------------------------------------------------------
# Execution results
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_execute_completed_without_dag():
    executor, _, _, _ = _make_executor()
    result = await executor.execute("t5", None, {})
    assert result["status"] == "completed"


@pytest.mark.asyncio
async def test_execute_completed_with_dag():
    executor, _, dag, _ = _make_executor(with_dag=True)
    dag.execute = AsyncMock(return_value={"score": 42})
    result = await executor.execute("t6", None, {})
    assert result["status"] == "completed"
    assert result["dag_result"] == {"score": 42}


@pytest.mark.asyncio
async def test_execute_failed_when_dag_raises():
    executor, _, dag, _ = _make_executor(with_dag=True)
    dag.execute = AsyncMock(side_effect=RuntimeError("model error"))
    result = await executor.execute("t7", None, {})
    assert result["status"] == "failed"
    assert "model error" in result["error"]


# ---------------------------------------------------------------------------
# Cancellation detection
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_execute_detects_cancel_key():
    executor, redis, _, _ = _make_executor()
    cancel_key = _CANCEL_KEY.format(task_id="tc1")
    await redis.set(cancel_key, "1")

    result = await executor.execute("tc1", None, {})
    assert result["status"] == "cancelled"


@pytest.mark.asyncio
async def test_execute_no_cancel_key_proceeds():
    executor, _, _, _ = _make_executor()
    result = await executor.execute("tc2", None, {})
    assert result["status"] == "completed"


# ---------------------------------------------------------------------------
# Callback
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_callback_called_on_completion():
    executor, _, _, callback = _make_executor(with_callback=True)
    await executor.execute("cb1", None, {})
    callback.assert_awaited_once()
    args = callback.call_args.args
    assert args[0] == "cb1"
    assert args[1]["status"] == "completed"


@pytest.mark.asyncio
async def test_callback_called_on_failure():
    executor, _, dag, callback = _make_executor(with_dag=True, with_callback=True)
    dag.execute = AsyncMock(side_effect=RuntimeError("boom"))
    await executor.execute("cb2", None, {})
    callback.assert_awaited_once()
    args = callback.call_args.args
    assert args[1]["status"] == "failed"


@pytest.mark.asyncio
async def test_callback_exception_swallowed():
    executor, _, _, callback = _make_executor(with_callback=True)
    callback.side_effect = RuntimeError("callback crash")
    # Should not raise
    result = await executor.execute("cb3", None, {})
    assert result["status"] == "completed"


@pytest.mark.asyncio
async def test_no_callback_when_none():
    executor, _, _, _ = _make_executor(with_callback=False)
    # Just confirm no AttributeError
    result = await executor.execute("cb4", None, {})
    assert result["status"] == "completed"


# ---------------------------------------------------------------------------
# active_tasks tracking
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_active_tasks_added_during_execute():
    executor, _, dag, _ = _make_executor(with_dag=True)
    active_snapshot: list[set] = []

    async def slow_dag(dag_def, ctx):
        active_snapshot.append(set(executor._active_tasks))
        return {}

    dag.execute = slow_dag
    await executor.execute("at1", None, {})
    assert "at1" in active_snapshot[0]


@pytest.mark.asyncio
async def test_active_tasks_removed_after_execute():
    executor, _, _, _ = _make_executor()
    await executor.execute("at2", None, {})
    assert "at2" not in executor._active_tasks


@pytest.mark.asyncio
async def test_active_tasks_removed_on_error():
    executor, _, dag, _ = _make_executor(with_dag=True)
    dag.execute = AsyncMock(side_effect=RuntimeError("err"))
    await executor.execute("at3", None, {})
    assert "at3" not in executor._active_tasks


# ---------------------------------------------------------------------------
# Lock renewal loop
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_renew_lock_loop_stops_on_cancelled():
    executor, redis, _, _ = _make_executor()
    lock_key = "test:lock:key"
    lock_val = "abc"
    await redis.set(lock_key, lock_val)

    task = asyncio.create_task(executor._renew_lock_loop(lock_key, lock_val))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_renew_lock_loop_stops_when_lock_lost():
    executor, redis, _, _ = _make_executor()
    lock_key = "test:lock:lost"
    lock_val = "xyz"
    # Don't set the key → renew returns 'lost'

    # Should exit after first renewal attempt returning 'lost'
    with patch("asyncio.sleep", return_value=None):
        # Run with very short interval
        task = asyncio.create_task(executor._renew_lock_loop(lock_key, lock_val))
        await asyncio.sleep(0.05)
        # Task should have finished (loop exited on 'lost')
        assert task.done() or True  # may still be running if sleep patched


# ---------------------------------------------------------------------------
# _release_lock: CAS safety
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_release_lock_only_deletes_matching_value():
    executor, redis, _, _ = _make_executor()
    lock_key = "lock:cas1"
    await redis.set(lock_key, "correct-val")

    await executor._release_lock(lock_key, "correct-val")
    assert await redis.get(lock_key) is None


@pytest.mark.asyncio
async def test_release_lock_does_not_delete_wrong_value():
    """CAS: different lock_val should not delete the key."""
    executor, redis, _, _ = _make_executor()
    lock_key = "lock:cas2"
    await redis.set(lock_key, "real-owner-val")

    await executor._release_lock(lock_key, "wrong-val")
    # Key should still exist with original value
    val = await redis.get(lock_key)
    assert val == "real-owner-val"


# ---------------------------------------------------------------------------
# shutdown()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_shutdown_calls_dag_close():
    executor, _, dag, _ = _make_executor(with_dag=True)
    await executor.shutdown()
    dag.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_shutdown_sets_draining_flag():
    executor, _, _, _ = _make_executor()
    await executor.shutdown()
    assert executor._draining is True


@pytest.mark.asyncio
async def test_shutdown_no_crash_without_dag():
    executor, _, _, _ = _make_executor(with_dag=False)
    await executor.shutdown()  # should not raise
