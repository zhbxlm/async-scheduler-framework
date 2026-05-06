"""Tests for QueueManager — enqueue, dequeue, complete, fail, cancel, stats.

Covers:
- enqueue(): accepted, returns position/count
- enqueue(): rejected when queue full
- enqueue(): circuit-breaker open → raises RuntimeError
- enqueue(): circuit-breaker closed → proceeds
- dequeue_ready(): returns task_id when available and within time window
- dequeue_ready(): returns None on empty queue
- dequeue_ready(): circuit-breaker open → returns None
- dequeue_ready(): concurrency limit respected
- complete(): removes from running, calls cb.record_success
- fail(): removes from running, calls cb.record_failure
- cancel(): removes from pending or running
- dequeue() alias: wraps dequeue_ready, returns dict
- invalidate_stats_cache(): forces refresh on next call
- discover_queue_capabilities(): reads from Redis set
"""
from __future__ import annotations

import time
import pytest
from unittest.mock import AsyncMock, MagicMock

from tests.fake_redis import FullFakeAsyncRedis
from src.platform.queue_manager import QueueManager, _CAPABILITIES_KEY
from src.platform import queue_keys as qk


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_qm(
    *,
    with_cb: bool = False,
    cb_allows: bool = True,
    max_depth: int = 100,
    max_concurrent: int = 4,
) -> tuple[QueueManager, FullFakeAsyncRedis, MagicMock | None]:
    redis = FullFakeAsyncRedis()
    cb = None
    if with_cb:
        cb = MagicMock()
        cb.allow_request = AsyncMock(return_value=cb_allows)
        cb.record_success = AsyncMock()
        cb.record_failure = AsyncMock()
    qm = QueueManager(
        redis_client=redis,
        circuit_breaker=cb,
        default_max_queue_depth=max_depth,
        default_max_concurrent=max_concurrent,
        ttl=3600,
    )
    return qm, redis, cb


# ---------------------------------------------------------------------------
# enqueue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enqueue_accepted():
    qm, _, _ = _make_qm()
    result = await qm.enqueue("cap_a", "task-1")
    assert result["accepted"] is True
    assert result["queue_position"] >= 0


@pytest.mark.asyncio
async def test_enqueue_returns_incrementing_position():
    qm, _, _ = _make_qm()
    r1 = await qm.enqueue("cap_a", "task-1")
    r2 = await qm.enqueue("cap_a", "task-2")
    assert r2["queue_position"] > r1["queue_position"]


@pytest.mark.asyncio
async def test_enqueue_rejected_when_full():
    qm, _, _ = _make_qm(max_depth=2)
    await qm.enqueue("cap_a", "t1")
    await qm.enqueue("cap_a", "t2")
    r3 = await qm.enqueue("cap_a", "t3")
    assert r3["accepted"] is False


@pytest.mark.asyncio
async def test_enqueue_blocked_by_open_circuit_breaker():
    qm, _, _ = _make_qm(with_cb=True, cb_allows=False)
    with pytest.raises(RuntimeError, match="Circuit breaker open"):
        await qm.enqueue("cap_a", "t1")


@pytest.mark.asyncio
async def test_enqueue_proceeds_with_closed_cb():
    qm, _, cb = _make_qm(with_cb=True, cb_allows=True)
    result = await qm.enqueue("cap_a", "t1")
    assert result["accepted"] is True


@pytest.mark.asyncio
async def test_enqueue_registers_capability():
    qm, redis, _ = _make_qm()
    await qm.enqueue("cap_gpu", "t1")
    caps = await redis.smembers(_CAPABILITIES_KEY)
    cap_strs = {c.decode() if isinstance(c, bytes) else c for c in caps}
    assert "cap_gpu" in cap_strs


@pytest.mark.asyncio
async def test_enqueue_pending_count_grows():
    qm, redis, _ = _make_qm()
    await qm.enqueue("cap_a", "t1")
    await qm.enqueue("cap_a", "t2")
    cnt = await redis.zcard(qk.pending("cap_a"))
    assert cnt == 2


# ---------------------------------------------------------------------------
# dequeue_ready
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dequeue_returns_task_id():
    qm, _, _ = _make_qm()
    await qm.enqueue("cap_a", "task-x")
    task_id = await qm.dequeue_ready("cap_a")
    assert task_id == "task-x"


@pytest.mark.asyncio
async def test_dequeue_returns_none_on_empty():
    qm, _, _ = _make_qm()
    result = await qm.dequeue_ready("cap_a")
    assert result is None


@pytest.mark.asyncio
async def test_dequeue_blocked_by_open_circuit_breaker():
    qm, _, _ = _make_qm(with_cb=True, cb_allows=False)
    result = await qm.dequeue_ready("cap_a")
    assert result is None


@pytest.mark.asyncio
async def test_dequeue_removes_from_pending():
    qm, redis, _ = _make_qm()
    await qm.enqueue("cap_a", "t1")
    await qm.dequeue_ready("cap_a")
    pending_cnt = await redis.zcard(qk.pending("cap_a"))
    assert pending_cnt == 0


@pytest.mark.asyncio
async def test_dequeue_moves_to_running():
    qm, redis, _ = _make_qm()
    await qm.enqueue("cap_a", "t1")
    await qm.dequeue_ready("cap_a")
    running_cnt = await redis.zcard(qk.running("cap_a"))
    assert running_cnt == 1


@pytest.mark.asyncio
async def test_dequeue_respects_concurrency_limit():
    qm, redis, _ = _make_qm(max_concurrent=2)
    for i in range(5):
        await qm.enqueue("cap_a", f"t{i}")

    # Dequeue up to limit
    results = []
    for _ in range(5):
        r = await qm.dequeue_ready("cap_a")
        if r:
            results.append(r)

    assert len(results) <= 2


@pytest.mark.asyncio
async def test_dequeue_future_task_not_returned():
    """Tasks with execute_after_ms in the future should not be dequeued."""
    qm, redis, _ = _make_qm()
    future_ms = int(time.time() * 1000) + 999_999
    await qm.enqueue("cap_a", "future-task", execute_after_ms=future_ms)
    result = await qm.dequeue_ready("cap_a")
    assert result is None


# ---------------------------------------------------------------------------
# complete / fail
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_complete_removes_from_running():
    qm, redis, _ = _make_qm()
    await qm.enqueue("cap_a", "t1")
    await qm.dequeue_ready("cap_a")
    await qm.complete("cap_a", "t1")
    assert await redis.zcard(qk.running("cap_a")) == 0


@pytest.mark.asyncio
async def test_complete_calls_cb_record_success():
    qm, _, cb = _make_qm(with_cb=True, cb_allows=True)
    await qm.enqueue("cap_a", "t1")
    await qm.dequeue_ready("cap_a")
    await qm.complete("cap_a", "t1")
    cb.record_success.assert_awaited_once_with("cap_a")


@pytest.mark.asyncio
async def test_fail_removes_from_running():
    qm, redis, _ = _make_qm()
    await qm.enqueue("cap_a", "t1")
    await qm.dequeue_ready("cap_a")
    await qm.fail("cap_a", "t1")
    assert await redis.zcard(qk.running("cap_a")) == 0


@pytest.mark.asyncio
async def test_fail_calls_cb_record_failure():
    qm, _, cb = _make_qm(with_cb=True, cb_allows=True)
    await qm.enqueue("cap_a", "t1")
    await qm.dequeue_ready("cap_a")
    await qm.fail("cap_a", "t1")
    cb.record_failure.assert_awaited_once_with("cap_a")


@pytest.mark.asyncio
async def test_complete_increments_total_completed():
    qm, redis, _ = _make_qm()
    await qm.enqueue("cap_a", "t1")
    await qm.dequeue_ready("cap_a")
    await qm.complete("cap_a", "t1")
    stats = await redis.hgetall(qk.stats("cap_a"))
    count = int(stats.get("total_completed", stats.get(b"total_completed", 0)))
    assert count == 1


@pytest.mark.asyncio
async def test_fail_increments_total_failed():
    qm, redis, _ = _make_qm()
    await qm.enqueue("cap_a", "t1")
    await qm.dequeue_ready("cap_a")
    await qm.fail("cap_a", "t1")
    stats = await redis.hgetall(qk.stats("cap_a"))
    count = int(stats.get("total_failed", stats.get(b"total_failed", 0)))
    assert count == 1


# ---------------------------------------------------------------------------
# cancel
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_removes_pending_task():
    qm, redis, _ = _make_qm()
    await qm.enqueue("cap_a", "t-cancel")
    result = await qm.cancel("cap_a", "t-cancel")
    assert result is True
    assert await redis.zcard(qk.pending("cap_a")) == 0


@pytest.mark.asyncio
async def test_cancel_removes_running_task():
    qm, redis, _ = _make_qm()
    await qm.enqueue("cap_a", "t-running")
    await qm.dequeue_ready("cap_a")
    result = await qm.cancel("cap_a", "t-running")
    assert result is True
    assert await redis.zcard(qk.running("cap_a")) == 0


@pytest.mark.asyncio
async def test_cancel_returns_false_for_unknown_task():
    qm, _, _ = _make_qm()
    result = await qm.cancel("cap_a", "nonexistent")
    assert result is False


# ---------------------------------------------------------------------------
# dequeue alias
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dequeue_alias_returns_dict():
    qm, _, _ = _make_qm()
    await qm.enqueue("cap_a", "t-alias")
    result = await qm.dequeue("cap_a")
    assert result is not None
    assert result["task_id"] == "t-alias"
    assert result["capability"] == "cap_a"


@pytest.mark.asyncio
async def test_dequeue_alias_returns_none_on_empty():
    qm, _, _ = _make_qm()
    result = await qm.dequeue("cap_a")
    assert result is None


# ---------------------------------------------------------------------------
# stats / cache
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_invalidate_stats_cache_forces_refresh():
    qm, _, _ = _make_qm()
    qm._stats_cache_time = time.time() + 9999  # simulate fresh cache
    await qm.invalidate_stats_cache()
    assert qm._stats_cache_time == 0.0


@pytest.mark.asyncio
async def test_discover_queue_capabilities_empty():
    qm, _, _ = _make_qm()
    caps = await qm.discover_queue_capabilities()
    assert isinstance(caps, list)


@pytest.mark.asyncio
async def test_discover_queue_capabilities_after_enqueue():
    qm, _, _ = _make_qm()
    await qm.enqueue("cap_special", "t1")
    caps = await qm.discover_queue_capabilities()
    assert "cap_special" in caps
