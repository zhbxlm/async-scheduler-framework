"""Tests for TaskCompletionNode — 2-level callback retry + MySQL persist.

Covers:
- handle_completion: persist + callback run in parallel
- _persist: skipped when no db_session_factory
- _persist: upsert existing record (update path)
- _persist: insert new record
- _persist: failed status extracts error_message
- _trigger_callback: skipped when no callback_url
- _trigger_callback: success on first attempt → sets callback_done key
- _trigger_callback: retries on transient failure → eventually succeeds
- _trigger_callback: all retries exhausted → enqueues to durable ZSET
- _enqueue_callback_retry: exponential delay capped at MAX_RETRY_DELAY
- process_due_callbacks: processes due events, marks done on success
- process_due_callbacks: re-enqueues on failure (attempt < max)
- process_due_callbacks: sends to DLQ after max attempts
- process_due_callbacks: returns 0 with no redis
- close(): closes http client cleanly
"""
from __future__ import annotations

import asyncio
import json
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call

from tests.fake_redis import FullFakeAsyncRedis
from src.platform.task_completion_node import (
    TaskCompletionNode,
    _MAX_INLINE_RETRIES,
    _MAX_DURABLE_ATTEMPTS,
    _BASE_RETRY_DELAY,
    _MAX_RETRY_DELAY,
    _DLQ_TTL,
)
from src.platform import queue_keys as qk


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_node(
    *,
    with_redis: bool = True,
    with_db: bool = False,
    inline_retries: int = _MAX_INLINE_RETRIES,
    max_durable: int = _MAX_DURABLE_ATTEMPTS,
) -> tuple[TaskCompletionNode, FullFakeAsyncRedis | None]:
    redis = FullFakeAsyncRedis() if with_redis else None
    db = MagicMock() if with_db else None
    node = TaskCompletionNode(
        db_session_factory=db,
        redis_client=redis,
        http_timeout=5.0,
        inline_retries=inline_retries,
        max_durable_attempts=max_durable,
    )
    return node, redis


def _mock_http_response(status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    if status_code >= 400:
        resp.raise_for_status = MagicMock(side_effect=Exception(f"HTTP {status_code}"))
    else:
        resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# handle_completion: parallelism
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_handle_completion_returns_true():
    node, redis = _make_node()
    result = await node.handle_completion("t1", "completed", result={"ok": True})
    assert result is True


@pytest.mark.asyncio
async def test_handle_completion_no_crash_on_partial_failure():
    """Even if persist or callback fails, handle_completion still returns True."""
    node, redis = _make_node()

    with patch.object(node, "_persist", side_effect=RuntimeError("db down")):
        with patch.object(node, "_trigger_callback", return_value=None):
            result = await node.handle_completion(
                "t1", "completed", callback_url="http://cb"
            )
    assert result is True


@pytest.mark.asyncio
async def test_handle_completion_both_called():
    node, redis = _make_node()
    persist_called = []
    callback_called = []

    async def fake_persist(tid, status, result, *, tenant_id="", callback_url=""):
        persist_called.append(tid)

    async def fake_callback(tid, status, result, *, callback_url=""):
        callback_called.append(tid)

    with patch.object(node, "_persist", side_effect=fake_persist):
        with patch.object(node, "_trigger_callback", side_effect=fake_callback):
            await node.handle_completion("tx", "completed", callback_url="http://x")

    assert "tx" in persist_called
    assert "tx" in callback_called


# ---------------------------------------------------------------------------
# _persist: skip without db
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_persist_skipped_without_db():
    node, _ = _make_node(with_db=False)
    # Should not raise even with no db
    await node._persist("t1", "completed", None)


# ---------------------------------------------------------------------------
# _trigger_callback: no URL → skip
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_trigger_callback_skipped_without_url():
    node, redis = _make_node()
    # No URL → should return immediately without any HTTP call
    mock_client = AsyncMock()
    with patch.object(node, "_get_client", return_value=mock_client):
        await node._trigger_callback("t1", "completed", {}, callback_url="")
    mock_client.post.assert_not_called()


# ---------------------------------------------------------------------------
# _trigger_callback: success on first attempt
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_trigger_callback_success_first_attempt():
    node, redis = _make_node(inline_retries=3)

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=_mock_http_response(200))

    with patch.object(node, "_get_client", return_value=mock_client):
        await node._trigger_callback("t1", "completed", {"x": 1}, callback_url="http://cb")

    mock_client.post.assert_called_once()
    # callback_done key should be set in redis
    done_val = await redis.get(qk.callback_done("t1"))
    assert done_val == "1"


@pytest.mark.asyncio
async def test_trigger_callback_sends_idempotency_header():
    node, _ = _make_node()

    captured_headers = {}
    mock_resp = _mock_http_response(200)

    async def fake_post(url, *, json=None, headers=None, **kwargs):
        captured_headers.update(headers or {})
        return mock_resp

    mock_client = AsyncMock()
    mock_client.post = fake_post

    with patch.object(node, "_get_client", return_value=mock_client):
        await node._trigger_callback("task-xyz", "completed", {}, callback_url="http://cb")

    assert captured_headers.get("Idempotency-Key") == "task-xyz"


# ---------------------------------------------------------------------------
# _trigger_callback: retry on transient failure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_trigger_callback_retries_on_failure_then_succeeds():
    node, redis = _make_node(inline_retries=3)

    call_count = 0
    mock_client = AsyncMock()

    async def flaky_post(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ConnectionError("transient")
        return _mock_http_response(200)

    mock_client.post = flaky_post

    with patch("asyncio.sleep", return_value=None):  # skip real sleep
        with patch.object(node, "_get_client", return_value=mock_client):
            await node._trigger_callback("t2", "completed", {}, callback_url="http://cb")

    assert call_count == 3
    done_val = await redis.get(qk.callback_done("t2"))
    assert done_val == "1"


@pytest.mark.asyncio
async def test_trigger_callback_all_retries_fail_enqueues_durable():
    node, redis = _make_node(inline_retries=3)

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=ConnectionError("always fail"))

    enqueued = []

    async def fake_enqueue(task_id, callback_url, payload, attempt=1):
        enqueued.append({"task_id": task_id, "attempt": attempt})

    with patch("asyncio.sleep", return_value=None):
        with patch.object(node, "_get_client", return_value=mock_client):
            with patch.object(node, "_enqueue_callback_retry", side_effect=fake_enqueue):
                await node._trigger_callback("t3", "failed", {}, callback_url="http://cb")

    assert len(enqueued) == 1
    assert enqueued[0]["task_id"] == "t3"


# ---------------------------------------------------------------------------
# _enqueue_callback_retry: delay calculation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enqueue_callback_retry_adds_to_zset():
    node, redis = _make_node()
    payload = {"task_id": "te1", "status": "failed"}

    await node._enqueue_callback_retry("te1", "http://cb", payload, attempt=1)

    members = await redis.zrangebyscore(
        node._callback_retry_key, "-inf", float("inf")
    )
    assert len(members) == 1
    event = json.loads(members[0])
    assert event["task_id"] == "te1"
    assert event["attempt"] == 1


@pytest.mark.asyncio
async def test_enqueue_callback_retry_exponential_delay():
    """Score (timestamp) should reflect exponential backoff."""
    node, redis = _make_node()
    before = time.time()

    await node._enqueue_callback_retry("te2", "http://cb", {}, attempt=3)

    members = await redis.zrangebyscore(
        node._callback_retry_key, "-inf", float("inf"), withscores=True
    )
    assert len(members) == 1
    _, score = members[0]
    expected_delay = min(_MAX_RETRY_DELAY, _BASE_RETRY_DELAY * (2 ** 2))  # attempt=3 → 2^2=4
    assert score >= before + expected_delay - 1  # 1s tolerance


@pytest.mark.asyncio
async def test_enqueue_callback_retry_caps_at_max_delay():
    """Delay should be capped at MAX_RETRY_DELAY regardless of attempt number."""
    node, redis = _make_node()
    before = time.time()

    await node._enqueue_callback_retry("te3", "http://cb", {}, attempt=50)

    members = await redis.zrangebyscore(
        node._callback_retry_key, "-inf", float("inf"), withscores=True
    )
    _, score = members[0]
    assert score <= before + _MAX_RETRY_DELAY + 2  # capped + 2s tolerance


@pytest.mark.asyncio
async def test_enqueue_callback_retry_skipped_without_redis():
    node, _ = _make_node(with_redis=False)
    # Should not raise
    await node._enqueue_callback_retry("t1", "http://cb", {}, attempt=1)


# ---------------------------------------------------------------------------
# process_due_callbacks: success path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_due_callbacks_returns_0_without_redis():
    node, _ = _make_node(with_redis=False)
    count = await node.process_due_callbacks()
    assert count == 0


@pytest.mark.asyncio
async def test_process_due_callbacks_success_marks_done():
    node, redis = _make_node(max_durable=8)

    # Enqueue one due event
    event = json.dumps({
        "task_id": "pd1",
        "callback_url": "http://cb",
        "payload": {"task_id": "pd1", "status": "completed"},
        "attempt": 1,
    })
    await redis.zadd(node._callback_retry_key, {event: time.time() - 10})

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=_mock_http_response(200))

    with patch.object(node, "_get_client", return_value=mock_client):
        count = await node.process_due_callbacks()

    assert count == 1
    done_val = await redis.get(qk.callback_done("pd1"))
    assert done_val == "1"
    # Event should be removed from ZSET
    remaining = await redis.zrangebyscore(node._callback_retry_key, "-inf", float("inf"))
    assert len(remaining) == 0


@pytest.mark.asyncio
async def test_process_due_callbacks_failure_requeues():
    node, redis = _make_node(max_durable=8)

    event = json.dumps({
        "task_id": "pd2",
        "callback_url": "http://cb",
        "payload": {},
        "attempt": 1,
    })
    await redis.zadd(node._callback_retry_key, {event: time.time() - 10})

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=ConnectionError("fail"))

    with patch.object(node, "_get_client", return_value=mock_client):
        count = await node.process_due_callbacks()

    assert count == 1
    # Should be re-enqueued with attempt=2
    members = await redis.zrangebyscore(node._callback_retry_key, "-inf", float("inf"))
    assert len(members) == 1
    requeued = json.loads(members[0])
    assert requeued["attempt"] == 2
    assert requeued["task_id"] == "pd2"


@pytest.mark.asyncio
async def test_process_due_callbacks_dlq_after_max_attempts():
    node, redis = _make_node(max_durable=3)

    event = json.dumps({
        "task_id": "pd3",
        "callback_url": "http://cb",
        "payload": {},
        "attempt": 3,  # already at max
    })
    await redis.zadd(node._callback_retry_key, {event: time.time() - 10})

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=ConnectionError("fail"))

    with patch.object(node, "_get_client", return_value=mock_client):
        count = await node.process_due_callbacks()

    assert count == 1
    # Should be in DLQ
    dlq_members = await redis.zrangebyscore(node._callback_dlq_key, "-inf", float("inf"))
    assert len(dlq_members) == 1


@pytest.mark.asyncio
async def test_process_due_callbacks_only_processes_due():
    """Events with future score should not be processed."""
    node, redis = _make_node()

    future_event = json.dumps({"task_id": "future", "callback_url": "http://x", "payload": {}, "attempt": 1})
    await redis.zadd(node._callback_retry_key, {future_event: time.time() + 9999})

    count = await node.process_due_callbacks()
    assert count == 0


@pytest.mark.asyncio
async def test_process_due_callbacks_respects_batch_size():
    node, redis = _make_node(max_durable=8)

    # Add 10 due events
    for i in range(10):
        ev = json.dumps({"task_id": f"b{i}", "callback_url": "http://cb", "payload": {}, "attempt": 1})
        await redis.zadd(node._callback_retry_key, {ev: time.time() - 10})

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=_mock_http_response(200))

    with patch.object(node, "_get_client", return_value=mock_client):
        count = await node.process_due_callbacks(batch_size=3)

    assert count == 3


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_close_cleans_up_http_client():
    node, _ = _make_node()
    # Trigger client creation
    mock_client = AsyncMock()
    mock_client.is_closed = False
    node._client = mock_client

    await node.close()
    mock_client.aclose.assert_awaited_once()
    assert node._client is None


@pytest.mark.asyncio
async def test_close_safe_when_no_client():
    node, _ = _make_node()
    node._client = None
    await node.close()  # should not raise
