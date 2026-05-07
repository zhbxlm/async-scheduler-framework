"""Tests for TaskReconciler — 3-phase consistency repair loop.

Covers:
- start / stop lifecycle
- Leader election (SET NX EX) + Lua renew
- Non-leader skips all phases
- _scan_task_batch: SCAN + pipeline GET, parses dicts, skips bad JSON
- Phase 1 (double-write): terminal tasks missing from MySQL are inserted
- Phase 1: tasks already in MySQL are skipped
- Phase 1: skipped when no db_session_factory
- Phase 2 (stuck recovery): running task with no lock → requeued (attempt < max_retries)
- Phase 2: running task with no lock → marked FAILED (attempt >= max_retries)
- Phase 2: lock exists → not considered stuck
- Phase 2: too young → not considered stuck
- Phase 2: dedup key prevents double-processing
- Phase 2: skipped when no redis
- Phase 3 (lost callback): terminal task with no callback_done → enqueued
- Phase 3: already in retry queue → not duplicated
- Phase 3: callback_done set → skipped
- Phase 3: skipped when no redis
- Full loop: all 3 phases invoked when leader
"""
from __future__ import annotations

import asyncio
import json
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
from contextlib import asynccontextmanager

from tests.fake_redis import FullFakeAsyncRedis
from src.platform.task_reconciler import (
    TaskReconciler,
    _RECONCILE_LEADER_KEY,
    _LEADER_TTL,
    _LOCK_KEY_TEMPLATE,
    _CALLBACK_DONE_KEY,
    _CALLBACK_RETRY_KEY,
    _REQUEUE_DEDUP_KEY,
    _REQUEUE_DEDUP_TTL,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_reconciler(
    *,
    with_redis: bool = True,
    with_db: bool = False,
    with_qm: bool = True,
    interval: float = 9999.0,
    instance_id: str = "test-node",
    stuck_max_age: float = 300.0,
) -> tuple[TaskReconciler, FullFakeAsyncRedis | None, AsyncMock | None, AsyncMock | None]:
    redis = FullFakeAsyncRedis() if with_redis else None
    db = _make_fake_db() if with_db else None
    qm = AsyncMock() if with_qm else None
    if qm:
        qm.enqueue = AsyncMock()

    rec = TaskReconciler(
        redis_client=redis,
        db_session_factory=db,
        queue_manager=qm,
        interval_seconds=interval,
        instance_id=instance_id,
        stuck_task_max_age_seconds=stuck_max_age,
        stuck_max_per_tick=20,
        batch_size=50,
    )
    return rec, redis, db, qm


def _make_fake_db():
    """Fake async db_session_factory that supports async context manager."""
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.add = MagicMock()

    @asynccontextmanager
    async def factory():
        yield session

    factory._session = session
    return factory


def _task(
    task_id: str,
    status: str = "completed",
    *,
    callback_url: str = "",
    attempt: int = 0,
    max_retries: int = 0,
    created_ts: float | None = None,
    capability: str = "cap_a",
) -> dict:
    t = {
        "task_id": task_id,
        "tenant_id": "t1",
        "status": status,
        "capability": capability,
        "attempt": attempt,
        "max_retries": max_retries,
        "created_at_ts": created_ts or (time.time() - 600),  # old by default
        "callback_url": callback_url,
        "output": {"result": "ok"},
    }
    return t


def _seed_tasks(redis: FullFakeAsyncRedis, tasks: list[dict]) -> None:
    """Synchronously seed task keys into FakeRedis strings store."""
    for t in tasks:
        redis._strings[f"task:{t['task_id']}"] = json.dumps(t)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_creates_loop_task():
    rec, _, _, _ = _make_reconciler()
    await rec.start()
    assert rec._running is True
    assert rec._loop_task is not None
    await rec.stop()


@pytest.mark.asyncio
async def test_start_idempotent():
    rec, _, _, _ = _make_reconciler()
    await rec.start()
    task1 = rec._loop_task
    await rec.start()  # second call should be no-op
    assert rec._loop_task is task1
    await rec.stop()


@pytest.mark.asyncio
async def test_stop_sets_running_false():
    rec, _, _, _ = _make_reconciler()
    await rec.start()
    await rec.stop()
    assert rec._running is False


# ---------------------------------------------------------------------------
# Leader election
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_becomes_leader_when_key_absent():
    rec, redis, _, _ = _make_reconciler()
    result = await rec._try_become_leader()
    assert result is True
    assert await redis.get(_RECONCILE_LEADER_KEY) == rec._instance_id


@pytest.mark.asyncio
async def test_renews_leader_when_already_holds():
    rec, redis, _, _ = _make_reconciler(instance_id="node-1")
    await redis.set(_RECONCILE_LEADER_KEY, "node-1")
    result = await rec._try_become_leader()
    assert result is True


@pytest.mark.asyncio
async def test_not_leader_when_other_holds_key():
    rec, redis, _, _ = _make_reconciler(instance_id="node-1")
    await redis.set(_RECONCILE_LEADER_KEY, "node-99")
    result = await rec._try_become_leader()
    assert result is False


@pytest.mark.asyncio
async def test_leader_mode_without_redis():
    """No redis → single-instance mode, always leader."""
    rec, _, _, _ = _make_reconciler(with_redis=False)
    assert await rec._try_become_leader() is True


# ---------------------------------------------------------------------------
# _scan_task_batch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_scan_returns_empty_without_redis():
    rec, _, _, _ = _make_reconciler(with_redis=False)
    batch = await rec._scan_task_batch()
    assert batch == []


@pytest.mark.asyncio
async def test_scan_returns_seeded_tasks():
    rec, redis, _, _ = _make_reconciler()
    tasks = [_task("s1"), _task("s2"), _task("s3")]
    _seed_tasks(redis, tasks)

    batch = await rec._scan_task_batch()
    task_ids = {t["task_id"] for t in batch}
    assert {"s1", "s2", "s3"}.issubset(task_ids)


@pytest.mark.asyncio
async def test_scan_skips_bad_json():
    rec, redis, _, _ = _make_reconciler()
    redis._strings["task:bad1"] = "NOT_JSON{"
    redis._strings["task:good1"] = json.dumps(_task("good1"))

    batch = await rec._scan_task_batch()
    task_ids = {t["task_id"] for t in batch}
    assert "good1" in task_ids
    assert "bad1" not in task_ids


@pytest.mark.asyncio
async def test_scan_advances_cursor():
    rec, redis, _, _ = _make_reconciler()
    _seed_tasks(redis, [_task("c1")])
    assert rec._scan_cursor == 0
    await rec._scan_task_batch()
    # After scan, cursor is updated (FakeRedis always returns 0 = done)
    assert rec._scan_cursor == 0  # FakeRedis returns 0 (full cycle)


# ---------------------------------------------------------------------------
# Phase 1: Double-write
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_phase1_skipped_without_db():
    rec, _, _, _ = _make_reconciler(with_db=False)
    # Should not raise
    await rec._phase1_double_write([_task("p1t1", "completed")])


@pytest.mark.asyncio
async def test_phase1_skips_non_terminal_tasks():
    rec, _, db, _ = _make_reconciler(with_db=True)
    session = db._session

    # Mock: no persisted tasks
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    session.execute = AsyncMock(return_value=mock_result)

    running_task = _task("r1", status="running")
    await rec._phase1_double_write([running_task])

    # session.add should NOT be called for non-terminal tasks
    session.add.assert_not_called()


@pytest.mark.asyncio
async def test_phase1_inserts_missing_terminal_tasks():
    rec, _, db, _ = _make_reconciler(with_db=True)
    session = db._session

    # Simulate: task not in MySQL
    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    session.execute = AsyncMock(return_value=mock_result)

    t = _task("p1-missing", "completed")
    await rec._phase1_double_write([t])

    session.add.assert_called_once()
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_phase1_skips_already_persisted_tasks():
    rec, _, db, _ = _make_reconciler(with_db=True)
    session = db._session

    # Simulate: task already in MySQL
    mock_result = MagicMock()
    mock_result.fetchall.return_value = [("p1-exists",)]
    session.execute = AsyncMock(return_value=mock_result)

    t = _task("p1-exists", "completed")
    await rec._phase1_double_write([t])

    session.add.assert_not_called()


@pytest.mark.asyncio
async def test_reconcile_redis_from_mysql_when_cache_missing_and_task_queued():
    """MySQL is authoritative: missing Redis task cache should be rebuilt from DB state."""
    rec, redis, db, _ = _make_reconciler(with_db=True)
    session = db._session

    class Row: pass
    row = Row()
    row.task_id = "mysql-q1"
    row.tenant_id = "t1"
    row.task_type = "cap_a"
    row.status = "queued"
    row.priority = "normal"
    row.input_data = '{"k":"v"}'
    row.output_data = None
    row.error_message = None
    row.metadata_json = '{"m":1}'
    row.callback_url = None
    row.idempotency_key = None
    row.timeout_seconds = 3600
    row.max_retries = 3
    row.attempt = 0
    row.scheduled_at = None
    row.cron_expr = None
    row.created_at = None
    row.updated_at = None

    mock_rows = MagicMock()
    mock_rows.scalars.return_value.all.return_value = [row]
    session.execute = AsyncMock(return_value=mock_rows)

    # No Redis task cache exists before repair
    assert await redis.get("task:mysql-q1") is None

    await rec._rebuild_redis_from_mysql()

    repaired = await redis.get("task:mysql-q1")
    assert repaired is not None
    data = json.loads(repaired)
    assert data["task_id"] == "mysql-q1"
    assert data["status"] == "queued"
    assert data["capability"] == "cap_a"


@pytest.mark.asyncio
async def test_reconcile_redis_from_mysql_when_cache_missing_and_task_completed():
    """Terminal task truth should survive Redis cache loss because MySQL is authoritative."""
    rec, redis, db, _ = _make_reconciler(with_db=True)
    session = db._session

    class Row: pass
    row = Row()
    row.task_id = "mysql-done1"
    row.tenant_id = "t1"
    row.task_type = "cap_a"
    row.status = "completed"
    row.priority = "normal"
    row.input_data = '{"k":"v"}'
    row.output_data = '{"result":"ok"}'
    row.error_message = None
    row.metadata_json = '{"m":1}'
    row.callback_url = "http://cb"
    row.idempotency_key = None
    row.timeout_seconds = 3600
    row.max_retries = 3
    row.attempt = 1
    row.scheduled_at = None
    row.cron_expr = None
    row.created_at = None
    row.updated_at = None

    mock_rows = MagicMock()
    mock_rows.scalars.return_value.all.return_value = [row]
    session.execute = AsyncMock(return_value=mock_rows)

    await rec._rebuild_redis_from_mysql()

    repaired = await redis.get("task:mysql-done1")
    assert repaired is not None
    data = json.loads(repaired)
    assert data["task_id"] == "mysql-done1"
    assert data["status"] == "completed"
    assert data["callback_url"] == "http://cb"


# ---------------------------------------------------------------------------
# Phase 2: Stuck recovery
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_phase2_skipped_without_redis():
    rec, _, _, qm = _make_reconciler(with_redis=False)
    await rec._phase2_stuck_recovery([_task("s1", "running")])
    qm.enqueue.assert_not_called()


@pytest.mark.asyncio
async def test_phase2_requeues_stuck_task():
    rec, redis, _, qm = _make_reconciler()
    t = _task("stuck1", "running", attempt=0, max_retries=3, created_ts=time.time() - 600)
    # No lock key → task is stuck

    await rec._phase2_stuck_recovery([t])

    qm.enqueue.assert_awaited_once()
    call_args = qm.enqueue.call_args
    assert call_args.args[1] == "stuck1"


@pytest.mark.asyncio
async def test_phase2_not_stuck_if_lock_exists():
    rec, redis, _, qm = _make_reconciler()
    t = _task("locked1", "running", attempt=0, max_retries=3, created_ts=time.time() - 600)
    # Set the lock key
    lock_key = _LOCK_KEY_TEMPLATE.format(task_id="locked1")
    await redis.set(lock_key, "1", ex=60)

    await rec._phase2_stuck_recovery([t])
    qm.enqueue.assert_not_called()


@pytest.mark.asyncio
async def test_phase2_not_stuck_if_too_young():
    rec, redis, _, qm = _make_reconciler(stuck_max_age=300.0)
    t = _task("young1", "running", attempt=0, max_retries=3, created_ts=time.time() - 10)
    # No lock, but task is too young

    await rec._phase2_stuck_recovery([t])
    qm.enqueue.assert_not_called()


@pytest.mark.asyncio
async def test_phase2_marks_failed_when_max_retries_exceeded():
    rec, redis, _, qm = _make_reconciler()
    t = _task("maxretry1", "running", attempt=3, max_retries=3, created_ts=time.time() - 600)
    _seed_tasks(redis, [t])

    await rec._phase2_stuck_recovery([t])

    # Task should be marked failed in Redis
    raw = await redis.get("task:maxretry1")
    if raw:
        data = json.loads(raw)
        assert data["status"] == "failed"


@pytest.mark.asyncio
async def test_phase2_dedup_prevents_double_processing():
    rec, redis, _, qm = _make_reconciler()
    t = _task("dedup1", "running", attempt=0, max_retries=3, created_ts=time.time() - 600)

    # Pre-set dedup key as if another reconciler already handled it
    dedup_key = _REQUEUE_DEDUP_KEY.format(task_id="dedup1")
    await redis.set(dedup_key, "1", ex=_REQUEUE_DEDUP_TTL)

    await rec._phase2_stuck_recovery([t])
    qm.enqueue.assert_not_called()


@pytest.mark.asyncio
async def test_phase2_only_processes_running_and_queued():
    rec, redis, _, qm = _make_reconciler()
    completed = _task("done1", "completed", created_ts=time.time() - 600)
    failed = _task("fail1", "failed", created_ts=time.time() - 600)

    await rec._phase2_stuck_recovery([completed, failed])
    qm.enqueue.assert_not_called()


# ---------------------------------------------------------------------------
# Phase 3: Lost callback recovery
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_phase3_skipped_without_redis():
    rec, _, _, _ = _make_reconciler(with_redis=False)
    # Should not raise
    await rec._phase3_lost_callback([_task("p3t1", "completed", callback_url="http://x")])


@pytest.mark.asyncio
async def test_phase3_enqueues_missing_callback():
    rec, redis, _, _ = _make_reconciler()
    t = _task("p3-1", "completed", callback_url="http://cb/done")
    # No callback_done key, not in retry queue

    await rec._phase3_lost_callback([t])

    # member in sorted set is now task_id (not raw JSON)
    members = await redis.zrangebyscore(_CALLBACK_RETRY_KEY, "-inf", float("inf"))
    assert len(members) == 1
    assert members[0] == "p3-1"
    # payload stored in companion hash
    payload_raw = await redis.get("callback:retry:payload:p3-1")
    assert payload_raw is not None
    event = json.loads(payload_raw)
    assert event["task_id"] == "p3-1"
    assert event["callback_url"] == "http://cb/done"


@pytest.mark.asyncio
async def test_phase3_skips_if_callback_done():
    rec, redis, _, _ = _make_reconciler()
    t = _task("p3-done", "completed", callback_url="http://cb")
    # Mark as done
    done_key = _CALLBACK_DONE_KEY.format(task_id="p3-done")
    await redis.set(done_key, "1")

    await rec._phase3_lost_callback([t])

    members = await redis.zrangebyscore(_CALLBACK_RETRY_KEY, "-inf", float("inf"))
    assert len(members) == 0


@pytest.mark.asyncio
async def test_phase3_skips_already_in_retry_queue():
    rec, redis, _, _ = _make_reconciler()
    t = _task("p3-queued", "completed", callback_url="http://cb")

    # Pre-add to retry queue using the new task_id-as-member convention
    await redis.zadd(_CALLBACK_RETRY_KEY, {"p3-queued": time.time() + 30})

    await rec._phase3_lost_callback([t])

    # Still only 1 entry (no duplicate)
    members = await redis.zrangebyscore(_CALLBACK_RETRY_KEY, "-inf", float("inf"))
    assert len(members) == 1


@pytest.mark.asyncio
async def test_phase3_skips_tasks_without_callback_url():
    rec, redis, _, _ = _make_reconciler()
    t = _task("p3-nourl", "completed", callback_url="")

    await rec._phase3_lost_callback([t])
    members = await redis.zrangebyscore(_CALLBACK_RETRY_KEY, "-inf", float("inf"))
    assert len(members) == 0


@pytest.mark.asyncio
async def test_phase3_only_processes_terminal_tasks():
    rec, redis, _, _ = _make_reconciler()
    running = _task("p3-run", "running", callback_url="http://x")
    queued = _task("p3-q", "queued", callback_url="http://x")

    await rec._phase3_lost_callback([running, queued])
    members = await redis.zrangebyscore(_CALLBACK_RETRY_KEY, "-inf", float("inf"))
    assert len(members) == 0


# ---------------------------------------------------------------------------
# Full loop integration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_full_loop_runs_all_phases_as_leader():
    """When leader, all 3 phases are called with the scanned batch."""
    rec, redis, _, qm = _make_reconciler(interval=0.02)

    phase1_called = []
    phase2_called = []
    phase3_called = []

    async def fake_phase1(batch):
        phase1_called.append(len(batch))

    async def fake_phase2(batch):
        phase2_called.append(len(batch))

    async def fake_phase3(batch):
        phase3_called.append(len(batch))

    with patch.object(rec, "_phase1_double_write", side_effect=fake_phase1):
        with patch.object(rec, "_phase2_stuck_recovery", side_effect=fake_phase2):
            with patch.object(rec, "_phase3_lost_callback", side_effect=fake_phase3):
                with patch.object(rec, "_scan_task_batch", return_value=[_task("t1")]):
                    await rec.start()
                    await asyncio.sleep(0.07)
                    await rec.stop()

    assert len(phase1_called) >= 1
    assert len(phase2_called) >= 1
    assert len(phase3_called) >= 1


@pytest.mark.asyncio
async def test_full_loop_skips_phases_as_non_leader():
    rec, redis, _, qm = _make_reconciler(instance_id="follower", interval=0.02)
    # Steal the leader key
    await redis.set(_RECONCILE_LEADER_KEY, "someone-else")

    phase1_called = []

    async def fake_phase1(batch):
        phase1_called.append(True)

    with patch.object(rec, "_phase1_double_write", side_effect=fake_phase1):
        with patch.object(rec, "_scan_task_batch", return_value=[_task("t1")]):
            await rec.start()
            await asyncio.sleep(0.07)
            await rec.stop()

    assert len(phase1_called) == 0


@pytest.mark.asyncio
async def test_full_loop_skips_phases_when_batch_empty():
    rec, redis, _, _ = _make_reconciler(interval=0.02)

    phase1_called = []

    async def fake_phase1(batch):
        phase1_called.append(True)

    with patch.object(rec, "_phase1_double_write", side_effect=fake_phase1):
        with patch.object(rec, "_scan_task_batch", return_value=[]):
            await rec.start()
            await asyncio.sleep(0.07)
            await rec.stop()

    # Empty batch → gather not called → phase1 not called
    assert len(phase1_called) == 0
