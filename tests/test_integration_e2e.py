"""
Integration tests: full service path with real async SQLite DB and FakeRedis.
No mocks — services use actual SQLAlchemy async sessions.
"""
from __future__ import annotations

import pytest

from src.models.callback_outbox import CallbackDeliveryStatus, CallbackOutboxRecord
from src.services.callback_ops import CallbackOpsService
from src.services.force_operations import ForceOperationService
from src.services.operator_queries import OperatorQueryService
from src.services.replay_lineage import ReplayLineageService

# ---------------------------------------------------------------------------
# Scenario 1: Replay lineage with real DB
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_replay_lineage_creates_new_task_run(async_db_session_factory):
    svc = ReplayLineageService(session_factory=async_db_session_factory)
    result = await svc.replay_task(
        task_id="task-int-001",
        actor="alice",
        reason="integration test replay",
        from_run_key="rk-old-001",
    )
    assert result["ok"] is True
    assert result["task_id"] == "task-int-001"
    new_rk = result["new_run_key"]
    assert new_rk is not None

    # Verify run was persisted
    from sqlalchemy import select

    from src.models.task_run import TaskRunRecord
    async with async_db_session_factory() as session:
        stmt = select(TaskRunRecord).where(TaskRunRecord.task_id == "task-int-001")
        rows = (await session.execute(stmt)).scalars().all()
    assert len(rows) == 1
    assert rows[0].run_key == new_rk
    assert rows[0].status == "replay_requested"


@pytest.mark.asyncio
async def test_replay_lineage_records_operator_action(async_db_session_factory):
    svc = ReplayLineageService(session_factory=async_db_session_factory)
    await svc.replay_task(
        task_id="task-int-002",
        actor="bob",
        reason="integration audit check",
    )
    # Verify operator action recorded
    ops_svc = OperatorQueryService(async_db_session_factory)
    actions = await ops_svc.list_actions(target_type="task", target_id="task-int-002")
    assert len(actions) >= 1
    assert actions[0].actor == "bob"
    assert actions[0].action_type == "task_replay_requested"


# ---------------------------------------------------------------------------
# Scenario 2: Force-lease-eviction with real Redis
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_force_lease_eviction_deletes_redis_key(async_db_session_factory, fake_redis):
    # Plant a lock key in Redis
    lock_key = "task_lock:task-int-003"
    await fake_redis.set(lock_key, "worker-xyz")
    assert await fake_redis.exists(lock_key) == 1

    async def evict():
        deleted = await fake_redis.delete(lock_key)
        return {"evicted": bool(deleted), "key": lock_key}

    svc = ForceOperationService(session_factory=async_db_session_factory)
    result = await svc.execute_force_operation(
        operation="force_lease_eviction",
        actor="admin_user",
        actor_role="admin",
        reason="stale lock after crash",
        target_type="task",
        target_id="task-int-003",
        task_id="task-int-003",
        executor=evict,
    )
    assert result["ok"] is True
    assert result["side_effect"]["evicted"] is True
    # Redis key should be gone
    assert await fake_redis.exists(lock_key) == 0


# ---------------------------------------------------------------------------
# Scenario 3: Dead-letter callback replay end-to-end
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dead_letter_replay_resets_status(async_db_session_factory):
    # Write a dead-letter record
    async with async_db_session_factory() as session:
        row = CallbackOutboxRecord(
            task_id="task-int-004",
            callback_url="http://example.com/cb",
            payload_json="{}",
            delivery_status=CallbackDeliveryStatus.DEAD_LETTER,
        )
        session.add(row)
        await session.commit()
        outbox_id = row.id

    svc = CallbackOpsService(session_factory=async_db_session_factory)
    ok = await svc.replay_dead_letter(outbox_id, actor="alice", reason="integration retry")
    assert ok is True

    # Verify status reset
    async with async_db_session_factory() as session:
        updated = await session.get(CallbackOutboxRecord, outbox_id)
    assert updated.delivery_status == CallbackDeliveryStatus.PENDING
    assert updated.last_error is None


@pytest.mark.asyncio
async def test_dead_letter_ack_marks_acknowledged(async_db_session_factory):
    async with async_db_session_factory() as session:
        row = CallbackOutboxRecord(
            task_id="task-int-005",
            callback_url="http://example.com/cb",
            payload_json="{}",
            delivery_status=CallbackDeliveryStatus.DEAD_LETTER,
        )
        session.add(row)
        await session.commit()
        outbox_id = row.id

    svc = CallbackOpsService(session_factory=async_db_session_factory)
    ok = await svc.acknowledge_dead_letter(outbox_id, actor="ops-team", reason="reviewed")
    assert ok is True

    async with async_db_session_factory() as session:
        updated = await session.get(CallbackOutboxRecord, outbox_id)
    assert updated.acknowledged_by == "ops-team"
    assert updated.acknowledged_at is not None
