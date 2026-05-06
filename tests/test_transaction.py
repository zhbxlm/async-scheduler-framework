"""Tests for data consistency: AtomicWriteCoordinator + CompensationService."""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest
import fakeredis.aioredis as fakeredis

from src.common.transaction import (
    AtomicWriteCoordinator,
    TxRecord,
    TxStatus,
    atomic_task_create,
    atomic_task_status_update,
)
from src.services.compensation import CompensationService


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def fake_redis():
    r = fakeredis.FakeRedis()
    return r


@pytest.fixture
def coordinator(fake_redis):
    return AtomicWriteCoordinator(
        mysql_session_factory=None,
        redis_client=fake_redis,
    )


# ──────────────────────────────────────────────────────────────────────────────
# AtomicWriteCoordinator — success path
# ──────────────────────────────────────────────────────────────────────────────

class TestAtomicWriteCoordinatorSuccess:
    @pytest.mark.asyncio
    async def test_both_succeed(self, coordinator, fake_redis):
        """Both MySQL and Redis succeed → COMMITTED."""
        mysql_called = []
        redis_called = []

        def mysql_fn():
            mysql_called.append(True)

        async def redis_fn():
            redis_called.append(True)
            await fake_redis.set("task:abc", b"data")

        tx = await coordinator.write_atomic(
            task_id="abc",
            operation="create_task",
            mysql_write_fn=mysql_fn,
            redis_write_fn=redis_fn,
        )

        assert tx.status == TxStatus.COMMITTED
        assert tx.mysql_written is True
        assert tx.redis_written is True
        assert mysql_called == [True]
        assert redis_called == [True]
        val = await fake_redis.get("task:abc")
        assert val == b"data"

    @pytest.mark.asyncio
    async def test_committed_no_tx_key_saved(self, coordinator, fake_redis):
        """Committed tx should NOT leave a tx:* key in Redis."""
        def mysql_fn(): pass
        async def redis_fn(): pass

        tx = await coordinator.write_atomic(
            task_id="xyz",
            operation="create_task",
            mysql_write_fn=mysql_fn,
            redis_write_fn=redis_fn,
            tx_id="tx-test-xyz",
        )
        assert tx.status == TxStatus.COMMITTED
        # No pending tx should be left
        key = await fake_redis.get("tx:tx-test-xyz")
        assert key is None


# ──────────────────────────────────────────────────────────────────────────────
# AtomicWriteCoordinator — failure paths
# ──────────────────────────────────────────────────────────────────────────────

class TestAtomicWriteCoordinatorFailures:
    @pytest.mark.asyncio
    async def test_mysql_failure_raises(self, coordinator):
        """MySQL failure should raise (abort, nothing to compensate)."""
        def mysql_fn():
            raise RuntimeError("DB down")

        async def redis_fn():
            pass

        with pytest.raises(RuntimeError, match="DB down"):
            await coordinator.write_atomic(
                task_id="fail-task",
                operation="create_task",
                mysql_write_fn=mysql_fn,
                redis_write_fn=redis_fn,
            )

    @pytest.mark.asyncio
    async def test_redis_failure_saves_pending_tx(self, coordinator, fake_redis):
        """Redis failure → tx saved as FAILED (for compensation)."""
        mysql_called = []

        def mysql_fn():
            mysql_called.append(True)

        async def redis_fn():
            raise ConnectionError("Redis unavailable")

        tx = await coordinator.write_atomic(
            task_id="partial-task",
            operation="create_task",
            mysql_write_fn=mysql_fn,
            redis_write_fn=redis_fn,
            tx_id="tx-partial-001",
        )

        assert tx.status == TxStatus.FAILED
        assert tx.mysql_written is True
        assert tx.redis_written is False
        assert "Redis unavailable" in tx.last_error

        # Tx should be saved in Redis for compensation
        saved = await fake_redis.get("tx:tx-partial-001")
        assert saved is not None
        data = json.loads(saved)
        assert data["status"] == TxStatus.FAILED.value
        assert data["task_id"] == "partial-task"

    @pytest.mark.asyncio
    async def test_redis_failure_calls_compensation_if_provided(self, fake_redis):
        """Redis failure should enqueue compensation if service provided."""
        compensation = MagicMock()
        compensation.enqueue = AsyncMock()

        coordinator = AtomicWriteCoordinator(
            mysql_session_factory=None,
            redis_client=fake_redis,
            compensation_service=compensation,
        )

        def mysql_fn(): pass
        async def redis_fn():
            raise ConnectionError("Redis down")

        tx = await coordinator.write_atomic(
            task_id="comp-task",
            operation="create_task",
            mysql_write_fn=mysql_fn,
            redis_write_fn=redis_fn,
        )

        assert tx.status == TxStatus.FAILED
        compensation.enqueue.assert_called_once_with(tx)


# ──────────────────────────────────────────────────────────────────────────────
# Convenience helpers
# ──────────────────────────────────────────────────────────────────────────────

class TestConvenienceFunctions:
    @pytest.mark.asyncio
    async def test_atomic_task_create(self, coordinator):
        """atomic_task_create should delegate correctly."""
        mysql_called = []
        redis_called = []

        def mysql_insert(): mysql_called.append(True)
        async def redis_set(): redis_called.append(True)

        tx = await atomic_task_create(
            coordinator=coordinator,
            task_id="task-create-1",
            mysql_insert=mysql_insert,
            redis_set=redis_set,
        )

        assert tx.status == TxStatus.COMMITTED
        assert tx.operation == "create_task"

    @pytest.mark.asyncio
    async def test_atomic_task_status_update(self, coordinator):
        """atomic_task_status_update should delegate correctly."""
        def mysql_update(): pass
        async def redis_update(): pass

        tx = await atomic_task_status_update(
            coordinator=coordinator,
            task_id="task-update-1",
            mysql_update=mysql_update,
            redis_update=redis_update,
        )

        assert tx.status == TxStatus.COMMITTED
        assert tx.operation == "update_status"


# ──────────────────────────────────────────────────────────────────────────────
# TxRecord dataclass
# ──────────────────────────────────────────────────────────────────────────────

class TestTxRecord:
    def test_defaults(self):
        tx = TxRecord(tx_id="tx-1", task_id="t-1", operation="create_task")
        assert tx.status == TxStatus.PENDING_REDIS
        assert tx.mysql_written is False
        assert tx.redis_written is False
        assert tx.retry_count == 0
        assert tx.last_error == ""
        assert tx.payload == {}

    def test_custom_payload(self):
        tx = TxRecord(
            tx_id="tx-2",
            task_id="t-2",
            operation="update_status",
            payload={"status": "done"},
        )
        assert tx.payload == {"status": "done"}


# ──────────────────────────────────────────────────────────────────────────────
# CompensationService
# ──────────────────────────────────────────────────────────────────────────────

class TestCompensationService:
    @pytest.mark.asyncio
    async def test_start_and_stop(self, fake_redis):
        """Service should start and stop cleanly."""
        svc = CompensationService(
            mysql_session_factory=None,
            redis_client=fake_redis,
            scan_interval_seconds=999,  # Don't actually run scan
        )
        await svc.start()
        assert svc._running is True
        await svc.stop()
        assert svc._running is False

    @pytest.mark.asyncio
    async def test_enqueue_saves_tx(self, fake_redis):
        """Enqueue should save tx to Redis."""
        svc = CompensationService(
            mysql_session_factory=None,
            redis_client=fake_redis,
        )
        tx = TxRecord(
            tx_id="tx-enqueue-1",
            task_id="t-1",
            operation="create_task",
            status=TxStatus.FAILED,
        )
        await svc.enqueue(tx)

        saved = await fake_redis.get("tx:tx-enqueue-1")
        assert saved is not None
        data = json.loads(saved)
        assert data["tx_id"] == "tx-enqueue-1"
        assert data["status"] == TxStatus.FAILED.value

    @pytest.mark.asyncio
    async def test_scan_pending_transactions_empty(self, fake_redis):
        """No pending transactions → returns empty list."""
        svc = CompensationService(
            mysql_session_factory=None,
            redis_client=fake_redis,
        )
        result = await svc._scan_pending_transactions()
        assert result == []

    @pytest.mark.asyncio
    async def test_scan_pending_transactions_finds_failed(self, fake_redis):
        """Scan should find FAILED transactions."""
        svc = CompensationService(
            mysql_session_factory=None,
            redis_client=fake_redis,
        )

        # Manually insert a failed tx
        tx_data = json.dumps({
            "tx_id": "tx-failed-1",
            "task_id": "t-failed-1",
            "operation": "create_task",
            "status": TxStatus.FAILED.value,
            "mysql_written": True,
            "redis_written": False,
            "created_at": time.time(),
            "retry_count": 0,
            "last_error": "Redis down",
            "payload": {},
        })
        await fake_redis.set("tx:tx-failed-1", tx_data, ex=300)

        result = await svc._scan_pending_transactions()
        assert len(result) == 1
        assert result[0].tx_id == "tx-failed-1"
        assert result[0].status == TxStatus.FAILED

    @pytest.mark.asyncio
    async def test_scan_ignores_committed(self, fake_redis):
        """Scan should skip COMMITTED transactions."""
        svc = CompensationService(
            mysql_session_factory=None,
            redis_client=fake_redis,
        )

        tx_data = json.dumps({
            "tx_id": "tx-committed-1",
            "task_id": "t-committed-1",
            "operation": "create_task",
            "status": TxStatus.COMMITTED.value,
            "mysql_written": True,
            "redis_written": True,
            "created_at": time.time(),
            "retry_count": 0,
            "last_error": "",
            "payload": {},
        })
        await fake_redis.set("tx:tx-committed-1", tx_data, ex=300)

        result = await svc._scan_pending_transactions()
        assert result == []

    @pytest.mark.asyncio
    async def test_mark_expired_removes_key(self, fake_redis):
        """Marking expired should delete the tx key."""
        svc = CompensationService(
            mysql_session_factory=None,
            redis_client=fake_redis,
        )

        # Insert a tx
        tx = TxRecord(
            tx_id="tx-expire-1",
            task_id="t-1",
            operation="create_task",
            status=TxStatus.FAILED,
        )
        await svc.enqueue(tx)

        # Mark expired
        await svc._mark_expired(tx)

        # Should be gone
        result = await fake_redis.get("tx:tx-expire-1")
        assert result is None
        assert tx.status == TxStatus.EXPIRED

    @pytest.mark.asyncio
    async def test_max_retries_marks_expired(self, fake_redis):
        """Exceeded max retries should mark tx as expired."""
        svc = CompensationService(
            mysql_session_factory=None,
            redis_client=fake_redis,
            max_retries=2,
        )

        tx = TxRecord(
            tx_id="tx-max-retries",
            task_id="t-1",
            operation="create_task",
            status=TxStatus.FAILED,
            retry_count=3,  # Exceeds max_retries=2
        )

        await svc._repair_transaction(tx)
        assert tx.status == TxStatus.EXPIRED


# ──────────────────────────────────────────────────────────────────────────────
# Integration: Coordinator + Compensation
# ──────────────────────────────────────────────────────────────────────────────

class TestIntegration:
    @pytest.mark.asyncio
    async def test_redis_failure_then_compensation_repair(self, fake_redis):
        """Full flow: Redis fails → compensation enqueued → scan picks it up."""
        svc = CompensationService(
            mysql_session_factory=None,
            redis_client=fake_redis,
        )

        coordinator = AtomicWriteCoordinator(
            mysql_session_factory=None,
            redis_client=fake_redis,
            compensation_service=svc,
        )

        redis_broken = True

        def mysql_fn(): pass
        async def redis_fn():
            if redis_broken:
                raise ConnectionError("Redis temporarily down")

        # Step 1: Atomic write fails on Redis
        tx = await coordinator.write_atomic(
            task_id="int-task-1",
            operation="create_task",
            mysql_write_fn=mysql_fn,
            redis_write_fn=redis_fn,
            tx_id="tx-int-001",
        )
        assert tx.status == TxStatus.FAILED

        # Step 2: Compensation service finds the pending tx
        pending = await svc._scan_pending_transactions()
        assert any(t.tx_id == "tx-int-001" for t in pending)

    @pytest.mark.asyncio
    async def test_no_leftover_tx_on_success(self, fake_redis):
        """Successful atomic write leaves no pending tx in Redis."""
        svc = CompensationService(
            mysql_session_factory=None,
            redis_client=fake_redis,
        )

        coordinator = AtomicWriteCoordinator(
            mysql_session_factory=None,
            redis_client=fake_redis,
            compensation_service=svc,
        )

        def mysql_fn(): pass
        async def redis_fn():
            await fake_redis.set("task:int-task-2", b"ok")

        tx = await coordinator.write_atomic(
            task_id="int-task-2",
            operation="create_task",
            mysql_write_fn=mysql_fn,
            redis_write_fn=redis_fn,
        )

        assert tx.status == TxStatus.COMMITTED

        # No pending txs should exist
        pending = await svc._scan_pending_transactions()
        assert pending == []
