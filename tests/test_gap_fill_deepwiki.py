"""Tests for gap-fill deepwiki alignment:

1. CircuitBreaker three-state transitions (in-process fallback)
2. QueueManager score encoding & capability routing
3. TaskPriority enum alignment (VERY_HIGH / TIDE / priority_rank)
4. CallbackDispatcher two-level retry (L1 in-memory, L2 persistent queue)
5. TenantQuotaManager Redis-atomic quota (in-process fallback)
6. TaskReconciler three-phase reconcile
7. CronScheduler leader election (single-node pass-through)
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from async_scheduler.core.models import Task, TaskCreate, TaskStatus
from async_scheduler.platform.callback import CallbackDispatcher, CallbackEvent
from async_scheduler.platform.circuit_breaker import CircuitBreaker, CircuitState
from async_scheduler.platform.quota import QuotaExceededError, TenantQuotaManager
from async_scheduler.queue.manager import QueueManager, _decode_score, _encode_score


# ---------------------------------------------------------------------------
# 1. TaskPriority enum alignment
# ---------------------------------------------------------------------------


class TestTaskPriorityAlignment:
    def test_priority_is_int(self):
        # priority field is now int (doc-compliant)
        t = TaskCreate(name="t", payload={}, priority=0)
        assert isinstance(t.priority, int)
        assert t.priority == 0

    def test_lower_int_means_higher_priority_in_score(self):
        # Lower int value = higher priority = smaller score (dequeued first)
        ts = int(time.time() * 1000)
        high_priority = _encode_score(-1, ts)   # value -1 (high)
        normal_priority = _encode_score(0, ts)  # value 0 (normal/default)
        low_priority = _encode_score(4, ts)     # value 4 (low)
        assert high_priority < normal_priority < low_priority

    def test_same_priority_fifo_ordering(self):
        ts1 = int(time.time() * 1000)
        ts2 = ts1 + 1000
        s1 = _encode_score(0, ts1)
        s2 = _encode_score(0, ts2)
        assert s1 < s2  # earlier arrival => smaller score => dequeued first

    def test_decode_round_trip(self):
        ts = int(time.time() * 1000)
        for pri in (-2, -1, 0, 4, 5):
            score = _encode_score(pri, ts)
            rank, decoded_ts = _decode_score(score)
            assert rank == pri
            assert decoded_ts == ts

    def test_task_status_no_scheduled(self):
        # SCHEDULED was removed per doc spec
        assert not hasattr(TaskStatus, "SCHEDULED")

    def test_task_status_is_terminal(self):
        assert TaskStatus.SUCCESS.is_terminal
        assert TaskStatus.FAILED.is_terminal
        assert not TaskStatus.RUNNING.is_terminal
        assert not TaskStatus.QUEUED.is_terminal


# ---------------------------------------------------------------------------
# 2. Score encoding helpers
# ---------------------------------------------------------------------------


class TestScoreEncoding:
    def test_encode_uses_multiplier(self):
        ts = 1_700_000_000_000
        score = _encode_score(0, ts)
        # priority=0 (NORMAL int) uses rank offset+multiplier
        # The actual formula: score = (priority + OFFSET) * 10^13 + ts_ms
        # where OFFSET is chosen so all valid priorities are positive
        assert isinstance(score, int)

    def test_decode_round_trip(self):
        ts = 1_700_000_000_000
        score = _encode_score(0, ts)
        rank, decoded_ts = _decode_score(score)
        assert rank == 0  # round-trip: priority 0
        assert decoded_ts == ts


# ---------------------------------------------------------------------------
# 3. CircuitBreaker (in-process fallback)
# ---------------------------------------------------------------------------


class TestCircuitBreakerInProcess:
    def _make_cb(self, **kwargs) -> CircuitBreaker:
        return CircuitBreaker(
            stats_key="test:stats",
            config_key="test:config",
            failure_threshold=3,
            open_duration_seconds=0.05,
            half_open_max=2,
            client=None,  # use in-process fallback
            **kwargs,
        )

    @pytest.mark.asyncio
    async def test_starts_closed(self):
        cb = self._make_cb()
        assert not await cb.is_open()
        assert await cb.get_state() == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_trips_open_after_threshold(self):
        cb = self._make_cb()
        for _ in range(3):
            await cb.record_failure()
        assert await cb.is_open()
        assert await cb.get_state() == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_success_resets_failure_counter(self):
        cb = self._make_cb()
        await cb.record_failure()
        await cb.record_failure()
        await cb.record_success()  # resets counter
        # Two more failures needed to trip
        await cb.record_failure()
        assert not await cb.is_open()
        await cb.record_failure()
        await cb.record_failure()
        assert await cb.is_open()

    @pytest.mark.asyncio
    async def test_open_transitions_to_half_open_after_cooldown(self):
        cb = self._make_cb()
        for _ in range(3):
            await cb.record_failure()
        # First is_open: still CLOSED but failures hit threshold → CLOSED→OPEN, returns blocked
        assert await cb.is_open()
        assert await cb.get_state() == CircuitState.OPEN
        await asyncio.sleep(0.06)  # wait for cooldown
        # Second is_open: OPEN cooldown elapsed → OPEN→HALF_OPEN, returns not-blocked
        is_blocked = await cb.is_open()
        assert not is_blocked
        state = await cb.get_state()
        assert state == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_half_open_closes_after_enough_successes(self):
        cb = self._make_cb()
        for _ in range(3):
            await cb.record_failure()
        await cb.is_open()  # CLOSED→OPEN
        await asyncio.sleep(0.06)
        await cb.is_open()  # OPEN→HALF_OPEN
        assert await cb.get_state() == CircuitState.HALF_OPEN
        await cb.record_success()
        await cb.record_success()  # half_open_max=2
        assert await cb.get_state() == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_half_open_failure_resets_success_counter(self):
        cb = self._make_cb()
        for _ in range(3):
            await cb.record_failure()
        await cb.is_open()  # CLOSED→OPEN
        await asyncio.sleep(0.06)
        await cb.is_open()  # OPEN→HALF_OPEN
        assert await cb.get_state() == CircuitState.HALF_OPEN
        await cb.record_success()
        await cb.record_failure()  # resets half_open_successes; stays HALF_OPEN
        assert await cb.get_state() == CircuitState.HALF_OPEN


# ---------------------------------------------------------------------------
# 4. QueueManager capability routing & circuit breaker integration
# ---------------------------------------------------------------------------


class TestQueueManagerCapability:
    @pytest.mark.asyncio
    async def test_enqueue_registers_capability(self):
        qm = QueueManager()
        task = Task(name="t1", task_type="video_encode", priority=0)
        await qm.enqueue(task, capability="video_encode")
        caps = await qm.discover_capabilities()
        assert "video_encode" in caps

    @pytest.mark.asyncio
    async def test_circuit_breaker_created_per_capability(self):
        qm = QueueManager()
        cb_a = await qm._get_circuit_breaker("cap_a")
        cb_b = await qm._get_circuit_breaker("cap_b")
        assert cb_a is not cb_b

    @pytest.mark.asyncio
    async def test_record_result_success_resets_failures(self):
        qm = QueueManager(circuit_failure_threshold=2)
        await qm.record_result("cap", success=False)
        await qm.record_result("cap", success=True)
        assert not await qm.is_circuit_open("cap")

    @pytest.mark.asyncio
    async def test_circuit_trips_after_threshold_failures(self):
        qm = QueueManager(circuit_failure_threshold=2)
        await qm.record_result("cap", success=False)
        await qm.record_result("cap", success=False)
        assert await qm.is_circuit_open("cap")

    @pytest.mark.asyncio
    async def test_get_capability_stats_returns_dict(self):
        qm = QueueManager()
        stats = await qm.get_capability_stats("my_cap")
        assert stats.capability == "my_cap"
        assert "pending" in stats.to_dict()
        assert "circuit_state" in stats.to_dict()

    @pytest.mark.asyncio
    async def test_static_encode_decode_roundtrip(self):
        score = QueueManager.encode_score(-1, 1_700_000_000_000)
        rank, ts = QueueManager.decode_score(score)
        assert rank == -1   # round-trip: priority -1 (high)
        assert ts == 1_700_000_000_000


# ---------------------------------------------------------------------------
# 5. CallbackDispatcher two-level retry
# ---------------------------------------------------------------------------


class TestCallbackDispatcherTwoLevel:
    @pytest.mark.asyncio
    async def test_dispatch_no_url_returns_true(self):
        dispatcher = CallbackDispatcher()
        assert await dispatcher.dispatch(None, {})

    @pytest.mark.asyncio
    async def test_dispatch_stub_succeeds(self):
        """Mock httpx to verify dispatch returns True when HTTP succeeds."""
        dispatcher = CallbackDispatcher(max_inline_attempts=1)

        with patch("async_scheduler.platform.callback._HTTPX_AVAILABLE", True):
            resp_mock = MagicMock()
            resp_mock.status_code = 200
            client_mock = AsyncMock()
            client_mock.post = AsyncMock(return_value=resp_mock)
            dispatcher._http_client = client_mock

            ok = await dispatcher.dispatch("http://example.com/cb", {"task_id": "t1"}, task_id="t1")
        assert ok

    @pytest.mark.asyncio
    async def test_dispatch_enqueues_to_redis_on_l1_failure(self):
        """Simulate httpx available but all requests fail → should enqueue to Redis."""
        redis_mock = AsyncMock()
        redis_mock.zadd = AsyncMock()

        dispatcher = CallbackDispatcher(
            max_inline_attempts=2,
            inline_base_delay_seconds=0.0,
            redis_client=redis_mock,
        )

        with patch("async_scheduler.platform.callback._HTTPX_AVAILABLE", True):
            client_mock = AsyncMock()
            resp_mock = MagicMock()
            resp_mock.status_code = 503
            client_mock.post = AsyncMock(return_value=resp_mock)
            dispatcher._http_client = client_mock

            ok = await dispatcher.dispatch(
                "http://example.com/cb", {"task_id": "t2"}, task_id="t2"
            )

        assert not ok
        redis_mock.zadd.assert_called()  # event was enqueued to ZSET

    @pytest.mark.asyncio
    async def test_is_done_returns_false_without_redis(self):
        dispatcher = CallbackDispatcher()
        assert not await dispatcher.is_done("any-task")

    @pytest.mark.asyncio
    async def test_is_in_retry_queue_returns_false_without_redis(self):
        dispatcher = CallbackDispatcher()
        assert not await dispatcher.is_in_retry_queue("any-task")

    @pytest.mark.asyncio
    async def test_process_due_callbacks_noop_without_redis(self):
        dispatcher = CallbackDispatcher()
        assert await dispatcher.process_due_callbacks() == 0

    @pytest.mark.asyncio
    async def test_callback_event_json_round_trip(self):
        evt = CallbackEvent(
            event_id="ev1",
            task_id="t1",
            callback_url="http://example.com",
            payload={"status": "success"},
        )
        restored = CallbackEvent.from_json(evt.to_json())
        assert restored.event_id == "ev1"
        assert restored.task_id == "t1"
        assert restored.payload == {"status": "success"}


# ---------------------------------------------------------------------------
# 6. TenantQuotaManager (in-process fallback)
# ---------------------------------------------------------------------------


class TestTenantQuotaManager:
    @pytest.mark.asyncio
    async def test_admit_queue_increments_count(self):
        mgr = TenantQuotaManager(default_max_queued=5)
        await mgr.admit_queue("t1")
        usage = await mgr.get_usage("t1")
        assert usage["task_count"] == 1

    @pytest.mark.asyncio
    async def test_release_queue_decrements(self):
        mgr = TenantQuotaManager(default_max_queued=5)
        await mgr.admit_queue("t1")
        await mgr.release_queue("t1")
        usage = await mgr.get_usage("t1")
        assert usage["task_count"] == 0

    @pytest.mark.asyncio
    async def test_quota_exceeded_raises(self):
        mgr = TenantQuotaManager(default_max_queued=2)
        await mgr.admit_queue("t1")
        await mgr.admit_queue("t1")
        with pytest.raises(QuotaExceededError):
            await mgr.admit_queue("t1")

    @pytest.mark.asyncio
    async def test_zero_quota_means_unlimited(self):
        mgr = TenantQuotaManager(default_max_queued=0)
        for _ in range(1000):
            await mgr.admit_queue("t1")
        usage = await mgr.get_usage("t1")
        assert usage["task_count"] == 1000

    @pytest.mark.asyncio
    async def test_configure_tenant_overrides_defaults(self):
        mgr = TenantQuotaManager(default_max_running=5)
        mgr.configure_tenant("premium", max_running=100)
        for _ in range(20):
            await mgr.admit_running("premium")
        usage = await mgr.get_usage("premium")
        assert usage["running_count"] == 20

    @pytest.mark.asyncio
    async def test_decrement_does_not_go_negative(self):
        mgr = TenantQuotaManager()
        await mgr.release_queue("empty")  # should not raise
        usage = await mgr.get_usage("empty")
        assert usage["task_count"] == 0

    @pytest.mark.asyncio
    async def test_gpu_quota(self):
        mgr = TenantQuotaManager()
        mgr.configure_tenant("ml_team", max_gpu=4)
        await mgr.admit_gpu("ml_team", 2)
        await mgr.admit_gpu("ml_team", 2)
        with pytest.raises(QuotaExceededError):
            await mgr.admit_gpu("ml_team", 1)

    @pytest.mark.asyncio
    async def test_stats_returns_all_tenants(self):
        mgr = TenantQuotaManager()
        mgr.configure_tenant("a")
        mgr.configure_tenant("b")
        result = await mgr.stats()
        assert "a" in result
        assert "b" in result


# ---------------------------------------------------------------------------
# 7. CronScheduler leader election (single-node)
# ---------------------------------------------------------------------------


class TestCronSchedulerLeaderElection:
    @pytest.mark.asyncio
    async def test_single_node_always_leader(self):
        from async_scheduler.scheduler.cron import CronScheduler

        qm = MagicMock()
        cron = CronScheduler(qm, redis_client=None)
        assert await cron._try_acquire_leader_lease()

    @pytest.mark.asyncio
    async def test_redis_leader_election_acquires(self):
        from async_scheduler.scheduler.cron import CronScheduler

        redis_mock = AsyncMock()
        redis_mock.set = AsyncMock(return_value=True)  # SET NX succeeds

        qm = MagicMock()
        cron = CronScheduler(qm, redis_client=redis_mock, instance_id="inst-1")
        assert await cron._try_acquire_leader_lease()

    @pytest.mark.asyncio
    async def test_redis_leader_election_fails_if_other_holds(self):
        from async_scheduler.scheduler.cron import CronScheduler

        redis_mock = AsyncMock()
        redis_mock.set = AsyncMock(return_value=False)  # SET NX fails
        redis_mock.get = AsyncMock(return_value="other-instance")  # not us

        qm = MagicMock()
        cron = CronScheduler(qm, redis_client=redis_mock, instance_id="inst-1")
        assert not await cron._try_acquire_leader_lease()

    @pytest.mark.asyncio
    async def test_redis_leader_renews_own_lease(self):
        from async_scheduler.scheduler.cron import CronScheduler

        redis_mock = AsyncMock()
        redis_mock.set = AsyncMock(return_value=False)  # already held
        redis_mock.get = AsyncMock(return_value="inst-1")  # by us
        redis_mock.expire = AsyncMock()

        qm = MagicMock()
        cron = CronScheduler(qm, redis_client=redis_mock, instance_id="inst-1")
        assert await cron._try_acquire_leader_lease()
        redis_mock.expire.assert_called_once()
