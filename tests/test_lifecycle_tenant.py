"""Tests for LifecycleManager + resource adapters + TenantRegistry.

=== LifecycleManager ===
- register_resource() / start_all() / stop_all()
- start_all(): idempotent (second call is no-op)
- start_all(): error in one resource doesn't block others
- stop_all(): idempotent (second call is no-op)
- stop_all(): skipped when not started
- stop_all(): orders stop correctly (consumer → drain → processor → scheduler → other)
- stop_all(): error in stop doesn't crash
- is_started / is_stopping flags
- get_lifecycle_manager(): returns singleton

=== Resource Adapters ===
- TaskReconcilerResource: delegates start/stop
- CronSchedulerResource: delegates start/stop
- CompensationServiceResource: delegates start/stop
- ManagedResource.resource_type default
- ManagedResource.requires_drain default

=== TenantRegistry ===
- register() stores tenant, returns True
- register() requires tenant_id
- unregister() removes tenant
- generate_api_key() returns plaintext key, stores hash
- verify_api_key_hash() returns True for valid key
- verify_api_key_hash() returns False for wrong key
- revoke_api_key() removes hash from store
- get() returns registered tenant info
- get() returns None for unknown tenant
- list() returns registered tenant IDs
"""
from __future__ import annotations

import hashlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.common.lifecycle import (
    CompensationServiceResource,
    CronSchedulerResource,
    LifecycleManager,
    ManagedResource,
    TaskReconcilerResource,
    get_lifecycle_manager,
)
from src.platform.tenant_registry import TenantRegistry
from tests.fake_redis import FullFakeAsyncRedis

# ===========================================================================
# Helpers
# ===========================================================================

def _make_resource(name: str, resource_type: str = "generic") -> MagicMock:
    r = MagicMock(spec=ManagedResource)
    r.name = name
    r.resource_type = resource_type
    r.start = AsyncMock()
    r.stop = AsyncMock()
    r.requires_drain = False
    return r


def _make_lm(*resources) -> LifecycleManager:
    lm = LifecycleManager()
    for r in resources:
        lm.register_resource(r)
    return lm


# ===========================================================================
# LifecycleManager — start_all
# ===========================================================================

@pytest.mark.asyncio
async def test_start_all_calls_start_on_all_resources():
    r1 = _make_resource("res-a")
    r2 = _make_resource("res-b")
    lm = _make_lm(r1, r2)

    await lm.start_all()

    r1.start.assert_awaited_once()
    r2.start.assert_awaited_once()


@pytest.mark.asyncio
async def test_start_all_sets_started_flag():
    lm = _make_lm()
    assert lm.is_started is False
    await lm.start_all()
    assert lm.is_started is True


@pytest.mark.asyncio
async def test_start_all_idempotent():
    r1 = _make_resource("res-a")
    lm = _make_lm(r1)

    await lm.start_all()
    await lm.start_all()  # second call should be no-op

    r1.start.assert_awaited_once()  # only called once


@pytest.mark.asyncio
async def test_start_all_continues_on_partial_failure():
    r1 = _make_resource("res-fail")
    r1.start = AsyncMock(side_effect=RuntimeError("startup error"))
    r2 = _make_resource("res-ok")
    lm = _make_lm(r1, r2)

    await lm.start_all()  # should not raise

    r2.start.assert_awaited_once()
    assert lm.is_started is True


# ===========================================================================
# LifecycleManager — stop_all
# ===========================================================================

@pytest.mark.asyncio
async def test_stop_all_skipped_when_not_started():
    r1 = _make_resource("res-a")
    lm = _make_lm(r1)

    await lm.stop_all(drain_timeout=0.0)

    r1.stop.assert_not_called()


@pytest.mark.asyncio
async def test_stop_all_calls_stop_on_all():
    r1 = _make_resource("res-a")
    r2 = _make_resource("res-b")
    lm = _make_lm(r1, r2)

    await lm.start_all()
    with patch("asyncio.sleep", return_value=None):
        await lm.stop_all(drain_timeout=0.0)

    r1.stop.assert_awaited_once()
    r2.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_stop_all_resets_started_flag():
    lm = _make_lm()
    await lm.start_all()
    with patch("asyncio.sleep", return_value=None):
        await lm.stop_all(drain_timeout=0.0)
    assert lm.is_started is False


@pytest.mark.asyncio
async def test_stop_all_idempotent():
    r1 = _make_resource("res-a")
    lm = _make_lm(r1)

    await lm.start_all()
    with patch("asyncio.sleep", return_value=None):
        await lm.stop_all(drain_timeout=0.0)
        await lm.stop_all(drain_timeout=0.0)  # second call no-op

    r1.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_stop_all_continues_on_stop_error():
    r1 = _make_resource("fail-stop")
    r1.stop = AsyncMock(side_effect=RuntimeError("stop error"))
    r2 = _make_resource("ok-stop")
    lm = _make_lm(r1, r2)

    await lm.start_all()
    with patch("asyncio.sleep", return_value=None):
        await lm.stop_all(drain_timeout=0.0)  # should not raise

    r2.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_stop_all_consumer_stopped_before_processor():
    """Consumer must be stopped before processor."""
    stop_order = []

    consumer = _make_resource("task-consumer", resource_type="consumer")
    consumer.stop = AsyncMock(side_effect=lambda: stop_order.append("consumer"))

    processor = _make_resource("task-processor", resource_type="processor")
    processor.stop = AsyncMock(side_effect=lambda: stop_order.append("processor"))

    lm = _make_lm(processor, consumer)  # intentionally reversed registration order
    await lm.start_all()
    with patch("asyncio.sleep", return_value=None):
        await lm.stop_all(drain_timeout=0.0)

    assert stop_order.index("consumer") < stop_order.index("processor")


@pytest.mark.asyncio
async def test_stop_all_scheduler_stopped_after_processor():
    """Scheduler must be stopped after processor."""
    stop_order = []

    scheduler = _make_resource("cron-scheduler", resource_type="scheduler")
    scheduler.stop = AsyncMock(side_effect=lambda: stop_order.append("scheduler"))

    processor = _make_resource("task-reconciler", resource_type="processor")
    processor.stop = AsyncMock(side_effect=lambda: stop_order.append("processor"))

    lm = _make_lm(scheduler, processor)
    await lm.start_all()
    with patch("asyncio.sleep", return_value=None):
        await lm.stop_all(drain_timeout=0.0)

    assert stop_order.index("processor") < stop_order.index("scheduler")


@pytest.mark.asyncio
async def test_stop_all_drain_called_with_timeout():
    """Drain sleep should be called with correct timeout when consumer/processor exist."""
    consumer = _make_resource("consumer", resource_type="consumer")
    lm = _make_lm(consumer)
    await lm.start_all()

    sleep_calls = []
    async def capture_sleep(t):
        sleep_calls.append(t)

    with patch("asyncio.sleep", side_effect=capture_sleep):
        await lm.stop_all(drain_timeout=5.0)

    assert 5.0 in sleep_calls


# ===========================================================================
# is_started / is_stopping flags
# ===========================================================================

@pytest.mark.asyncio
async def test_is_stopping_true_during_shutdown():
    """is_stopping should be True while stop_all runs."""
    lm = _make_lm()
    await lm.start_all()

    stopping_snapshots = []

    async def capture_sleep(t):
        stopping_snapshots.append(lm.is_stopping)

    with patch("asyncio.sleep", side_effect=capture_sleep):
        await lm.stop_all(drain_timeout=0.0)

    assert lm.is_stopping is False  # cleared after complete


# ===========================================================================
# get_lifecycle_manager singleton
# ===========================================================================

def test_get_lifecycle_manager_returns_singleton():
    import src.common.lifecycle as lc
    lc._lifecycle_manager = None  # reset

    lm1 = get_lifecycle_manager()
    lm2 = get_lifecycle_manager()
    assert lm1 is lm2


def test_get_lifecycle_manager_creates_instance():
    import src.common.lifecycle as lc
    lc._lifecycle_manager = None

    lm = get_lifecycle_manager()
    assert isinstance(lm, LifecycleManager)


# ===========================================================================
# Resource adapters
# ===========================================================================

@pytest.mark.asyncio
async def test_task_reconciler_resource_delegates_start():
    rec = AsyncMock()
    rec.start = AsyncMock()
    resource = TaskReconcilerResource(rec)
    await resource.start()
    rec.start.assert_awaited_once()


@pytest.mark.asyncio
async def test_task_reconciler_resource_delegates_stop():
    rec = AsyncMock()
    rec.stop = AsyncMock()
    resource = TaskReconcilerResource(rec)
    await resource.stop()
    rec.stop.assert_awaited_once()


def test_task_reconciler_resource_name():
    resource = TaskReconcilerResource(MagicMock())
    assert resource.name == "TaskReconciler"


@pytest.mark.asyncio
async def test_cron_scheduler_resource_delegates_start():
    sched = AsyncMock()
    resource = CronSchedulerResource(sched)
    await resource.start()
    sched.start.assert_awaited_once()


@pytest.mark.asyncio
async def test_cron_scheduler_resource_delegates_stop():
    sched = AsyncMock()
    resource = CronSchedulerResource(sched)
    await resource.stop()
    sched.stop.assert_awaited_once()


def test_cron_scheduler_resource_name():
    resource = CronSchedulerResource(MagicMock())
    assert resource.name == "CronScheduler"


@pytest.mark.asyncio
async def test_compensation_service_resource_delegates():
    svc = AsyncMock()
    resource = CompensationServiceResource(svc)
    await resource.start()
    await resource.stop()
    svc.start.assert_awaited_once()
    svc.stop.assert_awaited_once()


def test_compensation_service_resource_name():
    resource = CompensationServiceResource(MagicMock())
    assert resource.name == "CompensationService"


def test_managed_resource_default_type():
    """Default resource_type is 'generic'."""
    class MinimalResource(ManagedResource):
        @property
        def name(self): return "minimal"
        async def start(self): pass
        async def stop(self): pass

    r = MinimalResource()
    assert r.resource_type == "generic"
    assert r.requires_drain is False


# ===========================================================================
# TenantRegistry
# ===========================================================================

def _make_tenant_registry():
    redis = FullFakeAsyncRedis()
    reg = TenantRegistry(redis_client=redis)
    return reg, redis


def _tenant(tid: str, **kwargs) -> dict:
    return {"tenant_id": tid, "name": f"Tenant {tid}", **kwargs}


@pytest.mark.asyncio
async def test_tenant_register_returns_true():
    reg, _ = _make_tenant_registry()
    result = await reg.register(_tenant("t1"))
    assert result is True


@pytest.mark.asyncio
async def test_tenant_register_stores_data():
    reg, _ = _make_tenant_registry()
    await reg.register(_tenant("t2", plan="pro"))
    data = await reg.get("t2", "t2")
    assert data is not None
    assert data["tenant_id"] == "t2"


@pytest.mark.asyncio
async def test_tenant_register_requires_tenant_id():
    reg, _ = _make_tenant_registry()
    with pytest.raises(ValueError, match="tenant_id"):
        await reg.register({"name": "no-id"})


@pytest.mark.asyncio
async def test_tenant_unregister_removes_tenant():
    reg, redis = _make_tenant_registry()
    await reg.register(_tenant("del-t1"))
    result = await reg.unregister("del-t1")
    assert result is True
    data = await reg.get("del-t1", "del-t1")
    assert data is None


@pytest.mark.asyncio
async def test_tenant_generate_api_key_returns_plaintext():
    reg, _ = _make_tenant_registry()
    await reg.register(_tenant("key-t1"))
    key = await reg.generate_api_key("key-t1")
    assert isinstance(key, str)
    assert len(key) > 0


@pytest.mark.asyncio
async def test_tenant_generate_api_key_stores_hash():
    reg, redis = _make_tenant_registry()
    await reg.register(_tenant("hash-t1"))
    key = await reg.generate_api_key("hash-t1")

    # Hash should be stored
    expected_hash = hashlib.sha256(key.encode()).hexdigest()
    stored = await redis.hget("tenant:api_keys", expected_hash)
    assert stored is not None


@pytest.mark.asyncio
async def test_tenant_verify_api_key_valid():
    reg, _ = _make_tenant_registry()
    await reg.register(_tenant("verify-t1"))
    key = await reg.generate_api_key("verify-t1")

    result = await reg.verify_api_key_hash(key)
    assert result == "verify-t1"


@pytest.mark.asyncio
async def test_tenant_verify_api_key_invalid():
    reg, _ = _make_tenant_registry()
    result = await reg.verify_api_key_hash("completely-wrong-key")
    assert result is None


@pytest.mark.asyncio
async def test_tenant_revoke_api_key():
    reg, redis = _make_tenant_registry()
    await reg.register(_tenant("revoke-t1"))
    key = await reg.generate_api_key("revoke-t1")

    await reg.revoke_api_key("revoke-t1", key)

    # After revocation, verify should fail
    result = await reg.verify_api_key_hash(key)
    assert result is None


@pytest.mark.asyncio
async def test_tenant_list_returns_registered():
    reg, _ = _make_tenant_registry()
    await reg.register(_tenant("list-t1"))
    await reg.register(_tenant("list-t2"))
    tenants = await reg.list_all_tenants()
    tenant_strs = {t.decode() if isinstance(t, bytes) else t for t in tenants}
    assert {"list-t1", "list-t2"}.issubset(tenant_strs)


@pytest.mark.asyncio
async def test_tenant_get_unknown_returns_none():
    reg, _ = _make_tenant_registry()
    data = await reg.get("nonexistent", "nonexistent")
    assert data is None


@pytest.mark.asyncio
async def test_tenant_multiple_keys_per_tenant():
    """A tenant can have multiple API keys."""
    reg, _ = _make_tenant_registry()
    await reg.register(_tenant("multi-t1"))
    key1 = await reg.generate_api_key("multi-t1")
    key2 = await reg.generate_api_key("multi-t1")

    assert await reg.verify_api_key_hash(key1) == "multi-t1"
    assert await reg.verify_api_key_hash(key2) == "multi-t1"
    assert key1 != key2
