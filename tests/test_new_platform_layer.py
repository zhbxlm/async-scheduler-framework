"""Tests for QuotaEnforcer, CapabilityRegistry, ClusterRegistry, TaskRouter."""
from __future__ import annotations

import json
import pytest
from unittest.mock import AsyncMock, MagicMock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_redis_store():
    """In-memory fake async Redis for testing."""
    store = {}
    sets = {}
    r = AsyncMock()

    async def fake_set(key, val, **kwargs):
        store[key] = val if isinstance(val, (str, bytes)) else str(val)
        return True

    async def fake_get(key):
        v = store.get(key)
        if v is None:
            return None
        return v.encode() if isinstance(v, str) else v

    async def fake_hset(key, field=None, value=None, mapping=None):
        if key not in store:
            store[key] = {}
        if mapping:
            store[key].update(mapping)
        elif field is not None:
            store[key][field] = value

    async def fake_hget(key, field):
        d = store.get(key, {})
        val = d.get(field)
        if val is None:
            return None
        return val.encode() if isinstance(val, str) else str(val).encode()

    async def fake_hgetall(key):
        d = store.get(key, {})
        return {k.encode(): str(v).encode() for k, v in d.items()}

    async def fake_delete(*keys):
        for k in keys:
            store.pop(k, None)

    async def fake_sadd(key, *vals):
        sets.setdefault(key, set()).update(vals)

    async def fake_srem(key, *vals):
        sets.get(key, set()).discard(*vals)

    async def fake_smembers(key):
        return {v.encode() if isinstance(v, str) else v for v in sets.get(key, set())}

    async def fake_eval(script, numkeys, *args):
        # Simple pass-through for tests that don't need Lua
        return b"ok"

    r.set = fake_set
    r.get = fake_get
    r.hset = fake_hset
    r.hget = fake_hget
    r.hgetall = fake_hgetall
    r.delete = fake_delete
    r.sadd = fake_sadd
    r.srem = fake_srem
    r.smembers = fake_smembers
    r.eval = fake_eval
    r._store = store
    r._sets = sets
    return r


# ===========================================================================
# QuotaEnforcer
# ===========================================================================

class TestQuotaEnforcer:
    @pytest.mark.asyncio
    async def test_get_usage_empty(self):
        from src.platform.quota_enforcer import QuotaEnforcer
        r = _make_redis_store()
        q = QuotaEnforcer(r)
        usage = await q.get_usage("t1")
        assert usage["task_count"] == 0
        assert usage["gpu_count"] == 0

    @pytest.mark.asyncio
    async def test_reserve_task_submission_no_limit(self):
        """With max_queue_depth=0 (unlimited), increment should succeed."""
        from src.platform.quota_enforcer import QuotaEnforcer, FIELD_TASK_COUNT
        from src.models.tenant import TenantQuota, TenantInfo
        import json

        r = _make_redis_store()
        # Store tenant with unlimited quota
        tenant = TenantInfo(tenant_id="t1", quota=TenantQuota(max_queue_depth=0))
        r._store["tenant:t1"] = tenant.model_dump_json()

        call_args = []
        async def fake_eval(script, numkeys, *args):
            call_args.append(args)
            return b"1"   # new count = 1

        r.eval = fake_eval
        q = QuotaEnforcer(r)
        # Should not raise
        await q.reserve_task_submission("t1")

    @pytest.mark.asyncio
    async def test_quota_exceeded_error_raised(self):
        from src.platform.quota_enforcer import QuotaEnforcer, QuotaExceededError
        from src.models.tenant import TenantQuota, TenantInfo
        import json

        r = _make_redis_store()
        tenant = TenantInfo(tenant_id="t2", quota=TenantQuota(max_queue_depth=2))
        r._store["tenant:t2"] = tenant.model_dump_json()

        async def fake_eval(script, numkeys, *args):
            return b"quota_exceeded:2:2"

        r.eval = fake_eval
        q = QuotaEnforcer(r)
        with pytest.raises(QuotaExceededError) as exc_info:
            await q.reserve_task_submission("t2")
        assert exc_info.value.tenant_id == "t2"

    @pytest.mark.asyncio
    async def test_decrement_task_count(self):
        from src.platform.quota_enforcer import QuotaEnforcer

        r = _make_redis_store()
        calls = []
        async def fake_eval(script, numkeys, *args):
            calls.append(args)
            return b"0"

        r.eval = fake_eval
        q = QuotaEnforcer(r)
        await q.decrement_task_count("t1", delta=1)
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_reconcile_quota_counters_drift(self):
        from src.platform.quota_enforcer import QuotaEnforcer, FIELD_TASK_COUNT

        r = _make_redis_store()
        # Simulate usage key with drifted count
        r._store["tenant_usage:t1"] = {FIELD_TASK_COUNT: "5"}

        async def fake_hget(key, field):
            d = r._store.get(key, {})
            val = d.get(field)
            return str(val).encode() if val is not None else None

        async def fake_hset(key, field=None, value=None, mapping=None):
            if field and value:
                r._store.setdefault(key, {})[field] = value

        r.hget = fake_hget
        r.hset = fake_hset

        q = QuotaEnforcer(r)
        changes = await q.reconcile_quota_counters("t1", actual_task_count=3)
        assert FIELD_TASK_COUNT in changes
        assert changes[FIELD_TASK_COUNT] == 2   # drift = 5 - 3


# ===========================================================================
# CapabilityRegistry
# ===========================================================================

class TestCapabilityRegistry:
    @pytest.mark.asyncio
    async def test_register_and_get(self):
        from src.platform.capability_registry import CapabilityRegistry
        from src.models.capability import CapabilityInfo, CapabilityType

        r = _make_redis_store()
        reg = CapabilityRegistry(r)
        cap = CapabilityInfo(capability_name="inference", cluster_id="c1")
        await reg.register(cap)
        fetched = await reg.get("inference")
        assert fetched is not None
        assert fetched.capability_name == "inference"

    @pytest.mark.asyncio
    async def test_find_capability_filters_unhealthy(self):
        from src.platform.capability_registry import CapabilityRegistry
        from src.models.capability import CapabilityInfo, HealthStatus

        r = _make_redis_store()
        reg = CapabilityRegistry(r)
        cap = CapabilityInfo(
            capability_name="ml", health_status=HealthStatus.UNHEALTHY
        )
        await reg.register(cap)
        # find_capability should return None for UNHEALTHY
        found = await reg.find_capability("ml")
        assert found is None

    @pytest.mark.asyncio
    async def test_find_capability_returns_healthy(self):
        from src.platform.capability_registry import CapabilityRegistry
        from src.models.capability import CapabilityInfo, HealthStatus

        r = _make_redis_store()
        reg = CapabilityRegistry(r)
        cap = CapabilityInfo(capability_name="ml", health_status=HealthStatus.HEALTHY)
        await reg.register(cap)
        found = await reg.find_capability("ml")
        assert found is not None

    @pytest.mark.asyncio
    async def test_unregister(self):
        from src.platform.capability_registry import CapabilityRegistry
        from src.models.capability import CapabilityInfo

        r = _make_redis_store()
        reg = CapabilityRegistry(r)
        cap = CapabilityInfo(capability_name="echo")
        await reg.register(cap)
        await reg.unregister("echo")
        assert await reg.get("echo") is None


# ===========================================================================
# ClusterRegistry
# ===========================================================================

class TestClusterRegistry:
    @pytest.mark.asyncio
    async def test_register_and_get(self):
        from src.platform.cluster_registry import ClusterRegistry
        from src.models.cluster import ClusterInfo

        r = _make_redis_store()
        reg = ClusterRegistry(r)
        cluster = ClusterInfo(cluster_id="c1", ray_head_address="http://10.0.0.1:8265")
        await reg.register(cluster)
        fetched = await reg.get("c1")
        assert fetched is not None
        assert fetched.cluster_id == "c1"

    @pytest.mark.asyncio
    async def test_select_cluster_active_only(self):
        from src.platform.cluster_registry import ClusterRegistry
        from src.models.cluster import ClusterInfo, ClusterStatus, ClusterResources

        r = _make_redis_store()
        reg = ClusterRegistry(r)
        active = ClusterInfo(
            cluster_id="c1",
            status=ClusterStatus.ACTIVE,
            resources=ClusterResources(available_gpus=4),
        )
        offline = ClusterInfo(cluster_id="c2", status=ClusterStatus.OFFLINE)
        await reg.register(active)
        await reg.register(offline)

        selected = await reg.select_cluster()
        assert selected is not None
        assert selected.cluster_id == "c1"

    @pytest.mark.asyncio
    async def test_select_cluster_by_capability(self):
        from src.platform.cluster_registry import ClusterRegistry
        from src.models.cluster import ClusterInfo, ClusterStatus

        r = _make_redis_store()
        reg = ClusterRegistry(r)
        c1 = ClusterInfo(cluster_id="c1", status=ClusterStatus.ACTIVE, capabilities=["inference"])
        c2 = ClusterInfo(cluster_id="c2", status=ClusterStatus.ACTIVE, capabilities=["training"])
        await reg.register(c1)
        await reg.register(c2)

        sel = await reg.select_cluster(capability="inference")
        assert sel is not None
        assert sel.cluster_id == "c1"

    @pytest.mark.asyncio
    async def test_select_cluster_returns_none_when_no_match(self):
        from src.platform.cluster_registry import ClusterRegistry
        r = _make_redis_store()
        reg = ClusterRegistry(r)
        result = await reg.select_cluster(capability="nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_unregister(self):
        from src.platform.cluster_registry import ClusterRegistry
        from src.models.cluster import ClusterInfo

        r = _make_redis_store()
        reg = ClusterRegistry(r)
        await reg.register(ClusterInfo(cluster_id="c1"))
        await reg.unregister("c1")
        assert await reg.get("c1") is None


# ===========================================================================
# TaskRouter
# ===========================================================================

class TestTaskRouter:
    def _make_task_create(self, **kwargs):
        from src.models.task import TaskCreate, TaskPriority
        defaults = dict(task_type="inference", priority=TaskPriority.NORMAL)
        defaults.update(kwargs)
        return TaskCreate(**defaults)

    @pytest.mark.asyncio
    async def test_resolve_dag_orchestrated_by_default(self):
        from src.platform.task_router import TaskRouter
        from src.models.task import TaskDispatchMode

        router = TaskRouter()
        req = self._make_task_create()
        mode = router._resolve_dispatch_mode(req)
        assert mode == TaskDispatchMode.DAG_ORCHESTRATED

    @pytest.mark.asyncio
    async def test_resolve_raydata_explicit(self):
        from src.platform.task_router import TaskRouter
        from src.models.task import TaskDispatchMode

        router = TaskRouter()
        req = self._make_task_create(dispatch_mode=TaskDispatchMode.RAYDATA_NATIVE)
        mode = router._resolve_dispatch_mode(req)
        assert mode == TaskDispatchMode.RAYDATA_NATIVE

    @pytest.mark.asyncio
    async def test_resolve_raydata_by_task_type(self):
        from src.platform.task_router import TaskRouter
        from src.models.task import TaskDispatchMode

        router = TaskRouter(raydata_task_types=["batch_infer"])
        req = self._make_task_create(task_type="batch_infer")
        mode = router._resolve_dispatch_mode(req)
        assert mode == TaskDispatchMode.RAYDATA_NATIVE

    @pytest.mark.asyncio
    async def test_route_dag_enqueues(self):
        from src.platform.task_router import TaskRouter
        from src.models.task import TaskDispatchMode

        mock_queue = AsyncMock()
        router = TaskRouter(queue_manager=mock_queue)
        req = self._make_task_create()

        class FakeRecord:
            task_id = "t1"

        result = await router.route(req, FakeRecord())
        assert result["dispatch_mode"] == TaskDispatchMode.DAG_ORCHESTRATED.value
        mock_queue.enqueue.assert_called_once()

    @pytest.mark.asyncio
    async def test_route_raydata_no_cluster_still_succeeds(self):
        from src.platform.task_router import TaskRouter
        from src.models.task import TaskDispatchMode

        router = TaskRouter(raydata_task_types=["special"])
        req = self._make_task_create(task_type="special")

        class FakeRecord:
            task_id = "t2"

        result = await router.route(req, FakeRecord())
        assert result["dispatch_mode"] == TaskDispatchMode.RAYDATA_NATIVE.value
        assert result["current_step"] == "raydata_submitted"
