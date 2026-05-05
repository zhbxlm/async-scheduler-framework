"""Tests for CapabilityRegistry, ClusterRegistry."""
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
    def fake_register_script(script):
        return AsyncMock()
    r.register_script = fake_register_script
    return r


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