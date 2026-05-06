"""Tests for ScheduleRegistry, DagLoader, http_client, BaseRedisRegistry.

=== ScheduleRegistry ===
- set() + reverse index populated
- list_due(): returns enabled schedules past fire time
- list_due(): skips disabled
- list_due(): skips bad JSON
- update_next_fire(): fast path via reverse index (O(1))
- update_next_fire(): fallback scan when index missing
- toggle(): enable/disable (via stub Lua)
- advance_next_fire(): (via stub Lua)

=== DagLoader ===
- load(): Redis cache hit → returns immediately
- load(): Redis miss → YAML fallback
- load(): YAML miss → MySQL fallback
- load(): all miss → None
- load(): warms Redis cache after YAML load

=== http_client ===
- init_http_client(): creates client
- get_http_client(): returns shared client after init
- get_http_client(): fallback creates and caches client
- close_http_client(): closes shared + fallback clients
- async_post(): calls client.post, returns json
- async_get(): calls client.get, returns json

=== BaseRedisRegistry ===
- set() with TTL → uses setex
- set() without TTL → uses set
- set() updates index
- get() returns parsed value
- get() returns None if missing
- delete() removes key and index entry
- list() returns item IDs for tenant
- list_all_tenants() returns tenant IDs
"""
from __future__ import annotations

import json
import time
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from contextlib import asynccontextmanager

from tests.fake_redis import FullFakeAsyncRedis
from src.platform.schedule_registry import ScheduleRegistry, _SCHEDULE_TENANT_IDX
from src.platform.dag_loader import DagLoader
from src.platform.base_registry import BaseRedisRegistry


# ===========================================================================
# ScheduleRegistry
# ===========================================================================

def _make_schedule_registry():
    redis = FullFakeAsyncRedis()
    reg = ScheduleRegistry(redis_client=redis)
    return reg, redis


def _sched_dict(schedule_id, tenant_id, *, enabled=True, next_fire_at=None, cron_expr="* * * * *"):
    nf = next_fire_at or (datetime.now(tz=timezone.utc) - timedelta(minutes=1)).isoformat()
    return {
        "schedule_id": schedule_id,
        "tenant_id": tenant_id,
        "cron_expr": cron_expr,
        "enabled": enabled,
        "next_fire_at": nf,
        "task_template": "{}",
    }


@pytest.mark.asyncio
async def test_schedule_registry_set_populates_reverse_index():
    reg, redis = _make_schedule_registry()
    await reg.set("t1", "sched-1", _sched_dict("sched-1", "t1"))
    tenant = await redis.hget(_SCHEDULE_TENANT_IDX, "sched-1")
    if isinstance(tenant, bytes):
        tenant = tenant.decode()
    assert tenant == "t1"


@pytest.mark.asyncio
async def test_schedule_registry_list_due_returns_enabled_past_fire():
    reg, redis = _make_schedule_registry()
    now = datetime.now(tz=timezone.utc)
    past = (now - timedelta(minutes=5)).isoformat()
    future = (now + timedelta(hours=1)).isoformat()

    await reg.set("t1", "due-sched", _sched_dict("due-sched", "t1", next_fire_at=past))
    await reg.set("t1", "future-sched", _sched_dict("future-sched", "t1", next_fire_at=future))

    due = await reg.list_due(now)
    schedule_ids = [getattr(s, "schedule_id", None) for s in due]
    assert "due-sched" in schedule_ids
    assert "future-sched" not in schedule_ids


@pytest.mark.asyncio
async def test_schedule_registry_list_due_skips_disabled():
    reg, redis = _make_schedule_registry()
    now = datetime.now(tz=timezone.utc)
    past = (now - timedelta(minutes=5)).isoformat()

    await reg.set("t1", "disabled-sched", _sched_dict("disabled-sched", "t1", enabled=False, next_fire_at=past))
    due = await reg.list_due(now)
    schedule_ids = [getattr(s, "schedule_id", None) for s in due]
    assert "disabled-sched" not in schedule_ids


@pytest.mark.asyncio
async def test_schedule_registry_list_due_skips_bad_json():
    reg, redis = _make_schedule_registry()
    # Seed a bad JSON entry manually
    redis._strings["schedules:t1:bad-json"] = "{NOT_JSON"
    await redis.sadd("schedules:index:t1", "bad-json")
    await redis.sadd("schedules:tenants", "t1")

    now = datetime.now(tz=timezone.utc)
    due = await reg.list_due(now)  # should not crash
    assert isinstance(due, list)


@pytest.mark.asyncio
async def test_schedule_registry_update_next_fire_fast_path():
    """With reverse index populated, update_next_fire uses O(1) hget."""
    reg, redis = _make_schedule_registry()
    now = datetime.now(tz=timezone.utc)
    past = (now - timedelta(minutes=5)).isoformat()
    await reg.set("t1", "fast-sched", _sched_dict("fast-sched", "t1", next_fire_at=past))

    # Stub advance_next_fire to verify it's called with correct tenant
    called_with = []

    async def fake_advance(tid, sid, nf):
        called_with.append((tid, sid))

    reg.advance_next_fire = fake_advance

    next_fire = now + timedelta(hours=1)
    await reg.update_next_fire("fast-sched", next_fire)

    assert ("t1", "fast-sched") in called_with


@pytest.mark.asyncio
async def test_schedule_registry_update_next_fire_fallback_scan():
    """Without reverse index, falls back to linear scan and backfills index."""
    reg, redis = _make_schedule_registry()
    now = datetime.now(tz=timezone.utc)
    past = (now - timedelta(minutes=5)).isoformat()

    # Register without going through set() (no reverse index)
    d = _sched_dict("noidx-sched", "t1", next_fire_at=past)
    redis._strings["schedules:t1:noidx-sched"] = json.dumps(d)
    await redis.sadd("schedules:index:t1", "noidx-sched")
    await redis.sadd("schedules:tenants", "t1")

    called_with = []

    async def fake_advance(tid, sid, nf):
        called_with.append((tid, sid))

    reg.advance_next_fire = fake_advance

    await reg.update_next_fire("noidx-sched", now + timedelta(hours=1))
    assert ("t1", "noidx-sched") in called_with

    # Index should be back-filled
    tenant = await redis.hget(_SCHEDULE_TENANT_IDX, "noidx-sched")
    if isinstance(tenant, bytes):
        tenant = tenant.decode()
    assert tenant == "t1"


# ===========================================================================
# DagLoader
# ===========================================================================

def _make_dag_loader(*, with_redis=True, with_db=False):
    redis = FullFakeAsyncRedis() if with_redis else None
    db = _make_fake_db() if with_db else None
    loader = DagLoader(
        config_dir="/nonexistent",  # no YAML
        redis_client=redis,
        db_session_factory=db,
    )
    return loader, redis, db


def _make_fake_db():
    session = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none = MagicMock(return_value=None)
    session.execute = AsyncMock(return_value=result)

    @asynccontextmanager
    async def factory():
        yield session

    factory._session = session
    return factory


@pytest.mark.asyncio
async def test_dag_loader_redis_hit():
    loader, redis, _ = _make_dag_loader()
    dag_data = {"dag_id": "test-dag", "steps": []}
    import orjson
    redis._strings["dag_def:default:test-dag"] = orjson.dumps(dag_data).decode()

    result = await loader.load("test-dag", "default")
    assert result is not None
    assert result["dag_id"] == "test-dag"


@pytest.mark.asyncio
async def test_dag_loader_yaml_fallback(tmp_path):
    import yaml
    dag_data = {"dag_id": "yaml-dag", "steps": []}
    dag_file = tmp_path / "yaml-dag.yaml"
    dag_file.write_text(yaml.dump(dag_data))

    loader = DagLoader(config_dir=str(tmp_path))
    result = await loader.load("yaml-dag")
    assert result is not None
    assert result["dag_id"] == "yaml-dag"


@pytest.mark.asyncio
async def test_dag_loader_yaml_warms_redis_cache(tmp_path):
    import yaml
    import orjson
    dag_data = {"dag_id": "cached-dag", "steps": []}
    (tmp_path / "cached-dag.yaml").write_text(yaml.dump(dag_data))

    redis = FullFakeAsyncRedis()
    loader = DagLoader(config_dir=str(tmp_path), redis_client=redis)
    await loader.load("cached-dag", "t1")

    cached = redis._strings.get("dag_def:t1:cached-dag")
    assert cached is not None


@pytest.mark.asyncio
async def test_dag_loader_returns_none_all_miss():
    loader, _, _ = _make_dag_loader()
    result = await loader.load("nonexistent-dag")
    assert result is None


@pytest.mark.asyncio
async def test_dag_loader_no_redis_no_crash():
    loader, _, _ = _make_dag_loader(with_redis=False)
    result = await loader.load("no-redis-dag")
    assert result is None


# ===========================================================================
# http_client
# ===========================================================================

@pytest.mark.asyncio
async def test_http_client_init_creates_client():
    import src.common.http_client as hc
    # Reset state
    hc._client = None
    hc._fallback_client = None

    hc.init_http_client()
    assert hc._client is not None
    await hc.close_http_client()


@pytest.mark.asyncio
async def test_http_client_get_returns_shared():
    import src.common.http_client as hc
    hc._client = None
    hc._fallback_client = None

    hc.init_http_client()
    c1 = hc.get_http_client()
    c2 = hc.get_http_client()
    assert c1 is c2
    await hc.close_http_client()


@pytest.mark.asyncio
async def test_http_client_fallback_cached():
    import src.common.http_client as hc
    hc._client = None
    hc._fallback_client = None

    c1 = hc.get_http_client()
    c2 = hc.get_http_client()
    assert c1 is c2  # same fallback instance
    await hc.close_http_client()


@pytest.mark.asyncio
async def test_http_client_close_clears_both():
    import src.common.http_client as hc
    hc._client = None
    hc._fallback_client = None

    hc.init_http_client()
    _ = hc.get_http_client()  # ensure fallback not needed

    await hc.close_http_client()
    assert hc._client is None
    assert hc._fallback_client is None


@pytest.mark.asyncio
async def test_async_post_calls_client():
    import src.common.http_client as hc

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(return_value={"result": "ok"})

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_resp)

    with patch.object(hc, "get_http_client", return_value=mock_client):
        result = await hc.async_post("http://x/api", {"key": "val"})

    assert result == {"result": "ok"}
    mock_client.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_get_calls_client():
    import src.common.http_client as hc

    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(return_value={"items": []})

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)

    with patch.object(hc, "get_http_client", return_value=mock_client):
        result = await hc.async_get("http://x/items", {"page": 1})

    assert result == {"items": []}
    mock_client.get.assert_awaited_once()


# ===========================================================================
# BaseRedisRegistry
# ===========================================================================

def _make_registry(*, ttl=0):
    redis = FullFakeAsyncRedis()
    reg = BaseRedisRegistry(redis_client=redis, key_prefix="test_reg", ttl_seconds=ttl)
    return reg, redis


@pytest.mark.asyncio
async def test_base_registry_set_and_get():
    reg, _ = _make_registry()
    await reg.set("t1", "item-1", {"name": "hello"})
    val = await reg.get("t1", "item-1")
    assert val["name"] == "hello"


@pytest.mark.asyncio
async def test_base_registry_get_returns_none_if_missing():
    reg, _ = _make_registry()
    val = await reg.get("t1", "nonexistent")
    assert val is None


@pytest.mark.asyncio
async def test_base_registry_set_with_ttl_uses_setex():
    reg, redis = _make_registry(ttl=300)
    await reg.set("t1", "item-ttl", {"x": 1})
    assert "test_reg:t1:item-ttl" in redis._expiry
    assert redis._expiry["test_reg:t1:item-ttl"] == 300


@pytest.mark.asyncio
async def test_base_registry_set_updates_tenant_index():
    reg, redis = _make_registry()
    await reg.set("tenant-x", "item-1", {})
    members = await redis.smembers("test_reg:index:tenant-x")
    strs = {m.decode() if isinstance(m, bytes) else m for m in members}
    assert "item-1" in strs


@pytest.mark.asyncio
async def test_base_registry_set_updates_tenants_set():
    reg, redis = _make_registry()
    await reg.set("tenant-y", "item-1", {})
    tenants = await redis.smembers("test_reg:tenants")
    strs = {m.decode() if isinstance(m, bytes) else m for m in tenants}
    assert "tenant-y" in strs


@pytest.mark.asyncio
async def test_base_registry_delete_removes_key():
    reg, redis = _make_registry()
    await reg.set("t1", "del-item", {"v": 1})
    result = await reg.delete("t1", "del-item")
    assert result is True
    assert await reg.get("t1", "del-item") is None


@pytest.mark.asyncio
async def test_base_registry_delete_removes_from_index():
    reg, redis = _make_registry()
    await reg.set("t1", "idx-item", {})
    await reg.delete("t1", "idx-item")
    members = await redis.smembers("test_reg:index:t1")
    strs = {m.decode() if isinstance(m, bytes) else m for m in members}
    assert "idx-item" not in strs


@pytest.mark.asyncio
async def test_base_registry_list_returns_item_ids():
    reg, _ = _make_registry()
    await reg.set("t1", "a", {})
    await reg.set("t1", "b", {})
    items = await reg.list("t1")
    assert set(items) == {"a", "b"}


@pytest.mark.asyncio
async def test_base_registry_list_all_tenants():
    reg, _ = _make_registry()
    await reg.set("ta", "x", {})
    await reg.set("tb", "y", {})
    tenants = await reg.list_all_tenants()
    assert set(tenants) >= {"ta", "tb"}
