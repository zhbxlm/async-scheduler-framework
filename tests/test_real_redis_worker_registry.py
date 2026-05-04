"""Tests for WorkerRegistry backed by a fake Redis client.

The FakeAsyncRedis here supports both hashes and plain strings so
that the dual-key architecture (hash + liveness key) can be tested
without a real Redis server.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.redis_required


from async_scheduler.distributed.worker_registry import WorkerInfo, WorkerRegistry


class FakeAsyncRedis:
    """Minimal in-memory Redis fake supporting hset/hgetall/set/get/exists/delete/keys/pexpire."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.strings: dict[str, str] = {}
        self.ttls: dict[str, int] = {}  # stored as seconds for test assertions

    # --- Hash ops ---
    async def hset(self, key: str, mapping: dict[str, str]) -> int:
        self.hashes[key] = dict(mapping)
        return len(mapping)

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    # --- String ops ---
    async def set(self, key: str, value: str) -> bool:
        self.strings[key] = value
        return True

    async def get(self, key: str) -> str | None:
        return self.strings.get(key)

    # --- TTL ops ---
    async def expire(self, key: str, ttl: int) -> bool:
        if key in self.hashes or key in self.strings:
            self.ttls[key] = ttl
            return True
        return False

    async def pexpire(self, key: str, milliseconds: int) -> bool:
        seconds = max(1, milliseconds // 1000)
        return await self.expire(key, seconds)

    # --- Generic ops ---
    async def exists(self, key: str) -> int:
        return 1 if (key in self.hashes or key in self.strings) else 0

    async def delete(self, key: str) -> int:
        existed = key in self.hashes or key in self.strings
        self.hashes.pop(key, None)
        self.strings.pop(key, None)
        self.ttls.pop(key, None)
        return 1 if existed else 0

    async def keys(self, pattern: str) -> list[str]:
        prefix = pattern[:-1] if pattern.endswith("*") else pattern
        all_keys = list(self.hashes.keys()) + list(self.strings.keys())
        return [k for k in all_keys if k.startswith(prefix)]


@pytest.mark.asyncio
async def test_real_redis_worker_registry_registers_and_lists_workers() -> None:
    client = FakeAsyncRedis()
    registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=30, client=client)

    await registry.register(WorkerInfo(worker_id="worker-r1", name="alpha"))
    workers = await registry.list_workers()

    assert len(workers) == 1
    assert workers[0].worker_id == "worker-r1"
    assert workers[0].is_stale is False


@pytest.mark.asyncio
async def test_real_redis_worker_registry_heartbeat_refreshes_existing_worker() -> None:
    client = FakeAsyncRedis()
    registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=30, client=client)

    await registry.register(WorkerInfo(worker_id="worker-r2", name="beta"))
    updated = await registry.heartbeat("worker-r2")

    assert updated is True
    # liveness key TTL should be set (stored as seconds in fake)
    liveness_key = "async-scheduler:worker:worker-r2:live"
    assert liveness_key in client.ttls
    assert client.ttls[liveness_key] == 30


@pytest.mark.asyncio
async def test_real_redis_worker_registry_deregister_removes_worker() -> None:
    client = FakeAsyncRedis()
    registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=30, client=client)

    await registry.register(WorkerInfo(worker_id="worker-r3", name="gamma"))
    removed = await registry.deregister("worker-r3")

    assert removed is True
    assert await registry.is_live("worker-r3") is False
    assert await registry.list_workers(include_stale=True) == []


@pytest.mark.asyncio
async def test_real_redis_worker_registry_stale_still_visible_with_include_stale() -> None:
    """After liveness key expires the hash persists; include_stale=True shows it."""
    client = FakeAsyncRedis()
    registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=30, client=client)

    await registry.register(WorkerInfo(worker_id="worker-stale", name="stale"))

    # Simulate liveness key expiry by removing it from the fake
    liveness_key = "async-scheduler:worker:worker-stale:live"
    client.strings.pop(liveness_key, None)

    assert await registry.is_live("worker-stale") is False

    # Worker hash still present → visible with include_stale
    workers = await registry.list_workers(include_stale=True)
    assert len(workers) == 1
    assert workers[0].worker_id == "worker-stale"
    assert workers[0].is_stale is True

    # Excluded when include_stale=False
    assert await registry.list_workers() == []
