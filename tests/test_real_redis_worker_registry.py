from __future__ import annotations

import pytest

pytestmark = pytest.mark.redis_required


from async_scheduler.distributed.worker_registry import WorkerInfo, WorkerRegistry


class FakeAsyncRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.ttls: dict[str, int] = {}

    async def hset(self, key: str, mapping: dict[str, str]) -> int:
        self.hashes[key] = dict(mapping)
        return len(mapping)

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def expire(self, key: str, ttl: int) -> bool:
        if key not in self.hashes:
            return False
        self.ttls[key] = ttl
        return True

    async def exists(self, key: str) -> int:
        return 1 if key in self.hashes else 0

    async def delete(self, key: str) -> int:
        existed = key in self.hashes
        self.hashes.pop(key, None)
        self.ttls.pop(key, None)
        return 1 if existed else 0

    async def keys(self, pattern: str) -> list[str]:
        prefix = pattern[:-1] if pattern.endswith("*") else pattern
        return [key for key in self.hashes if key.startswith(prefix)]


@pytest.mark.asyncio
async def test_real_redis_worker_registry_registers_and_lists_workers() -> None:
    client = FakeAsyncRedis()
    registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=30, client=client)

    await registry.register(WorkerInfo(worker_id="worker-r1", name="alpha", capabilities=["echo"]))
    workers = await registry.list_workers()

    assert len(workers) == 1
    assert workers[0].worker_id == "worker-r1"
    assert workers[0].is_live is True


@pytest.mark.asyncio
async def test_real_redis_worker_registry_heartbeat_refreshes_existing_worker() -> None:
    client = FakeAsyncRedis()
    registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=30, client=client)

    await registry.register(WorkerInfo(worker_id="worker-r2", name="beta"))
    updated = await registry.heartbeat("worker-r2")

    assert updated is True
    assert client.ttls["async-scheduler:worker:worker-r2"] == 30


@pytest.mark.asyncio
async def test_real_redis_worker_registry_deregister_removes_worker() -> None:
    client = FakeAsyncRedis()
    registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=30, client=client)

    await registry.register(WorkerInfo(worker_id="worker-r3", name="gamma"))
    removed = await registry.deregister("worker-r3")

    assert removed is True
    assert await registry.is_live("worker-r3") is False
