from __future__ import annotations

import asyncio

import pytest

from async_scheduler.distributed.worker_registry import WorkerInfo, WorkerRegistry


@pytest.mark.asyncio
class TestWorkerRegistry:
    async def test_worker_registers_and_heartbeats(self) -> None:
        registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=1.0)

        worker = WorkerInfo(worker_id="worker-1", name="primary", capabilities=["default"])
        await registry.register(worker)

        workers = await registry.list_workers()
        assert len(workers) == 1
        assert workers[0].worker_id == "worker-1"
        assert workers[0].is_live is True

        await asyncio.sleep(0.05)
        await registry.heartbeat("worker-1")

        workers = await registry.list_workers()
        assert len(workers) == 1
        assert workers[0].worker_id == "worker-1"
        assert workers[0].is_live is True

    async def test_stale_worker_becomes_not_live_after_ttl(self) -> None:
        registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=0.1)

        worker = WorkerInfo(worker_id="worker-stale", name="stale")
        await registry.register(worker)

        assert await registry.is_live("worker-stale") is True
        await asyncio.sleep(0.15)
        assert await registry.is_live("worker-stale") is False

        workers = await registry.list_workers(include_stale=True)
        assert len(workers) == 1
        assert workers[0].worker_id == "worker-stale"
        assert workers[0].is_live is False

    async def test_multiple_workers_appear_independently(self) -> None:
        registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=1.0)

        await registry.register(WorkerInfo(worker_id="worker-a", name="alpha", capabilities=["echo"]))
        await registry.register(WorkerInfo(worker_id="worker-b", name="beta", capabilities=["compute"]))

        workers = await registry.list_workers()
        worker_ids = {worker.worker_id for worker in workers}

        assert worker_ids == {"worker-a", "worker-b"}

    async def test_deregister_removes_worker(self) -> None:
        registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=1.0)

        await registry.register(WorkerInfo(worker_id="worker-x", name="x"))
        assert await registry.is_live("worker-x") is True

        removed = await registry.deregister("worker-x")
        assert removed is True
        assert await registry.is_live("worker-x") is False
        assert await registry.list_workers() == []

    async def test_unknown_worker_heartbeat_returns_false(self) -> None:
        registry = WorkerRegistry(redis_url="redis://localhost:6379/0", heartbeat_ttl_seconds=1.0)

        updated = await registry.heartbeat("missing-worker")

        assert updated is False
