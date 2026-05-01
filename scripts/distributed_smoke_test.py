#!/usr/bin/env python3
"""Distributed transition smoke test for the Redis-backed coordination path.

Run directly:
    python scripts/distributed_smoke_test.py

This smoke variant does not require a live Redis server. It validates the
real-Redis-path coordination flow using a shared fake async Redis client.
If fakeredis.aioredis is available, it also runs an additional compatibility
check against fakeredis.
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from async_scheduler.backends.redis import (
    RedisCompletionDedupBackend,
    RedisLockBackend,
    RedisQueueBackend,
)
from async_scheduler.core.models import Task, TaskPriority, TaskStatus
from async_scheduler.distributed.worker_registry import WorkerInfo, WorkerRegistry

__test__ = False


class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    YELLOW = "\033[93m"
    RESET = "\033[0m"
    BOLD = "\033[1m"


def print_success(msg: str) -> None:
    print(f"{Colors.GREEN}✓ {msg}{Colors.RESET}")


def print_error(msg: str) -> None:
    print(f"{Colors.RED}✗ {msg}{Colors.RESET}")


def print_skip(msg: str) -> None:
    print(f"{Colors.YELLOW}- {msg}{Colors.RESET}")


def print_header(msg: str) -> None:
    print(f"\n{Colors.BOLD}{Colors.BLUE}=== {msg} ==={Colors.RESET}\n")


class SharedFakeAsyncRedis:
    def __init__(self) -> None:
        self.values: dict[str, tuple[str, int | None]] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.lists: dict[str, list[str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False):
        if nx and key in self.values:
            return None
        self.values[key] = (value, ex)
        return True

    async def get(self, key: str):
        row = self.values.get(key)
        return None if row is None else row[0]

    async def delete(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if key in self.values:
                del self.values[key]
                count += 1
            if key in self.hashes:
                del self.hashes[key]
                count += 1
            if key in self.lists:
                del self.lists[key]
                count += 1
            if key in self.zsets:
                del self.zsets[key]
                count += 1
        return count

    async def expire(self, key: str, ttl: int):
        if key in self.values:
            value, _ = self.values[key]
            self.values[key] = (value, ttl)
            return True
        if key in self.hashes:
            return True
        return False

    async def rpush(self, key: str, value: str) -> int:
        self.lists.setdefault(key, []).append(value)
        return len(self.lists[key])

    async def lpop(self, key: str):
        values = self.lists.get(key, [])
        if not values:
            return None
        return values.pop(0)

    async def llen(self, key: str) -> int:
        return len(self.lists.get(key, []))

    async def lrem(self, key: str, count: int, value: str) -> int:
        values = self.lists.get(key, [])
        removed = 0
        kept: list[str] = []
        for item in values:
            if item == value and (count == 0 or removed < count):
                removed += 1
                continue
            kept.append(item)
        self.lists[key] = kept
        return removed

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            bucket[member] = score
        return len(mapping)

    async def zrangebyscore(self, key: str, min_score: float, max_score: float):
        bucket = self.zsets.get(key, {})
        return [member for member, score in bucket.items() if min_score <= score <= max_score]

    async def zrem(self, key: str, member: str) -> int:
        bucket = self.zsets.get(key, {})
        existed = member in bucket
        bucket.pop(member, None)
        return 1 if existed else 0

    async def hset(self, key: str, mapping: dict[str, str]) -> int:
        self.hashes[key] = dict(mapping)
        return len(mapping)

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def exists(self, key: str) -> int:
        return 1 if key in self.hashes or key in self.values else 0

    async def keys(self, pattern: str) -> list[str]:
        prefix = pattern[:-1] if pattern.endswith("*") else pattern
        return [key for key in self.hashes if key.startswith(prefix)]

    async def flushdb(self) -> None:
        self.values.clear()
        self.hashes.clear()
        self.lists.clear()
        self.zsets.clear()


async def smoke_shared_client_coordination() -> bool:
    print_header("Distributed Coordination via Shared Fake Redis")
    try:
        client = SharedFakeAsyncRedis()
        queue = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)
        lock = RedisLockBackend(redis_url="redis://localhost:6379/0", client=client)
        dedup = RedisCompletionDedupBackend(redis_url="redis://localhost:6379/0", client=client)
        workers = WorkerRegistry(redis_url="redis://localhost:6379/0", client=client)

        task = Task(
            id="distributed-smoke-task",
            name="distributed-smoke-task",
            payload={"smoke": True},
            priority=TaskPriority.NORMAL,
            status=TaskStatus.PENDING,
            created_at=datetime.utcnow(),
        )

        await workers.register(WorkerInfo(worker_id="smoke-worker", name="smoke"))
        assert await workers.is_live("smoke-worker") is True
        print_success("Worker registry path works")

        await queue.enqueue(task)
        candidate = await queue.dequeue()
        assert candidate is not None and candidate.id == task.id
        print_success("Queue path works")

        lease = await lock.acquire(f"task:{task.id}", ttl=30)
        assert lease is not None
        assert await lock.extend(lease, ttl=45) is True
        print_success("Lock path works")

        first = await dedup.claim_once(f"{task.id}:success", ttl_seconds=60)
        second = await dedup.claim_once(f"{task.id}:success", ttl_seconds=60)
        assert first is True and second is False
        print_success("Completion dedupe path works")

        assert await lock.release(lease) is True
        assert await workers.deregister("smoke-worker") is True
        print_success("Release and deregister path works")
        return True
    except Exception as e:
        print_error(f"Shared fake Redis smoke failed: {e}")
        return False


async def smoke_fakeredis_compat() -> bool:
    print_header("fakeredis Compatibility")
    try:
        import fakeredis.aioredis as fakeredis_aioredis
    except Exception:
        print_skip("fakeredis.aioredis unavailable; compatibility check skipped")
        return True

    try:
        client = fakeredis_aioredis.FakeRedis(decode_responses=True)
        queue = RedisQueueBackend(redis_url="redis://localhost:6379/0", client=client)
        lock = RedisLockBackend(redis_url="redis://localhost:6379/0", client=client)
        dedup = RedisCompletionDedupBackend(redis_url="redis://localhost:6379/0", client=client)
        workers = WorkerRegistry(redis_url="redis://localhost:6379/0", client=client)

        task = Task(
            id="fakeredis-smoke-task",
            name="fakeredis-smoke-task",
            payload={"smoke": "fakeredis"},
            priority=TaskPriority.NORMAL,
            status=TaskStatus.PENDING,
            created_at=datetime.utcnow(),
        )

        await workers.register(WorkerInfo(worker_id="fakeredis-smoke-worker", name="fakeredis"))
        await queue.enqueue(task)
        assert (await queue.dequeue()) is not None
        lease = await lock.acquire(f"task:{task.id}", ttl=30)
        assert lease is not None
        assert await dedup.claim_once(f"{task.id}:success", ttl_seconds=60) is True
        assert await lock.release(lease) is True
        print_success("fakeredis compatibility path works")
        return True
    except Exception as e:
        print_error(f"fakeredis compatibility smoke failed: {e}")
        return False


async def run_all() -> int:
    tests = [
        smoke_shared_client_coordination,
        smoke_fakeredis_compat,
    ]
    passed = 0
    failed = 0
    for test in tests:
        if await test():
            passed += 1
        else:
            failed += 1
    print(f"\nPassed: {passed}  Failed: {failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run_all()))
