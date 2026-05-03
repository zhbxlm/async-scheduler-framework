from __future__ import annotations

import pytest

pytestmark = pytest.mark.redis_required


from async_scheduler.backends.redis import RedisLockBackend
from async_scheduler.backends.base import LockHandle


class FakeAsyncRedis:
    def __init__(self) -> None:
        self.values: dict[str, tuple[str, int | None]] = {}

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False):
        if nx and key in self.values:
            return None
        self.values[key] = (value, ex)
        return True

    async def get(self, key: str):
        row = self.values.get(key)
        return None if row is None else row[0]

    async def delete(self, key: str):
        existed = key in self.values
        self.values.pop(key, None)
        return 1 if existed else 0

    async def expire(self, key: str, ttl: int):
        if key not in self.values:
            return False
        value, _ = self.values[key]
        self.values[key] = (value, ttl)
        return True


@pytest.mark.asyncio
async def test_real_redis_lock_backend_uses_set_nx_for_acquire() -> None:
    client = FakeAsyncRedis()
    backend = RedisLockBackend(redis_url="redis://localhost:6379/0", client=client)

    first = await backend.acquire("task:1", ttl=30)
    second = await backend.acquire("task:1", ttl=30)

    assert first is not None
    assert second is None


@pytest.mark.asyncio
async def test_real_redis_lock_backend_release_requires_matching_token() -> None:
    client = FakeAsyncRedis()
    backend = RedisLockBackend(redis_url="redis://localhost:6379/0", client=client)

    handle = await backend.acquire("task:2", ttl=30)
    assert handle is not None

    wrong = LockHandle(key="task:2", token="wrong")
    released_wrong = await backend.release(wrong)
    released_right = await backend.release(handle)

    assert released_wrong is False
    assert released_right is True


@pytest.mark.asyncio
async def test_real_redis_lock_backend_extend_requires_matching_token() -> None:
    client = FakeAsyncRedis()
    backend = RedisLockBackend(redis_url="redis://localhost:6379/0", client=client)

    handle = await backend.acquire("task:3", ttl=30)
    assert handle is not None

    wrong = LockHandle(key="task:3", token="wrong")
    extended_wrong = await backend.extend(wrong, ttl=60)
    extended_right = await backend.extend(handle, ttl=60)

    assert extended_wrong is False
    assert extended_right is True
