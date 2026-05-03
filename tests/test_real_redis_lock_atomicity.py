from __future__ import annotations

import pytest

pytestmark = pytest.mark.redis_required


from async_scheduler.backends.base import LockHandle
from async_scheduler.backends.redis import RedisLockBackend


class RacingFakeAsyncRedis:
    def __init__(self) -> None:
        self.values: dict[str, tuple[str, int | None]] = {}
        self.on_get = None

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False):
        if nx and key in self.values:
            return None
        self.values[key] = (value, ex)
        return True

    async def get(self, key: str):
        row = self.values.get(key)
        value = None if row is None else row[0]
        if self.on_get is not None:
            await self.on_get(key, value)
        return value

    async def compare_delete(self, key: str, expected: str) -> int:
        row = self.values.get(key)
        actual = None if row is None else row[0]
        if self.on_get is not None:
            await self.on_get(key, actual)
            row = self.values.get(key)
            actual = None if row is None else row[0]
        if actual != expected:
            return 0
        self.values.pop(key, None)
        return 1

    async def compare_expire(self, key: str, expected: str, ttl: int) -> bool:
        row = self.values.get(key)
        actual = None if row is None else row[0]
        if self.on_get is not None:
            await self.on_get(key, actual)
            row = self.values.get(key)
            actual = None if row is None else row[0]
        if actual != expected:
            return False
        self.values[key] = (actual, ttl)
        return True

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
async def test_release_does_not_delete_replaced_lock_after_get_delete_race() -> None:
    client = RacingFakeAsyncRedis()
    backend = RedisLockBackend(redis_url="redis://localhost:6379/0", client=client)

    handle = await backend.acquire("task:race-release", ttl=30)
    assert handle is not None

    async def replace_lock(key: str, value: str | None) -> None:
        if value == handle.token:
            client.values[key] = ("new-owner-token", 30)
            client.on_get = None

    client.on_get = replace_lock
    released = await backend.release(handle)

    assert released is False
    assert client.values["async-scheduler:lock:task:race-release"][0] == "new-owner-token"


@pytest.mark.asyncio
async def test_extend_does_not_refresh_replaced_lock_after_get_expire_race() -> None:
    client = RacingFakeAsyncRedis()
    backend = RedisLockBackend(redis_url="redis://localhost:6379/0", client=client)

    handle = await backend.acquire("task:race-extend", ttl=30)
    assert handle is not None

    async def replace_lock(key: str, value: str | None) -> None:
        if value == handle.token:
            client.values[key] = ("new-owner-token", 5)
            client.on_get = None

    client.on_get = replace_lock
    extended = await backend.extend(handle, ttl=60)

    assert extended is False
    assert client.values["async-scheduler:lock:task:race-extend"] == ("new-owner-token", 5)


@pytest.mark.asyncio
async def test_compare_and_release_still_allows_legit_owner_cleanup() -> None:
    client = RacingFakeAsyncRedis()
    backend = RedisLockBackend(redis_url="redis://localhost:6379/0", client=client)

    handle = await backend.acquire("task:normal-release", ttl=30)
    assert handle is not None

    assert await backend.release(handle) is True
    assert "async-scheduler:lock:task:normal-release" not in client.values
