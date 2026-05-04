from __future__ import annotations

import pytest

pytestmark = pytest.mark.redis_required


from async_scheduler.backends.redis import RedisLockBackend


class EvalFakeAsyncRedis:
    def __init__(self) -> None:
        self.values: dict[str, tuple[str, int | None]] = {}
        self.eval_calls: list[tuple[str, int, tuple[object, ...]]] = []

    async def set(self, key: str, value: str, ex: int | None = None, px: int | None = None, nx: bool = False):
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


    async def pexpire(self, key: str, milliseconds: int) -> int:
        """Millisecond expire - store as seconds for simplicity."""
        return await self.expire(key, max(1, milliseconds // 1000))
    async def eval(self, script: str, numkeys: int, *args: object):
        self.eval_calls.append((script, numkeys, args))
        key = str(args[0])
        token = str(args[1])
        row = self.values.get(key)
        actual = None if row is None else row[0]

        if "del" in script:
            if actual != token:
                return 0
            self.values.pop(key, None)
            return 1

        ttl = int(args[2])
        if actual != token:
            return 0
        self.values[key] = (actual, ttl)
        return 1


@pytest.mark.asyncio
@pytest.mark.redis_required
async def test_release_uses_eval_atomic_compare_delete_when_available() -> None:
    client = EvalFakeAsyncRedis()
    backend = RedisLockBackend(redis_url="redis://localhost:6379/0", client=client)

    handle = await backend.acquire("task:lua-release", ttl=30)
    assert handle is not None

    released = await backend.release(handle)

    assert released is True
    assert client.eval_calls
    assert "del" in client.eval_calls[0][0]
    assert "async-scheduler:lock:task:lua-release" not in client.values


@pytest.mark.asyncio
@pytest.mark.redis_required
async def test_extend_uses_eval_atomic_compare_expire_when_available() -> None:
    client = EvalFakeAsyncRedis()
    backend = RedisLockBackend(redis_url="redis://localhost:6379/0", client=client)

    handle = await backend.acquire("task:lua-extend", ttl=30)
    assert handle is not None

    extended = await backend.extend(handle, ttl=60)

    assert extended is True
    assert client.eval_calls
    assert "expire" in client.eval_calls[-1][0]
    assert client.values["async-scheduler:lock:task:lua-extend"][1] in (60, 60000)  # seconds or ms


@pytest.mark.asyncio
@pytest.mark.redis_required
async def test_eval_release_refuses_wrong_owner() -> None:
    client = EvalFakeAsyncRedis()
    backend = RedisLockBackend(redis_url="redis://localhost:6379/0", client=client)

    handle = await backend.acquire("task:lua-owner", ttl=30)
    assert handle is not None
    client.values["async-scheduler:lock:task:lua-owner"] = ("other-token", 30)

    released = await backend.release(handle)

    assert released is False
    assert client.values["async-scheduler:lock:task:lua-owner"][0] == "other-token"
