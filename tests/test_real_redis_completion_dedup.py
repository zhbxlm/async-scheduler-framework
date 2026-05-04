from __future__ import annotations

import pytest

pytestmark = pytest.mark.redis_required


from async_scheduler.backends.redis import RedisCompletionDedupBackend


class FakeAsyncRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def set(self, key: str, value: str, ex: int | None = None, px: int | None = None, nx: bool = False):
        if nx and key in self.values:
            return None
        self.values[key] = value
        return True

    async def flushdb(self) -> None:
        self.values.clear()


@pytest.mark.asyncio
@pytest.mark.redis_required
async def test_completion_dedup_backend_uses_redis_set_nx_semantics() -> None:
    client = FakeAsyncRedis()
    backend = RedisCompletionDedupBackend(redis_url="redis://localhost:6379/0", client=client)

    first = await backend.claim_once("task-1:success", ttl_seconds=60)
    second = await backend.claim_once("task-1:success", ttl_seconds=60)

    assert first is True
    assert second is False


@pytest.mark.asyncio
@pytest.mark.redis_required
async def test_completion_dedup_backend_clear_flushes_client() -> None:
    client = FakeAsyncRedis()
    backend = RedisCompletionDedupBackend(redis_url="redis://localhost:6379/0", client=client)

    await backend.claim_once("task-2:success", ttl_seconds=60)
    await backend.clear()
    after_clear = await backend.claim_once("task-2:success", ttl_seconds=60)

    assert after_clear is True
