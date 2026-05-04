from __future__ import annotations

import pytest


class _FakeRedis:
    def __init__(self) -> None:
        self.retry_items: list[str] = []
        self.dlq_keys: list[str] = []

    async def zrangebyscore(self, key, start, end, **kwargs):
        return list(self.retry_items)

    async def keys(self, pattern):
        if pattern == "callback:dlq:*":
            return list(self.dlq_keys)
        return []


@pytest.mark.asyncio
async def test_callback_dispatcher_stats_summary() -> None:
    from async_scheduler.platform.callback import CallbackDispatcher

    fake_redis = _FakeRedis()
    fake_redis.retry_items = ["a", "b", "c"]
    fake_redis.dlq_keys = ["callback:dlq:1", "callback:dlq:2"]

    dispatcher = CallbackDispatcher(redis_client=fake_redis)
    stats = await dispatcher.get_stats()

    assert stats["redis_backed"] is True
    assert stats["retry_queue_size"] == 3
    assert stats["dead_letter_size"] == 2
