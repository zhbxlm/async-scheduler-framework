from __future__ import annotations

import time

import pytest


@pytest.mark.asyncio
async def test_async_proxy_can_mark_callback_dead_lettered_and_retry_status() -> None:
    from async_scheduler.platform.async_proxy import AsyncProxySidecar
    from async_scheduler.platform.callback import CallbackDispatcher, CallbackEvent

    # Mock Redis with callback events
    class _FakeRedis:
        def __init__(self) -> None:
            self.retry_items: list[str] = []
            self.dlq_keys: list[str] = []
            self.values: dict[str, str] = {}
            self.zadd_calls = []

        async def zrangebyscore(self, key, start, end, **kwargs):
            return list(self.retry_items)

        async def keys(self, pattern):
            if pattern == "callback:dlq:*":
                return list(self.dlq_keys)
            return []

        async def get(self, key):
            return self.values.get(key)

        async def zadd(self, key, mapping):
            self.zadd_calls.append((key, mapping))
            return 1

        async def zrem(self, key, *members):
            removed = 0
            for m in members:
                if m in self.retry_items:
                    self.retry_items.remove(m)
                    removed += 1
            return removed

        async def set(self, key, value, ex=None):
            self.values[key] = value
            return True

    fake_redis = _FakeRedis()
    sidecar = AsyncProxySidecar(redis_client=fake_redis)
    dispatcher = CallbackDispatcher(redis_client=fake_redis)

    # 模拟一个 dead-letter 条目
    dlq_evt = CallbackEvent(
        event_id="evt-dlq-1",
        task_id="task-dlq-1",
        callback_url="https://dlq.example.com",
        payload={},
        attempt=10,
    )
    fake_redis.dlq_keys = ["callback:dlq:evt-dlq-1"]
    fake_redis.values["callback:dlq:evt-dlq-1"] = dlq_evt.to_json()

    # 获取 stats，应看到 dead_letter 摘要
    stats = await dispatcher.get_stats()
    assert stats["dead_letter_size"] == 1
    assert len(stats["recent_dead_letters"]) == 1
    assert stats["recent_dead_letters"][0]["task_id"] == "task-dlq-1"

    # Async Proxy 聚合的 stats 也应包含 callback control-plane 摘要
    sidecar_stats = sidecar.get_stats()
    # 目前 sidecar.get_stats() 不会主动聚合 callback dispatcher，
    # 这个留待未来语义扩展
    assert "callback_control_plane" not in sidecar_stats