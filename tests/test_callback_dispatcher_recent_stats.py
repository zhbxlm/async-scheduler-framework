from __future__ import annotations

import json
import time

import pytest


class _FakeRedis:
    def __init__(self) -> None:
        self.retry_items: list[str] = []
        self.dlq_keys: list[str] = []
        self.values: dict[str, str] = {}

    async def zrangebyscore(self, key, start, end, **kwargs):
        return list(self.retry_items)

    async def keys(self, pattern):
        if pattern == "callback:dlq:*":
            return list(self.dlq_keys)
        return []

    async def get(self, key):
        return self.values.get(key)


@pytest.mark.asyncio
async def test_callback_dispatcher_stats_include_recent_retry_and_dlq_items() -> None:
    from async_scheduler.platform.callback import CallbackDispatcher, CallbackEvent

    fake_redis = _FakeRedis()
    retry_evt = CallbackEvent(
        event_id="evt-r1",
        task_id="task-r1",
        callback_url="https://retry.example.com",
        payload={},
        attempt=4,
        next_retry_at=time.time() + 30,
    )
    dlq_evt = CallbackEvent(
        event_id="evt-d1",
        task_id="task-d1",
        callback_url="https://dlq.example.com",
        payload={},
        attempt=10,
    )
    fake_redis.retry_items = [retry_evt.to_json()]
    fake_redis.dlq_keys = ["callback:dlq:evt-d1"]
    fake_redis.values["callback:dlq:evt-d1"] = dlq_evt.to_json()

    dispatcher = CallbackDispatcher(redis_client=fake_redis)
    stats = await dispatcher.get_stats()

    assert len(stats["recent_retry_events"]) == 1
    assert stats["recent_retry_events"][0]["task_id"] == "task-r1"
    assert len(stats["recent_dead_letters"]) == 1
    assert stats["recent_dead_letters"][0]["task_id"] == "task-d1"
