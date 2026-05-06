"""Tests for CronScheduler — leader election, schedule firing, idempotency.

Covers:
- start / stop lifecycle
- leader election (NX SET)
- leader re-election (instance already holds key)
- non-leader skips firing
- fire_due_schedules calls create_task for each due schedule
- idempotency key format (cron:<id>:<minute_bucket>)
- fire_one advances next_fire_at via update_next_fire
- fire_one exception is swallowed (loop continues)
- create_task exception is swallowed (update_next_fire still called)
- malformed task_template is tolerated
- multiple schedules all fire in one tick
"""
from __future__ import annotations

import asyncio
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch, call
from dataclasses import dataclass, field
from typing import Any

from tests.fake_redis import FullFakeAsyncRedis
from src.platform.cron_scheduler import CronScheduler, _LEADER_KEY, _LEADER_TTL


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

@dataclass
class FakeSchedule:
    schedule_id: str
    cron_expr: str
    tenant_id: str
    task_template: Any = field(default_factory=dict)  # dict or JSON str


def _make_scheduler(
    schedules: list[FakeSchedule] | None = None,
    *,
    instance_id: str = "test-instance",
    poll_interval: float = 9999.0,  # effectively disabled; we call _run_loop manually
    use_real_redis: bool = True,
) -> tuple[CronScheduler, FullFakeAsyncRedis, AsyncMock, AsyncMock]:
    """Build a CronScheduler with fake dependencies.

    Returns (scheduler, fake_redis, mock_schedule_repo, mock_task_creator).
    """
    fake_redis = FullFakeAsyncRedis()

    due_schedules = schedules or []

    mock_repo = AsyncMock()
    mock_repo.list_due = AsyncMock(return_value=due_schedules)
    mock_repo.update_next_fire = AsyncMock()

    mock_creator = AsyncMock()
    mock_creator.create_task = AsyncMock()

    scheduler = CronScheduler(
        redis_client=fake_redis,
        schedule_repository=mock_repo,
        task_creator=mock_creator,
        instance_id=instance_id,
        poll_interval=poll_interval,
        leader_ttl=_LEADER_TTL,
    )
    return scheduler, fake_redis, mock_repo, mock_creator


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_sets_running_flag():
    scheduler, _, _, _ = _make_scheduler(poll_interval=9999.0)
    assert scheduler._running is False
    task = asyncio.create_task(scheduler.start())
    await asyncio.sleep(0.02)
    assert scheduler._running is True
    await scheduler.stop()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


@pytest.mark.asyncio
async def test_stop_sets_running_false():
    scheduler, _, _, _ = _make_scheduler(poll_interval=9999.0)
    task = asyncio.create_task(scheduler.start())
    await asyncio.sleep(0.02)
    await scheduler.stop()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
    assert scheduler._running is False


# ---------------------------------------------------------------------------
# Leader election
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_becomes_leader_when_key_absent():
    scheduler, fake_redis, _, _ = _make_scheduler()
    result = await scheduler._try_become_leader()
    assert result is True
    stored = await fake_redis.get(_LEADER_KEY)
    assert stored == "test-instance"


@pytest.mark.asyncio
async def test_remains_leader_when_already_holds_key():
    scheduler, fake_redis, _, _ = _make_scheduler(instance_id="node-1")
    # Pre-set the key as if we already won
    await fake_redis.set(_LEADER_KEY, "node-1")
    result = await scheduler._try_become_leader()
    assert result is True


@pytest.mark.asyncio
async def test_not_leader_when_key_held_by_other():
    scheduler, fake_redis, _, _ = _make_scheduler(instance_id="node-1")
    await fake_redis.set(_LEADER_KEY, "node-99")
    result = await scheduler._try_become_leader()
    assert result is False


@pytest.mark.asyncio
async def test_non_leader_does_not_fire_schedules():
    s = FakeSchedule("s1", "* * * * *", "tenant_a", {"capability": "cap_a"})
    scheduler, fake_redis, mock_repo, mock_creator = _make_scheduler(schedules=[s])
    # Steal the leader key
    await fake_redis.set(_LEADER_KEY, "someone-else")

    # Manually run one loop iteration
    scheduler._running = True
    if await scheduler._try_become_leader():
        await scheduler._fire_due_schedules()
    scheduler._running = False

    mock_creator.create_task.assert_not_called()


@pytest.mark.asyncio
async def test_leader_election_sets_ttl():
    """After winning, the key should have an expiry set."""
    scheduler, fake_redis, _, _ = _make_scheduler()
    await scheduler._try_become_leader()
    # FakeRedis tracks expiry
    assert _LEADER_KEY in fake_redis._expiry
    assert fake_redis._expiry[_LEADER_KEY] == _LEADER_TTL


@pytest.mark.asyncio
async def test_leader_refresh_extends_ttl():
    """Re-acquiring as incumbent calls expire() to refresh the TTL."""
    scheduler, fake_redis, _, _ = _make_scheduler(instance_id="node-1")
    await fake_redis.set(_LEADER_KEY, "node-1")
    await scheduler._try_become_leader()
    assert fake_redis._expiry.get(_LEADER_KEY) == _LEADER_TTL


# ---------------------------------------------------------------------------
# Schedule firing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fire_due_schedules_calls_create_task():
    s = FakeSchedule("sched-1", "0 * * * *", "tenant_x", {"capability": "cap_a"})
    scheduler, _, mock_repo, mock_creator = _make_scheduler(schedules=[s])

    now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    await scheduler._fire_due_schedules()

    mock_creator.create_task.assert_awaited_once()
    kwargs = mock_creator.create_task.call_args.kwargs
    assert kwargs["tenant_id"] == "tenant_x"
    assert "idempotency_key" in kwargs


@pytest.mark.asyncio
async def test_fire_multiple_due_schedules():
    schedules = [
        FakeSchedule(f"s{i}", "* * * * *", "t1", {"capability": "cap_a"})
        for i in range(5)
    ]
    scheduler, _, mock_repo, mock_creator = _make_scheduler(schedules=schedules)
    await scheduler._fire_due_schedules()
    assert mock_creator.create_task.await_count == 5


@pytest.mark.asyncio
async def test_fire_one_updates_next_fire():
    s = FakeSchedule("sched-2", "0 9 * * *", "tenant_y", {"capability": "cap_b"})
    scheduler, _, mock_repo, mock_creator = _make_scheduler(schedules=[s])

    now = datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
    await scheduler._fire_one(s, now)

    mock_repo.update_next_fire.assert_awaited_once()
    args = mock_repo.update_next_fire.call_args.args
    assert args[0] == "sched-2"
    # next fire should be after now
    next_fire: datetime = args[1]
    assert next_fire > now


@pytest.mark.asyncio
async def test_fire_one_does_not_update_next_fire_when_create_fails():
    """If create_task raises, update_next_fire should NOT be called."""
    s = FakeSchedule("s-fail", "* * * * *", "t1", {"capability": "cap_a"})
    scheduler, _, mock_repo, mock_creator = _make_scheduler(schedules=[s])
    mock_creator.create_task = AsyncMock(side_effect=RuntimeError("create failed"))

    now = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    # Should not raise
    await scheduler._fire_one(s, now)

    mock_repo.update_next_fire.assert_not_awaited()


@pytest.mark.asyncio
async def test_fire_one_create_task_exception_does_not_propagate():
    """Exception in create_task is swallowed; loop should continue."""
    s = FakeSchedule("s-boom", "0 * * * *", "t2", {"capability": "cap_a"})
    scheduler, _, _, mock_creator = _make_scheduler(schedules=[s])
    mock_creator.create_task = AsyncMock(side_effect=RuntimeError("boom"))

    # Must not raise
    now = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    await scheduler._fire_one(s, now)


# ---------------------------------------------------------------------------
# Idempotency key
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_idempotency_key_format():
    """Idempotency key = 'cron:<schedule_id>:<minute_bucket>'."""
    s = FakeSchedule("sched-idem", "* * * * *", "t1", {"capability": "cap_a"})
    scheduler, _, _, mock_creator = _make_scheduler(schedules=[s])

    now = datetime(2024, 6, 1, 15, 37, 45, tzinfo=timezone.utc)  # :37:45
    await scheduler._fire_one(s, now)

    kwargs = mock_creator.create_task.call_args.kwargs
    idem_key = kwargs["idempotency_key"]

    # Expected bucket: now.timestamp() // 60 * 60
    expected_bucket = int(now.timestamp() // 60 * 60)
    assert idem_key == f"cron:sched-idem:{expected_bucket}"


@pytest.mark.asyncio
async def test_same_minute_same_idempotency_key():
    """Two fires in the same minute produce the same idempotency key."""
    s = FakeSchedule("sched-x", "* * * * *", "t1", {})
    scheduler, _, _, mock_creator = _make_scheduler(schedules=[s])

    now1 = datetime(2024, 6, 1, 12, 5, 0, tzinfo=timezone.utc)
    now2 = datetime(2024, 6, 1, 12, 5, 55, tzinfo=timezone.utc)  # same minute

    await scheduler._fire_one(s, now1)
    await scheduler._fire_one(s, now2)

    keys = [c.kwargs["idempotency_key"] for c in mock_creator.create_task.call_args_list]
    assert keys[0] == keys[1]


@pytest.mark.asyncio
async def test_different_minutes_different_idempotency_keys():
    """Two fires in different minutes produce different idempotency keys."""
    s = FakeSchedule("sched-y", "* * * * *", "t1", {})
    scheduler, _, _, mock_creator = _make_scheduler(schedules=[s])

    now1 = datetime(2024, 6, 1, 12, 5, 0, tzinfo=timezone.utc)
    now2 = datetime(2024, 6, 1, 12, 6, 0, tzinfo=timezone.utc)  # next minute

    await scheduler._fire_one(s, now1)
    await scheduler._fire_one(s, now2)

    keys = [c.kwargs["idempotency_key"] for c in mock_creator.create_task.call_args_list]
    assert keys[0] != keys[1]


# ---------------------------------------------------------------------------
# Task template handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_task_template_dict_unpacked_as_kwargs():
    """task_template dict fields are passed as kwargs to create_task."""
    s = FakeSchedule(
        "s-tmpl", "0 * * * *", "t1",
        {"capability": "cap_x", "priority": "high", "payload": {"k": "v"}},
    )
    scheduler, _, _, mock_creator = _make_scheduler(schedules=[s])
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    await scheduler._fire_one(s, now)

    kwargs = mock_creator.create_task.call_args.kwargs
    assert kwargs.get("capability") == "cap_x"
    assert kwargs.get("priority") == "high"


@pytest.mark.asyncio
async def test_task_template_json_string_parsed():
    """task_template as JSON string is parsed into dict before unpacking."""
    import json
    s = FakeSchedule(
        "s-json", "0 * * * *", "t1",
        json.dumps({"capability": "cap_json", "meta": "data"}),
    )
    scheduler, _, _, mock_creator = _make_scheduler(schedules=[s])
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    await scheduler._fire_one(s, now)

    kwargs = mock_creator.create_task.call_args.kwargs
    assert kwargs.get("capability") == "cap_json"


@pytest.mark.asyncio
async def test_malformed_task_template_falls_back_to_empty():
    """Malformed JSON string doesn't crash; fires with empty template."""
    s = FakeSchedule("s-bad", "0 * * * *", "t1", "{NOT_VALID_JSON")
    scheduler, _, mock_repo, mock_creator = _make_scheduler(schedules=[s])
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    await scheduler._fire_one(s, now)

    # create_task still called (with empty template kwargs)
    mock_creator.create_task.assert_awaited_once()


@pytest.mark.asyncio
async def test_empty_task_template_fires_with_only_tenant_and_idem():
    """Empty template still fires; kwargs contain at least tenant_id + idempotency_key."""
    s = FakeSchedule("s-empty", "0 * * * *", "t1", {})
    scheduler, _, _, mock_creator = _make_scheduler(schedules=[s])
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    await scheduler._fire_one(s, now)

    kwargs = mock_creator.create_task.call_args.kwargs
    assert "tenant_id" in kwargs
    assert "idempotency_key" in kwargs


# ---------------------------------------------------------------------------
# Full loop integration (short run)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_full_loop_fires_due_schedules():
    """Start scheduler with short poll_interval; verify create_task is called."""
    s = FakeSchedule("loop-s1", "* * * * *", "t1", {"capability": "cap_a"})
    scheduler, fake_redis, mock_repo, mock_creator = _make_scheduler(
        schedules=[s],
        poll_interval=0.02,
    )

    task = asyncio.create_task(scheduler.start())
    await asyncio.sleep(0.08)
    await scheduler.stop()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass

    assert mock_creator.create_task.await_count >= 1


@pytest.mark.asyncio
async def test_full_loop_does_not_fire_if_not_leader():
    """Non-leader instance never fires, even with due schedules."""
    s = FakeSchedule("loop-s2", "* * * * *", "t1", {"capability": "cap_a"})
    scheduler, fake_redis, mock_repo, mock_creator = _make_scheduler(
        schedules=[s],
        instance_id="me",
        poll_interval=0.02,
    )
    # Someone else is the leader
    await fake_redis.set(_LEADER_KEY, "not-me")

    task = asyncio.create_task(scheduler.start())
    await asyncio.sleep(0.08)
    await scheduler.stop()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass

    mock_creator.create_task.assert_not_called()
