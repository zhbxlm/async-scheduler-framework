"""Tests for StepExecutors — SyncStepExecutor, MapStepExecutor.

Covers:
SyncStepExecutor:
- execute(): success on first attempt
- execute(): retries on transient failure, succeeds on Nth attempt
- execute(): raises after all retries exhausted
- execute(): calls qm.try_recover_concurrent on success
- execute(): calls qm.adjust_concurrent(-1) on failure
- execute(): no qm → still works

MapStepExecutor:
- execute(): dispatches each shard, returns list of results
- execute(): empty shards → returns []
- execute(): one shard fails → raises RuntimeError listing errors
- execute(): respects max_concurrency from step object
- execute(): uses self._max_parallelism when step has no max_concurrency
- execute(): partial failure with CONTINUE policy is not tested here
  (MapStepExecutor itself doesn't implement error_policy; that's DagEngine's job)
"""
from __future__ import annotations

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock

from src.platform.step_executors import SyncStepExecutor, MapStepExecutor


# ---------------------------------------------------------------------------
# SyncStepExecutor
# ---------------------------------------------------------------------------

def _make_sync(*, max_retries: int = 0, retry_delay: float = 0.0, with_qm: bool = False):
    qm = AsyncMock() if with_qm else None
    if qm:
        qm.try_recover_concurrent = AsyncMock()
        qm.adjust_concurrent = AsyncMock()
    executor = SyncStepExecutor(queue_manager=qm, max_retries=max_retries, retry_delay_seconds=retry_delay)
    return executor, qm


@pytest.mark.asyncio
async def test_sync_success_first_attempt():
    executor, _ = _make_sync()

    async def dispatch(cap, step, data):
        return {"result": "ok"}

    result = await executor.execute("cap_a", "step1", {}, dispatch)
    assert result == {"result": "ok"}


@pytest.mark.asyncio
async def test_sync_retries_on_transient_failure():
    executor, _ = _make_sync(max_retries=2, retry_delay=0.0)
    call_count = 0

    async def flaky_dispatch(cap, step, data):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ConnectionError("transient")
        return {"ok": True}

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr("asyncio.sleep", AsyncMock())
        result = await executor.execute("cap_a", "step1", {}, flaky_dispatch)

    assert result == {"ok": True}
    assert call_count == 3


@pytest.mark.asyncio
async def test_sync_raises_after_all_retries():
    executor, _ = _make_sync(max_retries=2, retry_delay=0.0)

    async def always_fail(cap, step, data):
        raise RuntimeError("always fails")

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr("asyncio.sleep", AsyncMock())
        with pytest.raises(RuntimeError, match="failed after 3 attempts"):
            await executor.execute("cap_a", "step1", {}, always_fail)


@pytest.mark.asyncio
async def test_sync_calls_recover_on_success():
    executor, qm = _make_sync(with_qm=True)

    async def ok_dispatch(cap, step, data):
        return {}

    await executor.execute("cap_a", "step1", {}, ok_dispatch)
    qm.try_recover_concurrent.assert_awaited_once_with("cap_a")


@pytest.mark.asyncio
async def test_sync_calls_adjust_on_failure():
    executor, qm = _make_sync(max_retries=1, retry_delay=0.0, with_qm=True)
    call_count = 0

    async def fail_then_ok(cap, step, data):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("first fail")
        return {}

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr("asyncio.sleep", AsyncMock())
        await executor.execute("cap_a", "step1", {}, fail_then_ok)

    qm.adjust_concurrent.assert_awaited_once_with("cap_a", -1)


@pytest.mark.asyncio
async def test_sync_no_qm_still_works():
    executor, _ = _make_sync(with_qm=False)

    async def ok_dispatch(cap, step, data):
        return {"done": True}

    result = await executor.execute("cap_a", "step1", {}, ok_dispatch)
    assert result == {"done": True}


# ---------------------------------------------------------------------------
# MapStepExecutor
# ---------------------------------------------------------------------------

def _make_map(*, max_parallelism: int = 16):
    return MapStepExecutor(max_parallelism=max_parallelism)


@pytest.mark.asyncio
async def test_map_dispatches_each_shard():
    executor = _make_map()
    dispatched = []

    async def dispatch(cap, step_name, data):
        dispatched.append(data["shard"])
        return {"processed": data["shard"]}

    shards = ["a", "b", "c"]
    results = await executor.execute("cap_a", "map_step", {}, dispatch, shards=shards)

    assert sorted(dispatched) == ["a", "b", "c"]
    assert len(results) == 3


@pytest.mark.asyncio
async def test_map_returns_empty_for_no_shards():
    executor = _make_map()

    async def dispatch(cap, step, data):
        return {}

    results = await executor.execute("cap_a", "step", {}, dispatch, shards=[])
    assert results == []


@pytest.mark.asyncio
async def test_map_returns_empty_for_none_shards():
    executor = _make_map()

    async def dispatch(cap, step, data):
        return {}

    results = await executor.execute("cap_a", "step", {}, dispatch, shards=None)
    assert results == []


@pytest.mark.asyncio
async def test_map_raises_on_shard_failure():
    executor = _make_map()

    async def dispatch(cap, step, data):
        if data["shard"] == "bad":
            raise RuntimeError("bad shard")
        return {"ok": True}

    with pytest.raises(RuntimeError, match="shards failed"):
        await executor.execute("cap_a", "step", {}, dispatch, shards=["good", "bad"])


@pytest.mark.asyncio
async def test_map_respects_step_max_concurrency():
    """At most max_concurrency shards should run simultaneously."""
    executor = _make_map(max_parallelism=100)

    active = 0
    peak = 0

    async def slow_dispatch(cap, step, data):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.02)
        active -= 1
        return {}

    # step with max_concurrency=2
    step = MagicMock()
    step.max_concurrency = 2

    shards = list(range(6))
    await executor.execute("cap_a", "step", {}, slow_dispatch, shards=shards, step=step)

    assert peak <= 2


@pytest.mark.asyncio
async def test_map_uses_default_parallelism_without_step():
    executor = _make_map(max_parallelism=4)
    active = 0
    peak = 0

    async def slow_dispatch(cap, step, data):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return {}

    shards = list(range(10))
    await executor.execute("cap_a", "step", {}, slow_dispatch, shards=shards)
    assert peak <= 4


@pytest.mark.asyncio
async def test_map_passes_shard_index():
    executor = _make_map()
    indices = []

    async def dispatch(cap, step, data):
        indices.append(data["shard_index"])
        return {}

    await executor.execute("cap_a", "step", {}, dispatch, shards=["x", "y", "z"])
    assert sorted(indices) == [0, 1, 2]


@pytest.mark.asyncio
async def test_map_merges_input_data_with_shard():
    executor = _make_map()
    received = []

    async def dispatch(cap, step, data):
        received.append(dict(data))
        return {}

    await executor.execute("cap_a", "step", {"base_key": "base_val"}, dispatch, shards=["s1"])
    assert received[0]["base_key"] == "base_val"
    assert received[0]["shard"] == "s1"
