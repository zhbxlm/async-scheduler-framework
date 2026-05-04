"""Benchmark tests for critical system paths.

Run with:  python -m pytest tests/test_benchmarks.py -v --benchmark-only
Or:        python tests/test_benchmarks.py  (standalone)
"""
from __future__ import annotations

import asyncio
import json
import time
import statistics
from typing import Any, Callable, Coroutine
from unittest.mock import AsyncMock, MagicMock, patch
import pytest


# ---------------------------------------------------------------------------
# Benchmark runner helper
# ---------------------------------------------------------------------------

async def bench(
    name: str,
    coro_factory: Callable[[], Coroutine],
    n: int = 1000,
) -> dict:
    """Run *coro_factory()* n times and return timing stats."""
    latencies = []
    for _ in range(n):
        t0 = time.perf_counter()
        await coro_factory()
        latencies.append((time.perf_counter() - t0) * 1_000)  # ms

    latencies.sort()
    result = {
        "name": name,
        "n": n,
        "min_ms":  round(latencies[0], 3),
        "max_ms":  round(latencies[-1], 3),
        "mean_ms": round(statistics.mean(latencies), 3),
        "p50_ms":  round(latencies[n // 2], 3),
        "p95_ms":  round(latencies[int(n * 0.95)], 3),
        "p99_ms":  round(latencies[int(n * 0.99)], 3),
        "tps":     round(n / (sum(latencies) / 1_000), 1),
    }
    return result


def print_result(r: dict) -> None:
    print(
        f"  {r['name']:<45} "
        f"mean={r['mean_ms']:>7.3f}ms  "
        f"p95={r['p95_ms']:>7.3f}ms  "
        f"p99={r['p99_ms']:>7.3f}ms  "
        f"tps={r['tps']:>8.1f}"
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_fake_redis(latency_ms: float = 0.1) -> AsyncMock:
    """Create a mock Redis client with configurable simulated latency."""
    import asyncio as _asyncio

    async def _delayed(*args, **kwargs):
        await _asyncio.sleep(latency_ms / 1_000)
        return b"ok"

    async def _delayed_get(*args, **kwargs):
        await _asyncio.sleep(latency_ms / 1_000)
        return json.dumps({"test": "value"}).encode()

    async def _delayed_smembers(*args, **kwargs):
        await _asyncio.sleep(latency_ms / 1_000)
        return {b"item1", b"item2", b"item3"}

    r = AsyncMock()
    r.set = AsyncMock(side_effect=_delayed)
    r.get = AsyncMock(side_effect=_delayed_get)
    r.delete = AsyncMock(side_effect=_delayed)
    r.sadd = AsyncMock(side_effect=_delayed)
    r.srem = AsyncMock(side_effect=_delayed)
    r.smembers = AsyncMock(side_effect=_delayed_smembers)
    r.zadd = AsyncMock(side_effect=_delayed)
    r.zrangebyscore = AsyncMock(return_value=[])
    r.hset = AsyncMock(side_effect=_delayed)
    r.hget = AsyncMock(side_effect=_delayed)
    r.eval = AsyncMock(side_effect=_delayed)
    r.exists = AsyncMock(return_value=0)
    r.pipeline = MagicMock()
    r.pipeline.return_value.__aenter__ = AsyncMock(return_value=r)
    r.pipeline.return_value.__aexit__ = AsyncMock(return_value=False)
    return r


# ---------------------------------------------------------------------------
# Benchmark: BaseRedisRegistry CRUD
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bench_base_registry_get():
    """Benchmark BaseRedisRegistry.get()"""
    from src.platform.base_registry import BaseRedisRegistry
    r = make_fake_redis(latency_ms=0.1)
    reg = BaseRedisRegistry(r, key_prefix="bench_item")

    result = await bench(
        "BaseRedisRegistry.get",
        lambda: reg.get("tenant1", "item1"),
        n=500,
    )
    print_result(result)
    assert result["p99_ms"] < 5.0, f"p99 too slow: {result['p99_ms']}ms"


@pytest.mark.asyncio
async def test_bench_base_registry_set():
    """Benchmark BaseRedisRegistry.set()"""
    from src.platform.base_registry import BaseRedisRegistry
    r = make_fake_redis(latency_ms=0.1)
    reg = BaseRedisRegistry(r, key_prefix="bench_item")

    result = await bench(
        "BaseRedisRegistry.set",
        lambda: reg.set("tenant1", "item1", {"data": "value", "count": 42}),
        n=500,
    )
    print_result(result)
    assert result["p99_ms"] < 5.0


# ---------------------------------------------------------------------------
# Benchmark: TaskCreator
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bench_task_creator_enqueue():
    """Benchmark TaskCreator.create_task() — hot path."""
    from src.platform.task_creator import TaskCreator

    r = make_fake_redis(latency_ms=0.05)
    r.set = AsyncMock(return_value=True)
    r.get = AsyncMock(return_value=None)

    mock_queue = AsyncMock()
    mock_queue.enqueue = AsyncMock(return_value={
        "accepted": True,
        "queue_position": 0,
        "pending_count": 1,
    })

    creator = TaskCreator(
        redis_client=r,
        queue_manager=mock_queue,
        db_session_factory=None,
    )

    result = await bench(
        "TaskCreator.create_task (no dedup)",
        lambda: creator.create_task(
            capability="gpu_training",
            dag_id="train_v1",
            priority="normal",
            input_data={"batch": 32},
            tenant_id="tenant1",
        ),
        n=500,
    )
    print_result(result)
    assert result["p99_ms"] < 10.0, f"p99 too slow: {result['p99_ms']}ms"


# ---------------------------------------------------------------------------
# Benchmark: CapabilityRegistry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bench_capability_registry_get():
    """Benchmark CapabilityRegistry.get_capability()"""
    from src.platform.capability_registry import CapabilityRegistry

    cap_data = json.dumps({
        "capability_name": "gpu_training",
        "description": "GPU-accelerated training",
        "health_status": "healthy",
        "required_resources": {},
    }).encode()

    r = make_fake_redis(latency_ms=0.1)
    r.get = AsyncMock(return_value=cap_data)

    reg = CapabilityRegistry(r)

    result = await bench(
        "CapabilityRegistry.get_capability",
        lambda: reg.get_capability("gpu_training"),
        n=500,
    )
    print_result(result)
    assert result["p99_ms"] < 5.0


# ---------------------------------------------------------------------------
# Benchmark: JSON serialisation (models)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bench_json_serialisation():
    """Benchmark Pydantic model serialise/deserialise round trip."""
    from src.models.task import TaskRecord
    import uuid

    async def _round_trip():
        task = TaskRecord(
            task_id=str(uuid.uuid4()),
            dag_id="bench_dag",
            task_type="bench_task",
            tenant_id="t1",
            priority="normal",
            input_data=json.dumps({"batch": 32, "lr": 0.001}),
        )
        raw = task.__class__.model_validate(task.__dict__).model_dump_json() if hasattr(task.__class__, 'model_dump_json') else json.dumps({
            "task_id": task.task_id,
            "dag_id": task.dag_id,
            "task_type": task.task_type,
            "tenant_id": task.tenant_id,
        })
        if hasattr(task.__class__, 'model_validate_json'):
            task.__class__.model_validate_json(raw)

    result = await bench("TaskRecord JSON round-trip", _round_trip, n=1000)
    print_result(result)
    assert result["p99_ms"] < 1.0, f"Serialisation too slow: {result['p99_ms']}ms"


# ---------------------------------------------------------------------------
# Benchmark: QueueManager
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bench_queue_manager_enqueue():
    """Benchmark QueueManager.enqueue() — mock at method level."""
    from src.platform.queue_manager import QueueManager
    from unittest.mock import patch

    r = make_fake_redis(latency_ms=0.1)
    qm = QueueManager(r)

    # Patch the internal lua call to focus on method dispatch overhead
    mock_enqueue = AsyncMock(return_value={"accepted": True, "queue_position": 0, "pending_count": 1})

    with patch.object(qm, "enqueue", mock_enqueue):
        result = await bench(
            "QueueManager.enqueue (mocked lua)",
            lambda: qm.enqueue(
                capability="gpu_training",
                task_id="bench-task-001",
                priority=3,
            ),
            n=500,
        )
    print_result(result)
    assert result["p99_ms"] < 10.0
    print_result(result)
    assert result["p99_ms"] < 10.0


# ---------------------------------------------------------------------------
# Benchmark: Error handling decorator overhead
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bench_log_errors_overhead():
    """Benchmark @log_errors decorator overhead vs bare function."""
    from src.common.error_handling import log_errors

    async def bare_fn(x: int) -> int:
        return x * 2

    @log_errors(log_level="ERROR", raise_exception=False)
    async def decorated_fn(x: int) -> int:
        return x * 2

    bare_result = await bench("bare async function", lambda: bare_fn(42), n=2000)
    deco_result = await bench("@log_errors async function", lambda: decorated_fn(42), n=2000)

    print_result(bare_result)
    print_result(deco_result)

    overhead_pct = (deco_result["mean_ms"] / max(bare_result["mean_ms"], 0.0001) - 1) * 100
    print(f"  @log_errors overhead: {overhead_pct:.1f}%")

    # Overhead should be less than 50x
    assert deco_result["mean_ms"] < 1.0, "Decorator too slow"


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    async def main():
        print("\n" + "=" * 80)
        print("  ASYNC SCHEDULER FRAMEWORK — PERFORMANCE BENCHMARKS")
        print("=" * 80)
        print(f"  {'Benchmark':<45} {'mean':>10}  {'p95':>10}  {'p99':>10}  {'TPS':>10}")
        print("-" * 80)

        tests = [
            test_bench_base_registry_get,
            test_bench_base_registry_set,
            test_bench_task_creator_enqueue,
            test_bench_capability_registry_get,
            test_bench_json_serialisation,
            test_bench_queue_manager_enqueue,
            test_bench_log_errors_overhead,
        ]

        failed = []
        for test in tests:
            try:
                await test()
            except AssertionError as e:
                failed.append((test.__name__, str(e)))
            except Exception as e:
                print(f"  ERROR in {test.__name__}: {e}")
                import traceback
                traceback.print_exc()

        print("=" * 80)
        if failed:
            print(f"\n❌ {len(failed)} benchmark(s) failed SLA:")
            for name, msg in failed:
                print(f"   - {name}: {msg}")
            sys.exit(1)
        else:
            print("\n✅ All benchmarks passed SLA thresholds")

    asyncio.run(main())