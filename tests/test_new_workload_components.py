"""Tests for SchedulerActor, ActorPoolManager, BaseWorkerActor, queue_keys, Settings."""
from __future__ import annotations

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock


# ===========================================================================
# queue_keys
# ===========================================================================

def test_queue_keys_format():
    from src.platform.queue_keys import pending, running, stats, config, slot_running, slot_config
    assert pending("inference") == "{queue:inference}:pending"
    assert running("inference") == "{queue:inference}:running"
    assert stats("inference") == "{queue:inference}:stats"
    assert config("inference") == "{queue:inference}:config"
    assert slot_running("inference") == "{slot:inference}:running"
    assert slot_config("inference") == "{slot:inference}:config"

def test_queue_keys_different_caps():
    from src.platform.queue_keys import pending
    assert pending("ml") != pending("data")
    assert "{queue:ml}" in pending("ml")


# ===========================================================================
# Settings
# ===========================================================================

def test_settings_singleton():
    from config.settings import settings, Settings
    assert isinstance(settings, Settings)

def test_settings_redis_url():
    from config.settings import settings
    url = settings.redis.url
    assert url.startswith("redis://")

def test_settings_scaling_defaults():
    from config.settings import settings
    assert 0 < settings.scaling.up_threshold < 1
    assert 0 < settings.scaling.down_threshold < 1
    assert settings.scaling.cooldown_seconds > 0

def test_settings_task_config():
    from config.settings import settings
    assert settings.task.default_timeout_seconds == 3600
    assert settings.task.default_max_retries == 3

def test_settings_raydata_config():
    from config.settings import settings
    assert settings.task.raydata.submit_path == "/api/jobs/"
    assert settings.task.raydata.submit_timeout_seconds == 15

def test_settings_agent_config():
    from config.settings import settings
    assert settings.agent.heartbeat_interval == 10.0
    assert settings.agent.owner_ttl_seconds == 30


# ===========================================================================
# BaseWorkerActor
# ===========================================================================

def test_base_worker_run_calls_hooks():
    from src.workload.base_worker_actor import _BaseWorkerActorImpl

    class EchoWorker(_BaseWorkerActorImpl):
        def call(self, data):
            return {"echo": data.get("msg")}

    w = EchoWorker("echo")
    result = w.run({"msg": "hello"})
    assert result == {"echo": "hello"}
    assert w._call_count == 1

def test_base_worker_health_check():
    from src.workload.base_worker_actor import _BaseWorkerActorImpl

    class W(_BaseWorkerActorImpl):
        def call(self, data):
            return data

    w = W("test")
    h = w.health_check()
    assert h["status"] == "healthy"
    assert h["capability"] == "test"

def test_base_worker_call_not_implemented():
    from src.workload.base_worker_actor import _BaseWorkerActorImpl
    w = _BaseWorkerActorImpl("x")
    with pytest.raises(NotImplementedError):
        w.run({})

def test_base_worker_pre_post_process():
    from src.workload.base_worker_actor import _BaseWorkerActorImpl

    class W(_BaseWorkerActorImpl):
        def pre_process(self, data):
            data["pre"] = True
            return data
        def call(self, data):
            return data
        def post_process(self, result):
            result["post"] = True
            return result

    w = W("p")
    r = w.run({"x": 1})
    assert r["pre"] is True
    assert r["post"] is True


# ===========================================================================
# ActorPoolManager (no-Ray mode)
# ===========================================================================

@pytest.mark.asyncio
async def test_actor_pool_manager_start_and_status():
    from src.workload.actor_pool_manager import ActorPoolManager

    class FakeWorker:
        async def execute(self, *, task_id, step_name, payload, timeout_seconds=60):
            return {"output": payload.get("x", 0) * 2}

    async def factory(name, idx):
        return FakeWorker()

    apm = ActorPoolManager(
        capability="math",
        cluster_id="c1",
        tenant_id="t1",
        target_size=2,
        worker_factory=factory,
    )
    await apm.start()
    status = await apm.get_status()
    assert status["pool_size"] == 2
    assert status["capability"] == "math"

@pytest.mark.asyncio
async def test_actor_pool_manager_execute():
    from src.workload.actor_pool_manager import ActorPoolManager

    class FakeWorker:
        async def execute(self, *, task_id, step_name, payload, timeout_seconds=60):
            return {"result": "ok", "task_id": task_id}

    async def factory(name, idx):
        return FakeWorker()

    apm = ActorPoolManager(capability="echo", target_size=1, worker_factory=factory)
    await apm.start()
    result = await apm.execute(task_id="t1", step_name="s1", payload={})
    assert result["result"] == "ok"
    assert result["task_id"] == "t1"

@pytest.mark.asyncio
async def test_actor_pool_manager_resize_up():
    from src.workload.actor_pool_manager import ActorPoolManager

    async def factory(name, idx):
        return MagicMock(spec=["execute", "submit"])

    apm = ActorPoolManager(capability="x", target_size=1, max_size=4, worker_factory=factory)
    await apm.start()
    assert len(apm._workers) == 1
    await apm.resize(3)
    assert len(apm._workers) == 3

@pytest.mark.asyncio
async def test_actor_pool_manager_resize_down():
    from src.workload.actor_pool_manager import ActorPoolManager

    async def factory(name, idx):
        return MagicMock(spec=["execute"])

    apm = ActorPoolManager(capability="x", target_size=3, max_size=4, worker_factory=factory)
    await apm.start()
    assert len(apm._workers) == 3
    await apm.resize(1)
    assert len(apm._workers) == 1

@pytest.mark.asyncio
async def test_actor_pool_manager_least_loaded_selection():
    from src.workload.actor_pool_manager import ActorPoolManager, _WorkerHandle

    call_log = []

    class FakeWorker:
        def __init__(self, name):
            self.name = name
        async def execute(self, *, task_id, step_name, payload, timeout_seconds=60):
            call_log.append(self.name)
            return {}

    async def factory(name, idx):
        return FakeWorker(name)

    apm = ActorPoolManager(capability="lb", target_size=2, worker_factory=factory)
    await apm.start()

    # Simulate first worker having 1 inflight
    apm._workers[0].inflight.append("fake_ref")

    w = apm._select_worker()
    assert w is apm._workers[1]   # second should be selected (0 pending)

def test_actor_pool_worker_naming():
    from src.workload.actor_pool_manager import ActorPoolManager
    apm = ActorPoolManager(
        capability="infer",
        cluster_id="c1",
        tenant_id="t1",
        target_size=1,
    )
    name = apm._worker_name(0)
    assert name == "worker_t1_c1_infer_0"
    assert "t1" in name and "c1" in name and "infer" in name


# ===========================================================================
# SchedulerActor (no-Ray mode)
# ===========================================================================

@pytest.mark.asyncio
async def test_scheduler_actor_register_pool_and_capabilities():
    from src.workload.scheduler_actor import SchedulerActor

    sa = SchedulerActor(cluster_id="c1", release="v1")

    mock_pool = AsyncMock()
    await sa.register_pool("inference", mock_pool)
    assert "inference" in sa.get_capabilities()

@pytest.mark.asyncio
async def test_scheduler_actor_execute_sync():
    from src.workload.scheduler_actor import SchedulerActor

    sa = SchedulerActor(cluster_id="c1", release="v1")

    class FakePool:
        async def execute(self, task_id, step_name, payload):
            return {"result": "done", "task_id": task_id}

    await sa.register_pool("echo", FakePool())
    result = await sa.execute_sync("echo", "t1", "s1", {"x": 1})
    assert result["result"] == "done"

@pytest.mark.asyncio
async def test_scheduler_actor_submit_async():
    from src.workload.scheduler_actor import SchedulerActor

    sa = SchedulerActor(cluster_id="c1", release="v1")

    class FakePool:
        async def submit(self, task_id, step_name, payload):
            pass

    await sa.register_pool("echo", FakePool())
    handle = await sa.submit_async("echo", "t1", "s1", {})
    assert handle == "t1:s1"

@pytest.mark.asyncio
async def test_scheduler_actor_unknown_capability_raises():
    from src.workload.scheduler_actor import SchedulerActor
    sa = SchedulerActor(cluster_id="c1", release="v1")
    with pytest.raises(RuntimeError, match="no pool registered"):
        await sa.execute_sync("nonexistent", "t1", "s1", {})

def test_scheduler_actor_name_format():
    from src.workload.scheduler_actor import actor_name
    assert actor_name("c1", "v2") == "scheduler_c1__v2"

def test_tenant_actor_namespace():
    from src.workload.scheduler_actor import tenant_actor_namespace
    assert tenant_actor_namespace("") == "ray_async"
    assert tenant_actor_namespace("default") == "ray_async"
    assert tenant_actor_namespace("acme") == "ray_async_acme"
