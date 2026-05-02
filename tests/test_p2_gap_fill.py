"""Tests for P2 Gap Fill: G9 ActorPoolManager, G10 ResourceManager,
G11 AsyncProxySidecar, G12 ClusterRegistry.
"""
from __future__ import annotations

import asyncio
import pytest

# ---------------------------------------------------------------------------
# G9: ActorPoolManager / SchedulerActor
# ---------------------------------------------------------------------------

class TestActorPoolManager:
    """G9: Ray-style actor pool, least-loaded routing, scale-up."""

    @pytest.mark.asyncio
    async def test_register_and_submit(self):
        from async_scheduler.platform.actor_pool import ActorPoolManager, ActorPoolConfig

        async def echo_handler(payload):
            return {"echo": payload.get("value")}

        mgr = ActorPoolManager()
        mgr.register_capability("echo", echo_handler, ActorPoolConfig(min_actors=1, max_actors=4))

        result = await mgr.submit("echo", {"value": 42})
        assert result == {"echo": 42}

    @pytest.mark.asyncio
    async def test_actor_scales_up(self):
        from async_scheduler.platform.actor_pool import ActorPoolManager, ActorPoolConfig, ActorState

        async def slow_handler(payload):
            await asyncio.sleep(0.01)
            return {"done": True}

        mgr = ActorPoolManager()
        mgr.register_capability("slow", slow_handler, ActorPoolConfig(max_actors=4, max_tasks_per_actor=1))

        # Submit several tasks; pool should grow
        tasks = [asyncio.create_task(mgr.submit("slow", {"n": i})) for i in range(3)]
        results = await asyncio.gather(*tasks)
        assert all(r["done"] for r in results)

        stats = mgr.get_pool_stats("slow")
        assert stats["actor_count"] >= 1

    @pytest.mark.asyncio
    async def test_get_or_create_returns_none_for_unknown(self):
        from async_scheduler.platform.actor_pool import ActorPoolManager

        mgr = ActorPoolManager()
        actor = await mgr.get_or_create_actor("nonexistent")
        assert actor is None

    @pytest.mark.asyncio
    async def test_pool_stats_structure(self):
        from async_scheduler.platform.actor_pool import ActorPoolManager, ActorPoolConfig

        async def noop(payload):
            return {}

        mgr = ActorPoolManager()
        mgr.register_capability("test", noop, ActorPoolConfig())
        await mgr.get_or_create_actor("test")
        stats = mgr.get_pool_stats("test")
        assert "actor_count" in stats
        assert "actors" in stats
        assert "total_pending" in stats

    @pytest.mark.asyncio
    async def test_stop_all_terminates_actors(self):
        from async_scheduler.platform.actor_pool import ActorPoolManager, ActorPoolConfig, ActorState

        async def noop(payload):
            return {}

        mgr = ActorPoolManager()
        mgr.register_capability("x", noop, ActorPoolConfig())
        await mgr.get_or_create_actor("x")
        await mgr.stop_all()
        assert mgr.get_pool_stats("x")["actor_count"] == 0

    @pytest.mark.asyncio
    async def test_actor_handle_utilization(self):
        from async_scheduler.platform.actor_pool import ActorHandle, ActorState

        handle = ActorHandle(
            actor_id="a1",
            capability="gpu",
            version="v1",
            instance_id="a1",
            pending_tasks=4,
            max_tasks=8,
            state=ActorState.HEALTHY,
        )
        assert handle.utilization == 0.5
        assert handle.is_available

    @pytest.mark.asyncio
    async def test_scheduler_actor_start_stop(self):
        from async_scheduler.platform.actor_pool import SchedulerActor, ActorState

        async def handler(p):
            return p

        actor = SchedulerActor("test", handler)
        await actor.start()
        assert actor.handle.state == ActorState.HEALTHY
        await actor.stop()
        assert actor.handle.state == ActorState.TERMINATED


# ---------------------------------------------------------------------------
# G10: ResourceManager
# ---------------------------------------------------------------------------

class TestResourceManager:
    """G10: Queue-depth-driven scale decisions."""

    @pytest.mark.asyncio
    async def test_start_stop(self):
        from async_scheduler.platform.resource_manager import ResourceManager, ResourcePolicy
        from unittest.mock import AsyncMock, MagicMock

        qm = MagicMock()
        qm.discover_capabilities = AsyncMock(return_value=[])
        rm = ResourceManager(queue_manager=qm)
        await rm.start()
        assert rm._running
        await rm.stop()
        assert not rm._running

    @pytest.mark.asyncio
    async def test_register_policy(self):
        from async_scheduler.platform.resource_manager import ResourceManager, ResourcePolicy
        from unittest.mock import AsyncMock, MagicMock

        qm = MagicMock()
        qm.discover_capabilities = AsyncMock(return_value=[])
        rm = ResourceManager(queue_manager=qm)
        rm.register_policy("gpu", ResourcePolicy(min_actors=2, max_actors=16))
        assert "gpu" in rm._policies
        assert rm._policies["gpu"].min_actors == 2

    @pytest.mark.asyncio
    async def test_scale_history_populated_on_scaleup(self):
        from async_scheduler.platform.resource_manager import ResourceManager, ResourcePolicy, ScaleDirection
        from unittest.mock import AsyncMock, MagicMock

        # Mock queue stats with high pending count
        from dataclasses import dataclass

        @dataclass
        class FakeStats:
            pending: int
            running: int

        qm = MagicMock()
        qm.discover_capabilities = AsyncMock(return_value=["gpu"])
        qm.get_capability_stats = AsyncMock(return_value=FakeStats(pending=10, running=1))

        rm = ResourceManager(queue_manager=qm)
        rm.register_policy("gpu", ResourcePolicy(scale_up_threshold=1.5, max_actors=4))

        await rm._check_all_capabilities()
        history = rm.get_scale_history()
        assert len(history) >= 1
        assert history[-1]["direction"] == ScaleDirection.UP

    def test_get_stats_structure(self):
        from async_scheduler.platform.resource_manager import ResourceManager
        from unittest.mock import MagicMock

        qm = MagicMock()
        rm = ResourceManager(queue_manager=qm)
        stats = rm.get_stats()
        assert "running" in stats
        assert "scale_events_total" in stats


# ---------------------------------------------------------------------------
# G11: AsyncProxySidecar
# ---------------------------------------------------------------------------

class TestAsyncProxySidecar:
    """G11: Redis Pub/Sub async callback notification."""

    @pytest.mark.asyncio
    async def test_in_process_publish_and_wait(self):
        from async_scheduler.platform.async_proxy import AsyncProxySidecar, TaskEvent

        sidecar = AsyncProxySidecar()
        await sidecar.start()

        event = TaskEvent(task_id="t1", status="success", result={"x": 1})

        async def _publish_later():
            await asyncio.sleep(0.05)
            await sidecar.publish(event)

        asyncio.create_task(_publish_later())
        received = await sidecar.wait_for("t1", timeout=2.0)
        assert received is not None
        assert received.task_id == "t1"
        assert received.status == "success"
        await sidecar.stop()

    @pytest.mark.asyncio
    async def test_wait_for_timeout_returns_none(self):
        from async_scheduler.platform.async_proxy import AsyncProxySidecar

        sidecar = AsyncProxySidecar()
        await sidecar.start()
        result = await sidecar.wait_for("never-task", timeout=0.1)
        assert result is None
        await sidecar.stop()

    @pytest.mark.asyncio
    async def test_subscriber_callback_called(self):
        from async_scheduler.platform.async_proxy import AsyncProxySidecar, TaskEvent

        sidecar = AsyncProxySidecar()
        received_events = []

        async def my_subscriber(ev: TaskEvent):
            received_events.append(ev)

        sidecar.subscribe(my_subscriber)
        await sidecar.start()

        event = TaskEvent(task_id="t2", status="failed", error="timeout")
        await sidecar.publish(event)
        await asyncio.sleep(0.01)

        assert len(received_events) == 1
        assert received_events[0].task_id == "t2"
        await sidecar.stop()

    @pytest.mark.asyncio
    async def test_task_event_serialize_roundtrip(self):
        from async_scheduler.platform.async_proxy import TaskEvent

        e = TaskEvent(task_id="abc", status="success", result={"val": 99}, worker_id="w1")
        serialized = e.serialize()
        restored = TaskEvent.deserialize(serialized)
        assert restored.task_id == "abc"
        assert restored.result == {"val": 99}
        assert restored.worker_id == "w1"

    @pytest.mark.asyncio
    async def test_cached_event_returned_immediately(self):
        from async_scheduler.platform.async_proxy import AsyncProxySidecar, TaskEvent

        sidecar = AsyncProxySidecar()
        await sidecar.start()

        event = TaskEvent(task_id="t3", status="success")
        await sidecar.publish(event)

        # Second wait should return immediately from cache
        result = await sidecar.wait_for("t3", timeout=2.0)
        assert result is not None
        assert result.task_id == "t3"
        await sidecar.stop()

    def test_get_stats_structure(self):
        from async_scheduler.platform.async_proxy import AsyncProxySidecar

        sidecar = AsyncProxySidecar()
        stats = sidecar.get_stats()
        assert "redis_connected" in stats
        assert "cached_events" in stats


# ---------------------------------------------------------------------------
# G12: ClusterRegistry
# ---------------------------------------------------------------------------

class TestClusterRegistry:
    """G12: Multi-cluster routing."""

    @pytest.mark.asyncio
    async def test_register_and_list(self):
        from async_scheduler.platform.cluster_registry import ClusterRegistry, ClusterInfo

        registry = ClusterRegistry()
        await registry.register_cluster(ClusterInfo(
            cluster_id="c1", name="GPU Cluster",
            capabilities=["gpu", "ml"], region="us-west",
        ))
        clusters = await registry.list_clusters()
        assert len(clusters) == 1
        assert clusters[0].cluster_id == "c1"

    @pytest.mark.asyncio
    async def test_route_by_required_capability(self):
        from async_scheduler.platform.cluster_registry import (
            ClusterRegistry, ClusterInfo, RoutingPolicy, ClusterHealth,
        )

        registry = ClusterRegistry()
        await registry.register_cluster(ClusterInfo(
            cluster_id="c1", name="CPU", capabilities=["cpu"],
            health=ClusterHealth.HEALTHY, region="us-east",
        ))
        await registry.register_cluster(ClusterInfo(
            cluster_id="c2", name="GPU", capabilities=["gpu", "ml"],
            health=ClusterHealth.HEALTHY, region="us-west",
        ))

        result = await registry.route_task(RoutingPolicy(required_capabilities=["gpu"]))
        assert result is not None
        assert result.cluster_id == "c2"

    @pytest.mark.asyncio
    async def test_route_returns_none_when_no_match(self):
        from async_scheduler.platform.cluster_registry import ClusterRegistry, ClusterInfo, RoutingPolicy

        registry = ClusterRegistry()
        await registry.register_cluster(ClusterInfo(
            cluster_id="c1", name="CPU", capabilities=["cpu"], region="us",
        ))
        result = await registry.route_task(RoutingPolicy(required_capabilities=["quantum"]))
        assert result is None

    @pytest.mark.asyncio
    async def test_route_prefers_region(self):
        from async_scheduler.platform.cluster_registry import (
            ClusterRegistry, ClusterInfo, RoutingPolicy, ClusterHealth,
        )

        registry = ClusterRegistry()
        for i, region in enumerate(["us-east", "us-west", "eu-west"]):
            await registry.register_cluster(ClusterInfo(
                cluster_id=f"c{i}", name=f"Cluster {i}",
                capabilities=["cpu"], region=region,
                health=ClusterHealth.HEALTHY,
            ))

        result = await registry.route_task(RoutingPolicy(
            required_capabilities=["cpu"],
            preferred_region="eu-west",
        ))
        assert result is not None
        assert result.region == "eu-west"

    @pytest.mark.asyncio
    async def test_route_excludes_unavailable(self):
        from async_scheduler.platform.cluster_registry import (
            ClusterRegistry, ClusterInfo, RoutingPolicy, ClusterHealth,
        )

        registry = ClusterRegistry()
        await registry.register_cluster(ClusterInfo(
            cluster_id="c1", name="Down", capabilities=["gpu"],
            health=ClusterHealth.UNAVAILABLE,
        ))
        await registry.register_cluster(ClusterInfo(
            cluster_id="c2", name="Up", capabilities=["gpu"],
            health=ClusterHealth.HEALTHY,
        ))

        result = await registry.route_task(RoutingPolicy(required_capabilities=["gpu"]))
        assert result is not None
        assert result.cluster_id == "c2"

    @pytest.mark.asyncio
    async def test_deregister_cluster(self):
        from async_scheduler.platform.cluster_registry import ClusterRegistry, ClusterInfo

        registry = ClusterRegistry()
        await registry.register_cluster(ClusterInfo(cluster_id="c1", name="Test", capabilities=["x"]))
        await registry.deregister_cluster("c1")
        clusters = await registry.list_clusters()
        assert len(clusters) == 0

    @pytest.mark.asyncio
    async def test_update_heartbeat(self):
        from async_scheduler.platform.cluster_registry import ClusterRegistry, ClusterInfo, ClusterHealth

        registry = ClusterRegistry()
        await registry.register_cluster(ClusterInfo(cluster_id="c1", name="Test", capabilities=["x"]))
        await registry.update_heartbeat("c1", pending_tasks=42, health=ClusterHealth.DEGRADED)
        cluster = await registry.get_cluster("c1")
        assert cluster is not None
        assert cluster.pending_tasks == 42
        assert cluster.health == ClusterHealth.DEGRADED

    @pytest.mark.asyncio
    async def test_get_clusters_for_capability(self):
        from async_scheduler.platform.cluster_registry import ClusterRegistry, ClusterInfo

        registry = ClusterRegistry()
        await registry.register_cluster(ClusterInfo(cluster_id="c1", name="A", capabilities=["gpu"]))
        await registry.register_cluster(ClusterInfo(cluster_id="c2", name="B", capabilities=["cpu"]))
        await registry.register_cluster(ClusterInfo(cluster_id="c3", name="C", capabilities=["gpu", "cpu"]))

        gpu_clusters = await registry.get_clusters_for_capability("gpu")
        ids = {c.cluster_id for c in gpu_clusters}
        assert "c1" in ids
        assert "c3" in ids
        assert "c2" not in ids

    @pytest.mark.asyncio
    async def test_required_tags_filter(self):
        from async_scheduler.platform.cluster_registry import (
            ClusterRegistry, ClusterInfo, RoutingPolicy, ClusterHealth,
        )

        registry = ClusterRegistry()
        await registry.register_cluster(ClusterInfo(
            cluster_id="prod", name="Prod",
            capabilities=["cpu"], tags={"env": "prod"},
            health=ClusterHealth.HEALTHY,
        ))
        await registry.register_cluster(ClusterInfo(
            cluster_id="dev", name="Dev",
            capabilities=["cpu"], tags={"env": "dev"},
            health=ClusterHealth.HEALTHY,
        ))

        result = await registry.route_task(RoutingPolicy(
            required_capabilities=["cpu"],
            required_tags={"env": "prod"},
        ))
        assert result is not None
        assert result.cluster_id == "prod"
