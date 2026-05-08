from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class _FakeRedis:
    def register_script(self, script):
        return MagicMock()


def _settings(mysql_url: str | None = None):
    return SimpleNamespace(
        redis=SimpleNamespace(url="redis://localhost:6379/0"),
        mysql=SimpleNamespace(url=mysql_url),
        background=SimpleNamespace(
            compensation_interval=60,
            reconcile=SimpleNamespace(
                enabled=True,
                interval_seconds=30,
                stuck_max_per_tick=10,
                stuck_task_max_age_seconds=300,
                batch_size=50,
            ),
            cron=SimpleNamespace(enabled=True, poll_interval=15),
        ),
    )


@pytest.mark.asyncio
async def test_build_ops_api_sets_common_infra_and_role_specific_services():
    from src.platform.container import ServiceContainer

    with patch("src.common.redis_client.create_redis_client", new=AsyncMock(return_value=_FakeRedis())):
        container = await ServiceContainer.build_ops_api(_settings())

    assert container.lifecycle_manager is not None
    assert container.redis_client is not None
    assert container.circuit_breaker is not None
    assert container.queue_manager is not None
    assert container.capability_registry is not None
    assert container.cluster_registry is not None
    assert container.node_registry is not None
    assert container.schedule_registry is not None
    assert container.tenant_registry is not None
    assert container.dag_loader is not None


@pytest.mark.asyncio
async def test_build_task_api_without_mysql_skips_async_db_but_keeps_queue_stack():
    from src.platform.container import ServiceContainer

    with patch("src.common.redis_client.create_redis_client", new=AsyncMock(return_value=_FakeRedis())):
        container = await ServiceContainer.build_task_api(_settings(mysql_url=None))

    assert container.lifecycle_manager is not None
    assert container.redis_client is not None
    assert container.circuit_breaker is not None
    assert container.queue_manager is not None
    assert container.async_engine is None
    assert container.async_session_factory is None
    assert container.task_creator is not None
    assert container.task_completion_node is not None
    assert container.schedule_registry is not None
    assert container.tenant_registry is not None


@pytest.mark.asyncio
async def test_build_control_plane_with_mysql_sets_background_services():
    from src.platform.container import ServiceContainer

    fake_engine = object()
    fake_session_factory = object()

    with patch("src.common.redis_client.create_redis_client", new=AsyncMock(return_value=_FakeRedis())), patch(
        "src.common.async_db.init_async_engine", return_value=(fake_engine, fake_session_factory)
    ):
        container = await ServiceContainer.build_control_plane(_settings(mysql_url="mysql+aiomysql://user:pass@host/db"))

    assert container.lifecycle_manager is not None
    assert container.redis_client is not None
    assert container.async_engine is fake_engine
    assert container.async_session_factory is fake_session_factory
    assert container.circuit_breaker is not None
    assert container.queue_manager is not None
    assert container.schedule_registry is not None
    assert container.task_creator is not None
    assert container.task_completion_node is not None
    assert container.task_reconciler is not None
    assert container.task_completion_node is not None
    assert container.compensation_service is not None
    assert container.callback_dispatcher is not None
    assert container.cron_scheduler is not None
