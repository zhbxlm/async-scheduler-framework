from __future__ import annotations

import importlib

import pytest


@pytest.mark.asyncio
async def test_build_service_container_uses_default_settings(monkeypatch) -> None:
    import async_scheduler.platform.services as services_mod
    from async_scheduler.backends import BackendConfig

    class _FakeSettings:
        backends = BackendConfig()

    monkeypatch.setattr(services_mod, "get_settings", lambda: _FakeSettings())

    container = await services_mod.build_service_container()

    assert container.queue_manager is not None
    assert container.task_consumer is not None
    assert container.cron_scheduler is not None
    assert container.distributed_settings is None


@pytest.mark.asyncio
async def test_build_service_container_uses_redis_settings_when_configured(monkeypatch) -> None:
    import async_scheduler.platform.services as services_mod
    from async_scheduler.backends import BackendConfig

    class _FakeSettings:
        backends = BackendConfig(
            queue_type="redis",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
            lease_ttl_seconds=9.0,
            heartbeat_interval_seconds=3.0,
        )

    monkeypatch.setattr(services_mod, "get_settings", lambda: _FakeSettings())

    container = await services_mod.build_service_container()

    assert container.distributed_settings is not None
    assert container.distributed_settings.redis_url == "redis://localhost:6379/0"
    assert container.distributed_settings.lease_ttl_seconds == 9.0
    assert container.distributed_settings.heartbeat_interval_seconds == 3.0
