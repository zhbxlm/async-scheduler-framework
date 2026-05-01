from __future__ import annotations

import pytest

from async_scheduler.backends import BackendConfig
from async_scheduler.platform.services import build_service_container


class TestBackendConfig:
    def test_default_backend_config_uses_memory_mode(self) -> None:
        config = BackendConfig.default()

        assert config.queue_type == "memory"
        assert config.lock_type == "memory"
        assert config.registry_type == "memory"
        assert config.redis_url is None
        assert config.lease_ttl_seconds == 30.0
        assert config.heartbeat_interval_seconds == 10.0
        assert config.distributed_mode is False

    def test_redis_backend_config_enables_distributed_mode(self) -> None:
        config = BackendConfig(
            queue_type="redis",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
            lease_ttl_seconds=45.0,
            heartbeat_interval_seconds=15.0,
        )

        assert config.redis_url == "redis://localhost:6379/0"
        assert config.lease_ttl_seconds == 45.0
        assert config.heartbeat_interval_seconds == 15.0
        assert config.distributed_mode is True

    def test_redis_backend_requires_redis_url(self) -> None:
        with pytest.raises(ValueError, match="redis_url"):
            BackendConfig(queue_type="redis")


class TestServiceContainerDistributedConfig:
    @pytest.mark.asyncio
    async def test_build_service_container_with_memory_backend_still_works(self) -> None:
        services = await build_service_container()

        assert services.queue_manager is not None
        assert services.distributed_settings is None

    @pytest.mark.asyncio
    async def test_build_service_container_with_redis_config_exposes_distributed_settings(self) -> None:
        config = BackendConfig(
            queue_type="redis",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
            lease_ttl_seconds=45.0,
            heartbeat_interval_seconds=15.0,
        )

        services = await build_service_container(backend_config=config)

        assert services.queue_manager is not None
        assert services.distributed_settings is not None
        assert services.distributed_settings.redis_url == "redis://localhost:6379/0"
        assert services.distributed_settings.lease_ttl_seconds == 45.0
        assert services.distributed_settings.heartbeat_interval_seconds == 15.0
