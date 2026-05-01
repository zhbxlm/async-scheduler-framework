from __future__ import annotations

import pytest

from async_scheduler.backends.factory import BackendConfig
from async_scheduler.platform.services import build_service_container


@pytest.mark.asyncio
async def test_build_service_container_creates_worker_registry_in_distributed_mode() -> None:
    services = await build_service_container(
        BackendConfig(
            queue_type="redis",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
            distributed_mode=True,
            heartbeat_interval_seconds=5.0,
        )
    )

    assert services.distributed_settings is not None
    assert services.worker_registry is not None
    assert services.worker_registry._heartbeat_ttl_seconds == 10.0


@pytest.mark.asyncio
async def test_build_service_container_keeps_worker_registry_none_in_memory_mode() -> None:
    services = await build_service_container()

    assert services.distributed_settings is None
    assert services.worker_registry is None
