from __future__ import annotations

import asyncio

import pytest

from async_scheduler.backends.factory import BackendConfig, BackendFactory


@pytest.mark.asyncio
class TestRedisLockBackend:
    async def test_factory_can_create_redis_lock_backend(self) -> None:
        config = BackendConfig(
            queue_type="memory",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
        )

        backend = BackendFactory(config).create_lock_backend()

        assert backend is not None
        assert backend.__class__.__name__ == "RedisLockBackend"

    async def test_acquire_succeeds_when_unlocked(self) -> None:
        config = BackendConfig(
            queue_type="memory",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
        )
        backend = BackendFactory(config).create_lock_backend()

        handle = await backend.acquire("task-1", ttl=10.0)

        assert handle is not None
        assert handle.key == "task-1"
        assert handle.token
        assert handle.expires_at is not None

    async def test_second_acquire_fails_while_lock_active(self) -> None:
        config = BackendConfig(
            queue_type="memory",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
        )
        backend = BackendFactory(config).create_lock_backend()

        handle1 = await backend.acquire("task-1", ttl=10.0)
        handle2 = await backend.acquire("task-1", wait=0.1)

        assert handle1 is not None
        assert handle2 is None

    async def test_release_with_correct_token_succeeds(self) -> None:
        config = BackendConfig(
            queue_type="memory",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
        )
        backend = BackendFactory(config).create_lock_backend()

        handle = await backend.acquire("task-1", ttl=10.0)
        assert handle is not None

        released = await backend.release(handle)
        assert released is True

        # Now can re-acquire
        new_handle = await backend.acquire("task-1", wait=0.1)
        assert new_handle is not None

    async def test_release_with_wrong_token_fails(self) -> None:
        config = BackendConfig(
            queue_type="memory",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
        )
        backend = BackendFactory(config).create_lock_backend()

        handle = await backend.acquire("task-1", ttl=10.0)
        assert handle is not None

        # Simulate wrong token
        from async_scheduler.backends.base import LockHandle
        wrong_handle = LockHandle(key="task-1", token="wrong-token")

        released = await backend.release(wrong_handle)
        assert released is False

        # Lock still held
        assert await backend.is_locked("task-1") is True

    async def test_extend_with_correct_token_succeeds(self) -> None:
        config = BackendConfig(
            queue_type="memory",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
        )
        backend = BackendFactory(config).create_lock_backend()

        handle = await backend.acquire("task-1", ttl=1.0)
        assert handle is not None

        extended = await backend.extend(handle, ttl=10.0)
        assert extended is True

        # Lock still held
        assert await backend.is_locked("task-1") is True

    async def test_extend_with_wrong_token_fails(self) -> None:
        config = BackendConfig(
            queue_type="memory",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
        )
        backend = BackendFactory(config).create_lock_backend()

        handle = await backend.acquire("task-1", ttl=1.0)
        assert handle is not None

        from async_scheduler.backends.base import LockHandle
        wrong_handle = LockHandle(key="task-1", token="wrong-token")

        extended = await backend.extend(wrong_handle, ttl=10.0)
        assert extended is False

    async def test_expired_lease_can_be_reacquired(self) -> None:
        config = BackendConfig(
            queue_type="memory",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
        )
        backend = BackendFactory(config).create_lock_backend()

        handle1 = await backend.acquire("task-1", ttl=0.1)
        assert handle1 is not None

        await asyncio.sleep(0.15)  # Wait for expiry

        handle2 = await backend.acquire("task-1", wait=0.1)
        assert handle2 is not None

    async def test_is_locked_reflects_state(self) -> None:
        config = BackendConfig(
            queue_type="memory",
            lock_type="redis",
            registry_type="memory",
            redis_url="redis://localhost:6379/0",
        )
        backend = BackendFactory(config).create_lock_backend()

        assert await backend.is_locked("task-1") is False

        handle = await backend.acquire("task-1", ttl=1.0)
        assert handle is not None
        assert await backend.is_locked("task-1") is True

        await backend.release(handle)
        assert await backend.is_locked("task-1") is False
