from __future__ import annotations

import pytest
import pytest_asyncio
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from src.common.async_db import Base
from tests.fake_redis import FullFakeAsyncRedis

# ---------------------------------------------------------------------------
# Async SQLite in-memory engine + session factory for integration tests
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="function")
async def async_db_session_factory():
    """Async SQLite in-memory session factory for integration tests."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    @asynccontextmanager
    async def _factory():
        async with AsyncSessionLocal() as session:
            yield session

    yield _factory

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.fixture(scope="function")
def fake_redis():
    return FullFakeAsyncRedis()
