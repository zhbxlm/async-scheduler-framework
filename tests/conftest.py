"""Pytest fixtures shared across test modules."""
from __future__ import annotations

import pytest
import pytest_asyncio
import asyncio
from typing import AsyncGenerator
from fastapi.testclient import TestClient

from src.main import app


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function")
def test_client() -> TestClient:
    """Create a FastAPI TestClient for API testing."""
    return TestClient(app)


@pytest.fixture(scope="function")
async def async_client() -> AsyncGenerator[TestClient, None]:
    """Async version of test client for async tests."""
    with TestClient(app) as client:
        yield client


# Database fixtures
@pytest.fixture(scope="session")
def db_engine():
    """Database engine fixture for tests that need DB."""
    from sqlalchemy import create_engine
    from src.common.async_db import Base
    
    # Use in-memory SQLite for tests
    engine = create_engine("sqlite:///:memory:", echo=False)
    
    # Create tables
    Base.metadata.create_all(bind=engine)
    
    yield engine
    
    # Cleanup
    Base.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture(scope="function")
def db_session(db_engine):
    """Database session fixture."""
    from sqlalchemy.orm import sessionmaker
    
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)
    session = SessionLocal()
    
    try:
        yield session
    finally:
        session.rollback()
        session.close()


# Redis fixtures
@pytest.fixture(scope="function")
async def redis_client():
    """Redis client fixture for tests."""
    import redis.asyncio as aioredis
    from unittest.mock import AsyncMock
    
    # For unit tests, use mock by default
    # Integration tests can override with real Redis
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=None)
    mock_client.set = AsyncMock(return_value=True)
    mock_client.delete = AsyncMock(return_value=1)
    mock_client.sadd = AsyncMock(return_value=1)
    mock_client.smembers = AsyncMock(return_value=set())
    mock_client.pipeline.return_value = AsyncMock()
    from unittest.mock import MagicMock
    mock_client.register_script = lambda script: MagicMock()
    
    yield mock_client


# Service fixtures
@pytest.fixture(scope="function")
def task_creator(redis_client):
    """TaskCreator fixture for unit tests."""
    from src.platform.task_creator import TaskCreator
    from unittest.mock import AsyncMock
    
    mock_queue = AsyncMock()
    mock_queue.enqueue = AsyncMock(return_value={
        "accepted": True,
        "queue_position": 0,
        "pending_count": 1,
    })
    
    return TaskCreator(
        redis_client=redis_client,
        queue_manager=mock_queue,
        db_session_factory=None,
    )


@pytest.fixture(scope="function")
def task_reconciler(redis_client):
    """TaskReconciler fixture for unit tests."""
    from src.platform.task_reconciler import TaskReconciler
    from unittest.mock import AsyncMock
    
    mock_queue = AsyncMock()
    
    return TaskReconciler(
        redis_client=redis_client,
        db_session_factory=None,
        queue_manager=mock_queue,
        interval_seconds=3600,  # Long interval for tests
        stuck_max_per_tick=5,
        stuck_task_max_age_seconds=300,
        batch_size=10,
    )


# Environment variable management
@pytest.fixture(scope="function", autouse=True)
def env_vars(monkeypatch):
    """Set environment variables for tests."""
    # Clear any existing test env vars
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/15")  # Use DB 15 for tests
    monkeypatch.setenv("MYSQL_URL", "")
    monkeypatch.setenv("RECONCILE_ENABLED", "false")
    monkeypatch.setenv("CRON_SCHEDULER_ENABLED", "false")
    
    yield
    
    # Cleanup
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("MYSQL_URL", raising=False)
    monkeypatch.delenv("RECONCILE_ENABLED", raising=False)
    monkeypatch.delenv("CRON_SCHEDULER_ENABLED", raising=False)

# ---------------------------------------------------------------------------
# Async SQLite in-memory fixtures (re-exported from integration_fixtures)
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="function")
async def async_db_session_factory():
    """Async SQLite in-memory session factory — for integration tests."""
    from contextlib import asynccontextmanager
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from src.common.async_db import Base

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
    from tests.fake_redis import FullFakeAsyncRedis
    return FullFakeAsyncRedis()
