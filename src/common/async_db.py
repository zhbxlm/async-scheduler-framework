"""Async database session management for FastAPI.

Design notes:
- init_async_engine() returns (engine, session_factory) — no module-level globals
- engine + session_factory are stored in app.state during lifespan startup
- get_async_db(session_factory) provides session via injected factory
- async_dispose_engine(engine) is called during shutdown to cleanup connections

For testing, call init_async_engine() directly and pass the session_factory.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


def init_async_engine(url: str, **kwargs) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    """Initialize async SQLAlchemy engine and return (engine, session_factory).

    Args:
        url: Database URL (must start with aiomysql:// or asyncpg:// for async)
              Can also be a Pydantic DSN object which will be converted to string.
        **kwargs: Additional engine options

    Returns:
        Tuple of (AsyncEngine, async_sessionmaker) — caller stores these in app.state.
    """
    # Convert Pydantic DSN objects to string
    if hasattr(url, '__str__'):
        url = str(url)

    # Convert sync URL to async if needed
    if url.startswith("mysql://"):
        url = url.replace("mysql://", "aiomysql://", 1)
    elif url.startswith("mysql+aiomysql://"):
        # Keep mysql+aiomysql:// format for SQLAlchemy 2.0
        pass
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "asyncpg://", 1)

    # Ensure pool settings for production
    kwargs.setdefault("pool_pre_ping", True)
    kwargs.setdefault("pool_recycle", 3600)
    kwargs.setdefault("pool_size", 20)
    kwargs.setdefault("max_overflow", 10)

    engine = create_async_engine(url, **kwargs)
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    return engine, session_factory


@asynccontextmanager
async def get_async_db(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """Async context manager for database sessions.

    Usage:
        async with get_async_db(session_factory) as session:
            result = await session.execute(query)
            await session.commit()
    """
    session: AsyncSession = session_factory()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


async def async_dispose_engine(engine: AsyncEngine) -> None:
    """Dispose async engine (call during shutdown)."""
    await engine.dispose()