"""Async database session management for FastAPI."""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()

_async_engine: Optional[AsyncEngine] = None
_AsyncSessionLocal: Optional[async_sessionmaker[AsyncSession]] = None


def init_async_engine(url: str, **kwargs) -> None:
    """Initialize async SQLAlchemy engine.
    
    Args:
        url: Database URL (must start with aiomysql:// or asyncpg:// for async)
        **kwargs: Additional engine options
    """
    global _async_engine, _AsyncSessionLocal
    
    # Convert sync URL to async if needed
    if url.startswith("mysql://"):
        url = url.replace("mysql://", "aiomysql://", 1)
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "asyncpg://", 1)
    
    # Ensure pool settings for production
    kwargs.setdefault("pool_pre_ping", True)
    kwargs.setdefault("pool_recycle", 3600)
    kwargs.setdefault("pool_size", 20)
    kwargs.setdefault("max_overflow", 10)
    
    _async_engine = create_async_engine(url, **kwargs)
    _AsyncSessionLocal = async_sessionmaker(
        bind=_async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )


def get_async_engine() -> AsyncEngine:
    if _async_engine is None:
        raise RuntimeError("Async engine not initialized. Call init_async_engine() first.")
    return _async_engine


@asynccontextmanager
async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    """Async context manager for database sessions.
    
    Usage:
        async with get_async_db() as session:
            result = await session.execute(query)
            await session.commit()
    """
    if _AsyncSessionLocal is None:
        raise RuntimeError("Async session factory not initialized. Call init_async_engine() first.")
    
    session: AsyncSession = _AsyncSessionLocal()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


async def async_dispose_engine() -> None:
    """Dispose async engine (call during shutdown)."""
    global _async_engine, _AsyncSessionLocal
    if _async_engine:
        await _async_engine.dispose()
        _async_engine = None
        _AsyncSessionLocal = None