"""Persistence configuration and database setup."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from async_scheduler.settings import get_settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None
_bound_url: str | None = None


def _build_engine(url: str) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    db_settings = get_settings().database
    is_sqlite = url.startswith("sqlite")
    engine = create_async_engine(
        url,
        echo=db_settings.echo,
        pool_pre_ping=True,
        **({} if is_sqlite else {
            "pool_size": db_settings.pool_size,
            "max_overflow": db_settings.max_overflow,
            "pool_timeout": db_settings.pool_timeout,
            "pool_recycle": db_settings.pool_recycle,
        }),
    )
    session_factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    return engine, session_factory


def get_database_url() -> str:
    return get_settings().database.url


def _ensure_bound() -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    global _engine, _session_factory, _bound_url
    url = get_database_url()
    if _engine is None or _session_factory is None or _bound_url != url:
        _engine, _session_factory = _build_engine(url)
        _bound_url = url
    return _engine, _session_factory


async def rebind_engine() -> None:
    """Dispose and rebuild engine for current environment settings."""
    global _engine, _session_factory, _bound_url
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None
    _bound_url = None
    _ensure_bound()


async def init_db() -> None:
    """Initialize database tables."""
    from async_scheduler.persistence.models import Base

    engine, _ = _ensure_bound()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def drop_db() -> None:
    """Drop all database tables."""
    from sqlalchemy import text
    from async_scheduler.persistence.models import Base

    engine, _ = _ensure_bound()
    async with engine.begin() as conn:
        if get_database_url().startswith("mysql"):
            await conn.exec_driver_sql("SET FOREIGN_KEY_CHECKS=0")
            for table in reversed(Base.metadata.sorted_tables):
                await conn.exec_driver_sql(f"DROP TABLE IF EXISTS `{table.name}`")
            await conn.exec_driver_sql("SET FOREIGN_KEY_CHECKS=1")
        else:
            await conn.run_sync(lambda sync_conn: Base.metadata.drop_all(sync_conn, checkfirst=True))


async def close_db() -> None:
    """Dispose database engine and close pooled connections."""
    global _engine, _session_factory, _bound_url
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None
    _bound_url = None


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Get a database session."""
    _, session_factory = _ensure_bound()
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_session_no_context() -> AsyncSession:
    """Get a database session without context manager."""
    _, session_factory = _ensure_bound()
    return session_factory()
