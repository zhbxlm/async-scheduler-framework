"""Unified runtime settings for Async Scheduler.

Centralizes env-driven configuration that was previously scattered across the
codebase. This module intentionally avoids importing heavy framework modules
(e.g. backends/persistence) to prevent circular imports during bootstrap.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

DEFAULT_DATABASE_URL = "mysql+asyncmy://async_scheduler:async_scheduler@127.0.0.1:3306/async_scheduler"


@dataclass(frozen=True)
class DatabaseSettings:
    url: str = DEFAULT_DATABASE_URL
    echo: bool = False
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: float = 30.0
    pool_recycle: int = 1800

    @property
    def is_sqlite(self) -> bool:
        return self.url.startswith("sqlite")

    @property
    def is_mysql(self) -> bool:
        return self.url.startswith("mysql")


@dataclass(frozen=True)
class LoggingSettings:
    level: str = "INFO"
    fmt: str = "json"
    es_host: str | None = None
    es_index: str = "scheduler-logs"
    service_name: str = "async-scheduler"
    service_version: str | None = None
    node_id: str | None = None


@dataclass(frozen=True)
class BackendSettings:
    queue_type: str = "memory"
    lock_type: str = "memory"
    registry_type: str = "memory"
    redis_url: str | None = None
    lease_ttl_seconds: float = 30.0
    heartbeat_interval_seconds: float = 10.0


@dataclass(frozen=True)
class RuntimeSettings:
    database: DatabaseSettings
    logging: LoggingSettings
    backends: BackendSettings


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_settings(env: dict[str, str] | None = None) -> RuntimeSettings:
    source = env or os.environ

    database = DatabaseSettings(
        url=source.get("TEST_DATABASE_URL", source.get("DATABASE_URL", DEFAULT_DATABASE_URL)),
        echo=_bool(source.get("SQL_ECHO"), False),
        pool_size=int(source.get("DB_POOL_SIZE", "10")),
        max_overflow=int(source.get("DB_MAX_OVERFLOW", "20")),
        pool_timeout=float(source.get("DB_POOL_TIMEOUT", "30")),
        pool_recycle=int(source.get("DB_POOL_RECYCLE", "1800")),
    )

    logging = LoggingSettings(
        level=source.get("LOG_LEVEL", "INFO").upper(),
        fmt=source.get("LOG_FORMAT", "json"),
        es_host=source.get("LOG_ES_HOST"),
        es_index=source.get("LOG_ES_INDEX", "scheduler-logs"),
        service_name=source.get("SERVICE_NAME", "async-scheduler"),
        service_version=source.get("SERVICE_VERSION"),
        node_id=source.get("NODE_ID"),
    )

    backends = BackendSettings(
        queue_type=source.get("BACKEND_QUEUE_TYPE", source.get("QUEUE_TYPE", "memory")),
        lock_type=source.get("BACKEND_LOCK_TYPE", source.get("LOCK_TYPE", "memory")),
        registry_type=source.get("BACKEND_REGISTRY_TYPE", source.get("REGISTRY_TYPE", "memory")),
        redis_url=source.get("REDIS_URL"),
        lease_ttl_seconds=float(source.get("LEASE_TTL_SECONDS", "30.0")),
        heartbeat_interval_seconds=float(source.get("HEARTBEAT_INTERVAL_SECONDS", "10.0")),
    )

    return RuntimeSettings(database=database, logging=logging, backends=backends)


@lru_cache(maxsize=1)
def get_settings() -> RuntimeSettings:
    return load_settings()
