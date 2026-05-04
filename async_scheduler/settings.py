"""Unified runtime settings for Async Scheduler.

Centralizes env-driven configuration that was previously scattered across the
codebase. This module intentionally avoids importing heavy framework modules
(e.g. backends/persistence) to prevent circular imports during bootstrap.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

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
class RuntimeMetadata:
    environment: str = "local"
    deployment_role: str = "all-in-one"
    deployment_name: str | None = None
    service_name: str = "async-scheduler"
    node_id: str | None = None
    distributed_enabled: bool = False


@dataclass(frozen=True)
class RuntimeSettings:
    database: DatabaseSettings
    logging: LoggingSettings
    backends: BackendSettings
    runtime: RuntimeMetadata

    def summary(self) -> dict[str, Any]:
        return {
            "environment": self.runtime.environment,
            "deployment_role": self.runtime.deployment_role,
            "deployment_name": self.runtime.deployment_name,
            "service_name": self.runtime.service_name,
            "node_id": self.runtime.node_id,
            "distributed_enabled": self.runtime.distributed_enabled,
            "database_driver": "sqlite" if self.database.is_sqlite else ("mysql" if self.database.is_mysql else "other"),
            "database_url": self.database.url,
            "redis_configured": bool(self.backends.redis_url),
            "queue_type": self.backends.queue_type,
            "lock_type": self.backends.lock_type,
            "registry_type": self.backends.registry_type,
            "lease_ttl_seconds": self.backends.lease_ttl_seconds,
            "heartbeat_interval_seconds": self.backends.heartbeat_interval_seconds,
            "log_level": self.logging.level,
            "log_format": self.logging.fmt,
        }


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _infer_environment(source: dict[str, str]) -> str:
    value = source.get("ENVIRONMENT", "local").strip().lower()
    aliases = {
        "prod": "production",
        "production": "production",
        "stage": "staging",
        "staging": "staging",
        "test": "test",
        "testing": "test",
        "dev": "development",
        "development": "development",
        "local": "local",
    }
    return aliases.get(value, value or "local")


def _infer_distributed_enabled(source: dict[str, str], *, queue_type: str, lock_type: str, redis_url: str | None) -> bool:
    explicit = source.get("DISTRIBUTED_MODE")
    if explicit is not None:
        return _bool(explicit, False)
    return bool(redis_url) and (queue_type == "redis" or lock_type == "redis")


def _validate_settings(settings: RuntimeSettings) -> None:
    if settings.runtime.distributed_enabled and not settings.backends.redis_url:
        raise ValueError("REDIS_URL is required when distributed mode is enabled")
    if settings.backends.lease_ttl_seconds <= 0:
        raise ValueError("LEASE_TTL_SECONDS must be > 0")
    if settings.backends.heartbeat_interval_seconds <= 0:
        raise ValueError("HEARTBEAT_INTERVAL_SECONDS must be > 0")
    if settings.backends.heartbeat_interval_seconds >= settings.backends.lease_ttl_seconds:
        raise ValueError("heartbeat interval must be smaller than lease ttl")


def load_settings(env: dict[str, str] | None = None, *, validate: bool = False) -> RuntimeSettings:
    source = os.environ if env is None else env

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

    queue_type = source.get("BACKEND_QUEUE_TYPE", source.get("QUEUE_TYPE", "memory"))
    lock_type = source.get("BACKEND_LOCK_TYPE", source.get("LOCK_TYPE", "memory"))
    registry_type = source.get("BACKEND_REGISTRY_TYPE", source.get("REGISTRY_TYPE", "memory"))
    redis_url = source.get("REDIS_URL")

    backends = BackendSettings(
        queue_type=queue_type,
        lock_type=lock_type,
        registry_type=registry_type,
        redis_url=redis_url,
        lease_ttl_seconds=float(source.get("LEASE_TTL_SECONDS", "30.0")),
        heartbeat_interval_seconds=float(source.get("HEARTBEAT_INTERVAL_SECONDS", "10.0")),
    )

    runtime = RuntimeMetadata(
        environment=_infer_environment(source),
        deployment_role=source.get("DEPLOYMENT_ROLE", "all-in-one"),
        deployment_name=source.get("DEPLOYMENT_NAME"),
        service_name=logging.service_name,
        node_id=logging.node_id,
        distributed_enabled=_infer_distributed_enabled(
            source,
            queue_type=queue_type,
            lock_type=lock_type,
            redis_url=redis_url,
        ),
    )

    settings = RuntimeSettings(database=database, logging=logging, backends=backends, runtime=runtime)
    if validate:
        _validate_settings(settings)
    return settings


@lru_cache(maxsize=1)
def get_settings() -> RuntimeSettings:
    return load_settings()
