from __future__ import annotations

from async_scheduler.settings import DEFAULT_DATABASE_URL, load_settings


def test_load_settings_defaults() -> None:
    settings = load_settings(env={})

    assert settings.database.url == DEFAULT_DATABASE_URL
    assert settings.database.echo is False
    assert settings.logging.level == "INFO"
    assert settings.logging.fmt == "json"
    assert settings.backends.queue_type == "memory"
    assert settings.backends.lock_type == "memory"
    assert settings.backends.registry_type == "memory"


def test_load_settings_custom_env() -> None:
    settings = load_settings(
        env={
            "DATABASE_URL": "mysql+asyncmy://u:p@db:3306/test",
            "SQL_ECHO": "true",
            "DB_POOL_SIZE": "30",
            "DB_MAX_OVERFLOW": "40",
            "DB_POOL_TIMEOUT": "9.5",
            "DB_POOL_RECYCLE": "300",
            "LOG_LEVEL": "debug",
            "LOG_FORMAT": "text",
            "LOG_ES_HOST": "http://es:9200",
            "LOG_ES_INDEX": "scheduler-prod",
            "SERVICE_NAME": "scheduler-api",
            "SERVICE_VERSION": "1.2.3",
            "NODE_ID": "node-a",
            "QUEUE_TYPE": "redis",
            "LOCK_TYPE": "redis",
            "REGISTRY_TYPE": "memory",
            "REDIS_URL": "redis://localhost:6379/0",
            "LEASE_TTL_SECONDS": "12",
            "HEARTBEAT_INTERVAL_SECONDS": "3",
        }
    )

    assert settings.database.url == "mysql+asyncmy://u:p@db:3306/test"
    assert settings.database.echo is True
    assert settings.database.pool_size == 30
    assert settings.database.max_overflow == 40
    assert settings.database.pool_timeout == 9.5
    assert settings.database.pool_recycle == 300
    assert settings.database.is_sqlite is False

    assert settings.logging.level == "DEBUG"
    assert settings.logging.fmt == "text"
    assert settings.logging.es_host == "http://es:9200"
    assert settings.logging.es_index == "scheduler-prod"
    assert settings.logging.service_name == "scheduler-api"
    assert settings.logging.service_version == "1.2.3"
    assert settings.logging.node_id == "node-a"

    assert settings.backends.queue_type == "redis"
    assert settings.backends.lock_type == "redis"
    assert settings.backends.registry_type == "memory"
    assert settings.backends.redis_url == "redis://localhost:6379/0"
    assert settings.backends.lease_ttl_seconds == 12.0
    assert settings.backends.heartbeat_interval_seconds == 3.0


def test_load_settings_backend_prefers_backend_prefix() -> None:
    settings = load_settings(
        env={
            "QUEUE_TYPE": "memory",
            "BACKEND_QUEUE_TYPE": "redis",
            "LOCK_TYPE": "memory",
            "BACKEND_LOCK_TYPE": "redis",
            "REDIS_URL": "redis://localhost:6379/0",
        }
    )

    assert settings.backends.queue_type == "redis"
    assert settings.backends.lock_type == "redis"
