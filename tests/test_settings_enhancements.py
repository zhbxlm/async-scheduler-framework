from __future__ import annotations

import pytest

from async_scheduler.settings import load_settings


def test_load_settings_infers_distributed_enabled_from_redis_backends() -> None:
    settings = load_settings(
        env={
            "BACKEND_QUEUE_TYPE": "redis",
            "BACKEND_LOCK_TYPE": "redis",
            "REDIS_URL": "redis://localhost:6379/0",
        }
    )

    assert settings.runtime.distributed_enabled is True
    assert settings.runtime.deployment_role == "all-in-one"
    assert settings.runtime.environment == "local"


def test_load_settings_supports_runtime_metadata_and_summary() -> None:
    settings = load_settings(
        env={
            "ENVIRONMENT": "staging",
            "DEPLOYMENT_ROLE": "worker",
            "DEPLOYMENT_NAME": "split-dev",
            "SERVICE_NAME": "scheduler-worker",
            "NODE_ID": "node-a",
            "BACKEND_QUEUE_TYPE": "redis",
            "BACKEND_LOCK_TYPE": "redis",
            "REDIS_URL": "redis://cache:6379/0",
            "DATABASE_URL": "mysql+asyncmy://u:p@db:3306/test",
        }
    )

    assert settings.runtime.environment == "staging"
    assert settings.runtime.deployment_role == "worker"
    assert settings.runtime.deployment_name == "split-dev"
    assert settings.runtime.service_name == "scheduler-worker"
    assert settings.runtime.node_id == "node-a"

    summary = settings.summary()
    assert summary["environment"] == "staging"
    assert summary["deployment_role"] == "worker"
    assert summary["distributed_enabled"] is True
    assert summary["database_driver"] == "mysql"
    assert summary["redis_configured"] is True


def test_load_settings_validate_rejects_missing_redis_url_for_distributed_mode() -> None:
    with pytest.raises(ValueError, match="REDIS_URL"):
        load_settings(
            env={
                "DISTRIBUTED_MODE": "true",
                "BACKEND_QUEUE_TYPE": "redis",
                "BACKEND_LOCK_TYPE": "redis",
            },
            validate=True,
        )


def test_load_settings_validate_rejects_invalid_heartbeat_ratio() -> None:
    with pytest.raises(ValueError, match="heartbeat"):
        load_settings(
            env={
                "REDIS_URL": "redis://localhost:6379/0",
                "BACKEND_QUEUE_TYPE": "redis",
                "BACKEND_LOCK_TYPE": "redis",
                "LEASE_TTL_SECONDS": "5",
                "HEARTBEAT_INTERVAL_SECONDS": "5",
            },
            validate=True,
        )
