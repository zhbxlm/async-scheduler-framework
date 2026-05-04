"""config/settings.py — Global settings singleton.
aligned with docs/deepwiki-reference/配置说明.md

Three-tier priority: env vars > YAML file > code defaults.
All sub-configs accessible via `settings.<module>.<param>`.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Sub-config classes
# ---------------------------------------------------------------------------

@dataclass
class RedisConfig:
    host: str = field(default_factory=lambda: os.getenv("REDIS_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("REDIS_PORT", "6379")))
    db: int = field(default_factory=lambda: int(os.getenv("REDIS_DB", "0")))
    password: str | None = field(default_factory=lambda: os.getenv("REDIS_PASSWORD"))

    @property
    def url(self) -> str:
        auth = f":{self.password}@" if self.password else ""
        return f"redis://{auth}{self.host}:{self.port}/{self.db}"


@dataclass
class MySQLConfig:
    host: str = field(default_factory=lambda: os.getenv("MYSQL_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("MYSQL_PORT", "3306")))
    user: str = field(default_factory=lambda: os.getenv("MYSQL_USER", "root"))
    password: str = field(default_factory=lambda: os.getenv("MYSQL_PASSWORD", ""))
    database: str = field(default_factory=lambda: os.getenv("MYSQL_DATABASE", "async_scheduler"))
    pool_size: int = 5
    max_overflow: int = 10

    @property
    def url(self) -> str:
        return (
            f"mysql+asyncmy://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass
class InfraConfig:
    redis: RedisConfig = field(default_factory=RedisConfig)
    mysql: MySQLConfig = field(default_factory=MySQLConfig)


@dataclass
class DagConfig:
    """DAG engine configuration."""
    max_parallelism: int = field(
        default_factory=lambda: int(os.getenv("DAG_MAX_PARALLELISM", "8"))
    )
    step_timeout_seconds: int = 300
    dag_timeout_seconds: int = 3600
    checkpoint_enabled: bool = False


@dataclass
class RayDataConfig:
    """RayData integration configuration."""
    submit_path: str = "/api/jobs/"
    submit_timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("RAYDATA_SUBMIT_TIMEOUT", "15"))
    )
    api_token: str | None = field(
        default_factory=lambda: os.getenv("RAYDATA_API_TOKEN")
    )
    task_types: list[str] = field(default_factory=list)  # auto RAYDATA_NATIVE types


@dataclass
class TaskConfig:
    """Task execution configuration."""
    default_timeout_seconds: int = 3600
    default_max_retries: int = 3
    raydata: RayDataConfig = field(default_factory=RayDataConfig)


@dataclass
class AgentConfig:
    """Node agent configuration."""
    node_id: str = field(default_factory=lambda: os.getenv("AGENT_NODE_ID", ""))
    host: str = field(default_factory=lambda: os.getenv("AGENT_HOST", "127.0.0.1"))
    port: int = field(default_factory=lambda: int(os.getenv("AGENT_PORT", "9100")))
    heartbeat_interval: float = 10.0
    owner_ttl_seconds: int = 30


@dataclass
class ScalingConfig:
    """Auto-scaling configuration."""
    up_threshold: float = field(
        default_factory=lambda: float(os.getenv("SCALING_UP_THRESHOLD", "0.8"))
    )
    down_threshold: float = field(
        default_factory=lambda: float(os.getenv("SCALING_DOWN_THRESHOLD", "0.3"))
    )
    cooldown_seconds: int = field(
        default_factory=lambda: int(os.getenv("SCALING_COOLDOWN", "60"))
    )
    max_increment: int = 4
    min_actors: int = 1


@dataclass
class BackgroundConfig:
    """Background daemon task configuration."""
    reconciler_interval_seconds: float = 30.0
    heartbeat_timeout_seconds: float = 60.0
    stale_running_timeout_seconds: float = 300.0
    cron_poll_interval_seconds: float = 60.0


@dataclass
class TenantConfig:
    """Multi-tenancy configuration."""
    default_max_gpus: int = 0
    default_max_queue_depth: int = 0
    default_max_concurrent_tasks: int = 0
    worker_artifact_max_size_mb: int = field(
        default_factory=lambda: int(os.getenv("WORKER_ARTIFACT_MAX_SIZE_MB", "1024"))
    )
    worker_artifact_cache_max_entries: int = 50


# ---------------------------------------------------------------------------
# Top-level Settings
# ---------------------------------------------------------------------------

@dataclass
class Settings:
    """Global settings singleton aggregating all sub-configs."""
    infra: InfraConfig = field(default_factory=InfraConfig)
    dag: DagConfig = field(default_factory=DagConfig)
    task: TaskConfig = field(default_factory=TaskConfig)
    agent: AgentConfig = field(default_factory=AgentConfig)
    scaling: ScalingConfig = field(default_factory=ScalingConfig)
    background: BackgroundConfig = field(default_factory=BackgroundConfig)
    tenant: TenantConfig = field(default_factory=TenantConfig)

    # Convenience aliases
    @property
    def redis(self) -> RedisConfig:
        return self.infra.redis

    @property
    def mysql(self) -> MySQLConfig:
        return self.infra.mysql


# Global singleton
settings = Settings()
