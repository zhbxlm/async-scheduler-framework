"""config/settings.py — Global settings singleton.
aligned with docs/deepwiki-reference/配置说明.md

Three-tier priority: env vars > YAML file > code defaults.
Sub-config classes are defined in config/_infra.py, _dag.py, _task.py,
_scaling.py, _background.py, _tenant.py — imported here for convenience.
All sub-configs accessible via `settings.<module>.<param>`.
"""
from __future__ import annotations

from dataclasses import dataclass, field

# Re-export sub-config classes for backwards compatibility
from config._infra import InfraConfig, RedisConfig, MySQLConfig, ServerConfig  # noqa: F401
from config._dag import DagConfig                                               # noqa: F401
from config._task import TaskConfig, RayDataConfig, ConsumerConfig, ExecutionLockConfig  # noqa: F401
from config._scaling import ScalingConfig, CircuitBreakerConfig                 # noqa: F401
from config._background import BackgroundConfig                                 # noqa: F401
from config._tenant import TenantConfig                                         # noqa: F401


@dataclass
class AgentConfig:
    """Node agent configuration."""
    import os as _os
    node_id: str = field(default_factory=lambda: __import__('os').getenv("AGENT_NODE_ID", ""))
    host: str = field(default_factory=lambda: __import__('os').getenv("AGENT_HOST", "127.0.0.1"))
    port: int = field(default_factory=lambda: int(__import__('os').getenv("AGENT_PORT", "9100")))
    heartbeat_interval: float = 10.0
    owner_ttl_seconds: int = 30


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
