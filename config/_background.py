"""config/_background.py — Background daemon task config.
aligned with docs/deepwiki-reference/配置说明.md
"""
from __future__ import annotations
import os
from dataclasses import dataclass, field


@dataclass
class CronConfig:
    enabled: bool = True
    check_interval: float = field(default_factory=lambda: float(os.getenv("CRON_CHECK_INTERVAL", "60.0")))
    max_concurrent_fires: int = field(default_factory=lambda: int(os.getenv("CRON_MAX_CONCURRENT_FIRES", "16")))

    def __post_init__(self):
        self.enabled = os.getenv("CRON_SCHEDULER_ENABLED", "true").lower() == "true"


@dataclass
class CallbackRetryConfig:
    enabled: bool = True
    poll_interval: float = field(default_factory=lambda: float(os.getenv("CALLBACK_RETRY_POLL_INTERVAL", "5.0")))
    batch_size: int = field(default_factory=lambda: int(os.getenv("CALLBACK_RETRY_BATCH_SIZE", "50")))
    base_delay_seconds: int = field(default_factory=lambda: int(os.getenv("CALLBACK_RETRY_BASE_DELAY", "10")))
    max_delay_seconds: int = field(default_factory=lambda: int(os.getenv("CALLBACK_RETRY_MAX_DELAY", "600")))
    max_attempts: int = field(default_factory=lambda: int(os.getenv("CALLBACK_RETRY_MAX_ATTEMPTS", "8")))

    def __post_init__(self):
        self.enabled = os.getenv("CALLBACK_RETRY_ENABLED", "true").lower() == "true"


@dataclass
class ReconcileConfig:
    enabled: bool = True
    interval_seconds: float = field(default_factory=lambda: float(os.getenv("RECONCILE_INTERVAL_SECONDS", "15.0")))
    stuck_max_per_tick: int = field(default_factory=lambda: int(os.getenv("RECONCILE_STUCK_MAX_PER_TICK", "20")))
    stuck_task_max_age_seconds: int = field(default_factory=lambda: int(os.getenv("RECONCILE_STUCK_MAX_AGE_SECONDS", "300")))
    batch_size: int = field(default_factory=lambda: int(os.getenv("RECONCILE_BATCH_SIZE", "100")))

    def __post_init__(self):
        self.enabled = os.getenv("RECONCILE_ENABLED", "true").lower() == "true"


@dataclass
class RegistryTtlConfig:
    node_seconds: int = field(default_factory=lambda: int(os.getenv("REGISTRY_NODE_TTL_SECONDS", "604800")))
    schedule_seconds: int = field(default_factory=lambda: int(os.getenv("REGISTRY_SCHEDULE_TTL_SECONDS", "31536000")))


@dataclass
class BackgroundConfig:
    cron: CronConfig = field(default_factory=CronConfig)
    callback_retry: CallbackRetryConfig = field(default_factory=CallbackRetryConfig)
    reconcile: ReconcileConfig = field(default_factory=ReconcileConfig)
    registry_ttl: RegistryTtlConfig = field(default_factory=RegistryTtlConfig)
    heartbeat_timeout_seconds: float = field(default_factory=lambda: float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "60.0")))
    stale_running_timeout_seconds: float = field(default_factory=lambda: float(os.getenv("STALE_RUNNING_TIMEOUT_SECONDS", "300.0")))
    max_schedules_per_tenant: int = field(default_factory=lambda: int(os.getenv("MAX_SCHEDULES_PER_TENANT", "100")))
