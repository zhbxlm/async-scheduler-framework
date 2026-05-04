"""Background daemon task config."""
from __future__ import annotations
import os
from dataclasses import dataclass, field


@dataclass
class CronConfig:
    enabled: bool = True
    check_interval: float = 60.0

    def __post_init__(self):
        self.enabled = os.getenv("CRON_SCHEDULER_ENABLED", "true").lower() == "true"
        self.check_interval = float(os.getenv("CRON_SCHEDULER_CHECK_INTERVAL", self.check_interval))


@dataclass
class CallbackRetryConfig:
    enabled: bool = True
    poll_interval: float = 5.0
    batch_size: int = 50
    base_delay_seconds: int = 10
    max_delay_seconds: int = 600
    max_attempts: int = 8

    def __post_init__(self):
        self.enabled = os.getenv("CALLBACK_RETRY_ENABLED", "true").lower() == "true"
        self.poll_interval = float(os.getenv("CALLBACK_RETRY_POLL_INTERVAL", self.poll_interval))
        self.batch_size = int(os.getenv("CALLBACK_RETRY_BATCH_SIZE", self.batch_size))
        self.base_delay_seconds = int(os.getenv("CALLBACK_RETRY_BASE_DELAY_SECONDS", self.base_delay_seconds))
        self.max_delay_seconds = int(os.getenv("CALLBACK_RETRY_MAX_DELAY_SECONDS", self.max_delay_seconds))
        self.max_attempts = int(os.getenv("CALLBACK_RETRY_MAX_ATTEMPTS", self.max_attempts))


@dataclass
class ResourceGovernanceConfig:
    enabled: bool = True
    interval: float = 30.0

    def __post_init__(self):
        self.enabled = os.getenv("RESOURCE_GOVERNANCE_ENABLED", "true").lower() == "true"
        self.interval = float(os.getenv("RESOURCE_GOVERNANCE_INTERVAL", self.interval))


@dataclass
class RoleLockConfig:
    ttl_seconds: int = 30
    renew_interval_seconds: float = 10.0

    def __post_init__(self):
        self.ttl_seconds = int(os.getenv("BACKGROUND_ROLE_LOCK_TTL_SECONDS", self.ttl_seconds))
        self.renew_interval_seconds = float(os.getenv("BACKGROUND_ROLE_LOCK_RENEW_INTERVAL_SECONDS", self.renew_interval_seconds))


@dataclass
class ReconcileConfig:
    enabled: bool = True
    interval_seconds: float = 15.0
    stuck_max_per_tick: int = 20
    stuck_task_max_age_seconds: int = 300
    dead_letter_capacity: int = 1000

    def __post_init__(self):
        self.enabled = os.getenv("RECONCILE_ENABLED", "true").lower() == "true"
        self.interval_seconds = float(os.getenv("RECONCILE_INTERVAL_SECONDS", self.interval_seconds))
        self.stuck_max_per_tick = int(os.getenv("RECONCILE_STUCK_MAX_PER_TICK", self.stuck_max_per_tick))
        self.stuck_task_max_age_seconds = int(os.getenv("RECONCILE_STUCK_TASK_MAX_AGE_SECONDS", self.stuck_task_max_age_seconds))
        self.dead_letter_capacity = int(os.getenv("RECONCILE_DEAD_LETTER_CAPACITY", self.dead_letter_capacity))


@dataclass
class RegistryTtlConfig:
    node_seconds: int = 604800
    schedule_seconds: int = 31536000

    def __post_init__(self):
        self.node_seconds = int(os.getenv("REGISTRY_NODE_TTL_SECONDS", self.node_seconds))
        self.schedule_seconds = int(os.getenv("REGISTRY_SCHEDULE_TTL_SECONDS", self.schedule_seconds))


@dataclass
class BackgroundConfig:
    cron: CronConfig = field(default_factory=CronConfig)
    callback_retry: CallbackRetryConfig = field(default_factory=CallbackRetryConfig)
    resource_governance: ResourceGovernanceConfig = field(default_factory=ResourceGovernanceConfig)
    role_lock: RoleLockConfig = field(default_factory=RoleLockConfig)
    reconcile: ReconcileConfig = field(default_factory=ReconcileConfig)
    registry_ttl: RegistryTtlConfig = field(default_factory=RegistryTtlConfig)
    max_schedules_per_tenant: int = 100

    def __post_init__(self):
        self.max_schedules_per_tenant = int(os.getenv("MAX_SCHEDULES_PER_TENANT", self.max_schedules_per_tenant))
