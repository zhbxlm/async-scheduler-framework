"""config/_task.py — Task execution config.
aligned with docs/deepwiki-reference/配置说明.md
"""
from __future__ import annotations
import os
from dataclasses import dataclass, field


@dataclass
class ExecutionLockConfig:
    lease_seconds: int = field(default_factory=lambda: int(os.getenv("TASK_LOCK_LEASE_SECONDS", "60")))
    renew_interval_seconds: float = field(default_factory=lambda: float(os.getenv("TASK_LOCK_RENEW_INTERVAL", "15")))


@dataclass
class ConsumerConfig:
    enabled: bool = True
    poll_interval: float = field(default_factory=lambda: float(os.getenv("TASK_CONSUMER_POLL_INTERVAL", "2.0")))
    max_concurrent: int = field(default_factory=lambda: int(os.getenv("TASK_CONSUMER_MAX_CONCURRENT", "16")))
    stale_threshold_seconds: float = field(default_factory=lambda: float(os.getenv("TASK_CONSUMER_STALE_THRESHOLD", "300")))

    def __post_init__(self):
        self.enabled = os.getenv("TASK_CONSUMER_ENABLED", "true").lower() == "true"


@dataclass
class RayDataConfig:
    submit_path: str = "/api/jobs/"
    submit_timeout_seconds: int = field(default_factory=lambda: int(os.getenv("RAYDATA_SUBMIT_TIMEOUT", "15")))
    api_token: str = field(default_factory=lambda: os.getenv("RAYDATA_API_TOKEN", ""))
    task_types: list = field(default_factory=list)

    def __post_init__(self):
        raw = os.getenv("RAYDATA_TASK_TYPES", "")
        if raw:
            self.task_types = [t.strip() for t in raw.split(",") if t.strip()]


@dataclass
class TaskConfig:
    default_timeout_seconds: int = field(default_factory=lambda: int(os.getenv("TASK_DEFAULT_TIMEOUT_SECONDS", "3600")))
    default_max_retries: int = field(default_factory=lambda: int(os.getenv("TASK_DEFAULT_MAX_RETRIES", "3")))
    record_ttl_seconds: int = field(default_factory=lambda: int(os.getenv("TASK_RECORD_TTL_SECONDS", "2592000")))
    execution_lock: ExecutionLockConfig = field(default_factory=ExecutionLockConfig)
    consumer: ConsumerConfig = field(default_factory=ConsumerConfig)
    raydata: RayDataConfig = field(default_factory=RayDataConfig)
