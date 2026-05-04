"""Task execution config."""
from __future__ import annotations
import os
from dataclasses import dataclass, field


@dataclass
class ExecutionLockConfig:
    lease_seconds: int = 60
    renew_interval_seconds: int = 15

    def __post_init__(self):
        self.lease_seconds = int(os.getenv("TASK_EXECUTION_LOCK_LEASE_SECONDS", self.lease_seconds))
        self.renew_interval_seconds = int(os.getenv("TASK_EXECUTION_LOCK_RENEW_INTERVAL_SECONDS", self.renew_interval_seconds))


@dataclass
class ConsumerConfig:
    enabled: bool = True
    poll_interval: float = 2.0
    max_concurrent: int = 16
    requeue_backoff_base_seconds: int = 5
    requeue_backoff_max_seconds: int = 300

    def __post_init__(self):
        self.enabled = os.getenv("TASK_CONSUMER_ENABLED", "true").lower() == "true"
        self.poll_interval = float(os.getenv("TASK_CONSUMER_POLL_INTERVAL", self.poll_interval))
        self.max_concurrent = int(os.getenv("TASK_CONSUMER_MAX_CONCURRENT", self.max_concurrent))
        self.requeue_backoff_base_seconds = int(os.getenv("TASK_REQUEUE_BACKOFF_BASE_SECONDS", self.requeue_backoff_base_seconds))
        self.requeue_backoff_max_seconds = int(os.getenv("TASK_REQUEUE_BACKOFF_MAX_SECONDS", self.requeue_backoff_max_seconds))


@dataclass
class RayDataConfig:
    task_types: list = field(default_factory=list)
    task_type_cluster_map: dict = field(default_factory=dict)
    submit_path: str = "/api/jobs/"
    submit_timeout_seconds: int = 15
    api_token: str = ""

    def __post_init__(self):
        self.submit_path = os.getenv("RAYDATA_SUBMIT_PATH", self.submit_path)
        self.submit_timeout_seconds = int(os.getenv("RAYDATA_SUBMIT_TIMEOUT_SECONDS", self.submit_timeout_seconds))
        self.api_token = os.getenv("RAYDATA_API_TOKEN", self.api_token)


@dataclass
class TaskConfig:
    execution_lock: ExecutionLockConfig = field(default_factory=ExecutionLockConfig)
    consumer: ConsumerConfig = field(default_factory=ConsumerConfig)
    raydata: RayDataConfig = field(default_factory=RayDataConfig)
    persist_strict: bool = False
    scheduler_actor_version: str = "v1"
    scheduler_max_concurrency: int = 16
    record_ttl_seconds: int = 2592000

    def __post_init__(self):
        self.persist_strict = os.getenv("TASK_PERSIST_STRICT", "false").lower() == "true"
        self.scheduler_actor_version = os.getenv("SCHEDULER_ACTOR_VERSION", self.scheduler_actor_version)
        self.scheduler_max_concurrency = int(os.getenv("SCHEDULER_MAX_CONCURRENCY", self.scheduler_max_concurrency))
        self.record_ttl_seconds = int(os.getenv("TASK_RECORD_TTL_SECONDS", self.record_ttl_seconds))
