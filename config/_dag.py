"""config/_dag.py — DAG engine config.
aligned with docs/deepwiki-reference/配置说明.md
"""
from __future__ import annotations
import os
from dataclasses import dataclass, field


@dataclass
class DagConfig:
    http_timeout_seconds: int = field(default_factory=lambda: int(os.getenv("DAG_HTTP_TIMEOUT_SECONDS", "300")))
    max_parallelism: int = field(default_factory=lambda: int(os.getenv("DAG_MAX_PARALLELISM", "8")))
    step_timeout_seconds: int = field(default_factory=lambda: int(os.getenv("DAG_STEP_TIMEOUT_SECONDS", "300")))
    dag_timeout_seconds: int = field(default_factory=lambda: int(os.getenv("DAG_TIMEOUT_SECONDS", "3600")))
    context_ttl_seconds: int = field(default_factory=lambda: int(os.getenv("DAG_CONTEXT_TTL_SECONDS", "259200")))
    checkpoint_enabled: bool = False
    context_save_throttle_ms: int = 500
    io_executor_max_workers: int = field(default_factory=lambda: int(os.getenv("DAG_IO_EXECUTOR_MAX_WORKERS", "64")))
    dequeue_scan_limit: int = field(default_factory=lambda: int(os.getenv("DEQUEUE_SCAN_LIMIT", "50")))

    def __post_init__(self):
        self.checkpoint_enabled = os.getenv("DAG_CHECKPOINT_ENABLED", "false").lower() == "true"
