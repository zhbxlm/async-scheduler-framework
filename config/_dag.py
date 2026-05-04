# rebuilt from deepwiki-reference alignment
"""DAG orchestration config."""
from __future__ import annotations
import os
from dataclasses import dataclass, field


@dataclass
class DagConfig:
    config_dir: str = ""
    context_ttl_seconds: int = 259200          # 3 days
    http_timeout_seconds: int = 300
    ctx_locks_eviction_threshold: int = 1000
    io_executor_max_workers: int = 128
    context_save_throttle_ms: int = 500
    map_shard_save_interval: int = 10
    qps_limiter_cache_size: int = 1024
    qps_limiter_backend: str = "redis"
    streaming_executor_max_workers: int = 64
    max_parallelism: int = 8

    def __post_init__(self):
        self.config_dir                    = os.getenv("DAG_CONFIG_DIR", self.config_dir)
        self.context_ttl_seconds           = int(os.getenv("DAG_CONTEXT_TTL_SECONDS", self.context_ttl_seconds))
        self.http_timeout_seconds          = int(os.getenv("DAG_HTTP_TIMEOUT_SECONDS", self.http_timeout_seconds))
        self.ctx_locks_eviction_threshold  = int(os.getenv("DAG_CTX_LOCKS_EVICTION_THRESHOLD", self.ctx_locks_eviction_threshold))
        self.io_executor_max_workers       = int(os.getenv("DAG_IO_EXECUTOR_MAX_WORKERS", self.io_executor_max_workers))
        self.context_save_throttle_ms      = int(os.getenv("DAG_CONTEXT_SAVE_THROTTLE_MS", self.context_save_throttle_ms))
        self.map_shard_save_interval       = int(os.getenv("DAG_MAP_SHARD_SAVE_INTERVAL", self.map_shard_save_interval))
        self.qps_limiter_cache_size        = int(os.getenv("DAG_QPS_LIMITER_CACHE_SIZE", self.qps_limiter_cache_size))
        self.qps_limiter_backend           = os.getenv("DAG_QPS_LIMITER_BACKEND", self.qps_limiter_backend)
        self.streaming_executor_max_workers= int(os.getenv("DAG_STREAMING_EXECUTOR_MAX_WORKERS", self.streaming_executor_max_workers))
        self.max_parallelism               = int(os.getenv("DAG_MAX_PARALLELISM", self.max_parallelism))
