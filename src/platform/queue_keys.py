"""queue_keys.py — Redis key naming module.
aligned with docs/deepwiki-reference/队列管理.md

All Redis keys use hash-tag {capability} to ensure they land on
the same Redis Cluster slot, enabling multi-key atomic Lua operations.
"""
from __future__ import annotations


def pending(capability: str) -> str:
    """Sorted Set: tasks waiting to be dequeued."""
    return f"{{queue:{capability}}}:pending"


def running(capability: str) -> str:
    """Sorted Set: tasks currently being executed (score=enqueue_ts_ms)."""
    return f"{{queue:{capability}}}:running"


def stats(capability: str) -> str:
    """Hash: per-capability stats + circuit-breaker state."""
    return f"{{queue:{capability}}}:stats"


def config(capability: str) -> str:
    """Hash: per-capability queue configuration."""
    return f"{{queue:{capability}}}:config"


def slot_running(capability: str) -> str:
    """Sorted Set: step-level concurrency slots (score=acquired_ts_ms)."""
    return f"{{slot:{capability}}}:running"


def slot_config(capability: str) -> str:
    """Hash: step-level concurrency configuration."""
    return f"{{slot:{capability}}}:config"


def callback_retry() -> str:
    """ZSET: pending callback retries (score=next_retry_ts)."""
    return "callback:retry:pending"


def callback_done(task_id: str) -> str:
    """SET: mark callback as sent (NX)."""
    return f"callback:done:{task_id}"


def callback_dlq() -> str:
    """ZSET: dead-letter queue for failed callbacks."""
    return "callback:dlq"
