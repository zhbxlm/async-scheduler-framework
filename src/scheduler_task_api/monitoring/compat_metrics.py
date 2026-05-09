"""Compatibility metrics helpers used by task-api platform components."""
from __future__ import annotations

from src.monitoring.metrics import REQUEST_COUNT  # noqa: F401


def record_task_creation(capability: str, priority: str) -> None:
    # placeholder compatibility hook; keep best-effort semantics
    return None


def update_queue_metrics(capability: str, pending_count: int, running_count: int, max_concurrent: int | None = None) -> None:
    return None
