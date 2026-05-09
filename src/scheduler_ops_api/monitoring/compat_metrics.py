"""Compatibility metrics helpers used by ops-api platform components."""
from __future__ import annotations


def record_task_creation(capability: str, priority: str) -> None:
    return None


def update_queue_metrics(capability: str, pending_count: int, running_count: int, max_concurrent: int | None = None) -> None:
    return None
