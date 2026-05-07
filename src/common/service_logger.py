from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("platform.service")


def log_service_event(
    service: str,
    operation: str,
    *,
    task_id: str | None = None,
    actor: str | None = None,
    outcome: str = "ok",
    **extra: Any,
) -> None:
    """Emit a structured JSON log entry for a service-layer event."""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "operation": operation,
        "outcome": outcome,
    }
    if task_id is not None:
        entry["task_id"] = task_id
    if actor is not None:
        entry["actor"] = actor
    entry.update(extra)
    logger.info(json.dumps(entry, ensure_ascii=False))
