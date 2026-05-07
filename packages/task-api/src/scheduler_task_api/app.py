"""scheduler_task_api.app — FastAPI application factory for the Task API.

Mounts only the task-facing routes:
  POST   /tasks            submit a task
  GET    /tasks/{task_id}  get task status / result
  DELETE /tasks/{task_id}  cancel a task
  GET    /tasks            list tasks (with filters)
  GET    /health           liveness + readiness
  GET    /metrics          Prometheus metrics
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def create_app(**kwargs: Any):
    """Create and return the Task API FastAPI application.

    All framework internals are lazy-imported so this module can be
    imported without heavy dependencies being pulled in at import time.
    """
    from scheduler_task_api._internal import _build_app
    return _build_app(**kwargs)
