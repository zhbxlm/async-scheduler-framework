"""scheduler_task_api._internal — bridges public API to framework internals.

This module is an implementation detail. Never import from here directly.
"""
from __future__ import annotations

import logging
import os
import sys
from typing import Any

logger = logging.getLogger(__name__)


def _ensure_framework_on_path() -> None:
    """Add monorepo src/ to sys.path when running from a development checkout."""
    candidate = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..", "src")
    )
    repo_root = os.path.dirname(candidate)
    if os.path.isdir(candidate) and repo_root not in sys.path:
        sys.path.insert(0, repo_root)


def _build_app(**kwargs: Any):
    """Build and return the Task API FastAPI application."""
    _ensure_framework_on_path()

    try:
        from src.main_tasks import app
    except ImportError as exc:
        raise RuntimeError(
            "async-scheduler framework is not installed. "
            "Install it with: pip install async-scheduler-task-api"
        ) from exc

    return app
