"""scheduler_ops_api._internal — bridges public API to framework internals."""
from __future__ import annotations

import os, sys
from typing import Any


def _ensure_framework_on_path() -> None:
    candidate = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..", "src")
    )
    repo_root = os.path.dirname(candidate)
    if os.path.isdir(candidate) and repo_root not in sys.path:
        sys.path.insert(0, repo_root)


def _build_app(**kwargs: Any):
    _ensure_framework_on_path()
    try:
        from src.main import app
    except ImportError as exc:
        raise RuntimeError(
            "async-scheduler framework is not installed. "
            "Install it with: pip install async-scheduler-ops-api"
        ) from exc
    return app
