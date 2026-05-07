"""scheduler_ops_api.app — FastAPI application factory for the Ops API."""
from __future__ import annotations

from typing import Any


def create_app(**kwargs: Any):
    from scheduler_ops_api._internal import _build_app
    return _build_app(**kwargs)
