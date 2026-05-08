"""scheduler_ops_api._internal — wires the monorepo Ops API app into runtime-core."""
from __future__ import annotations

from typing import Any

from scheduler_runtime_core.app_factory import configure, build_app
from scheduler_runtime_core.logging_config import configure_logging

configure_logging()


def _ops_api_factory():
    from src.main import app  # late import — heavy, only at serve time
    return app


configure("ops-api", _ops_api_factory)


def _build_app(**kwargs: Any):
    return build_app("ops-api")
