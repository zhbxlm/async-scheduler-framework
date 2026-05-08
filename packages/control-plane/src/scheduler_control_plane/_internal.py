"""scheduler_control_plane._internal — package-local runtime integration.

Wires the monorepo ServiceContainer into runtime-core's control_plane module
at import time, so the packaged entrypoint is fully self-contained.
"""
from __future__ import annotations

from typing import Any

from src.platform.container import ServiceContainer, set_container
from scheduler_runtime_core import control_plane as _cp

_cp.configure(
    container_factory=ServiceContainer.build_control_plane,
    set_container=set_container,
)


def _build_runtime(**kwargs: Any):
    try:
        from scheduler_runtime_core.control_plane import get_run_forever
    except ImportError as exc:
        raise RuntimeError(
            "async-scheduler runtime core is not available."
        ) from exc

    return get_run_forever()
