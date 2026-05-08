"""scheduler_control_plane.app — control-plane application/runtime factory."""
from __future__ import annotations

from typing import Any


def create_runtime(**kwargs: Any):
    from scheduler_control_plane._internal import _build_runtime
    return _build_runtime(**kwargs)
