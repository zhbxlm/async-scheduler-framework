"""scheduler_control_plane.app — canonical runtime factory."""
from __future__ import annotations

from typing import Any

from scheduler_control_plane.runtime import run_forever


def create_runtime(**kwargs: Any):
    return run_forever
