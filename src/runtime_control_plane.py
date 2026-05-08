"""Shared control-plane runtime bootstrap (monorepo shim).

Wires the monorepo's ServiceContainer into the shared runtime-core
control_plane module, then re-exports lifespan/run_forever.
"""
from __future__ import annotations

from src.platform.container import ServiceContainer, set_container
from scheduler_runtime_core import control_plane as _cp

_cp.configure(
    container_factory=ServiceContainer.build_control_plane,
    set_container=set_container,
)

lifespan = _cp.lifespan
run_forever = _cp.run_forever
