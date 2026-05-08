"""Generic app-factory registry for independently packaged API roles.

Each deployable package (task-api, ops-api, …) calls configure() at import
time to register its FastAPI app factory.  The _build_app() helper then
invokes the factory on demand, keeping all heavy imports lazy.

Usage (in a package _internal.py)::

    from scheduler_runtime_core.app_factory import configure, build_app

    def _my_factory():
        from src.main_tasks import app
        return app

    configure("task-api", _my_factory)
"""
from __future__ import annotations

from typing import Any, Callable

_registry: dict[str, Callable[[], Any]] = {}


def configure(role: str, factory: Callable[[], Any]) -> None:
    """Register an app factory for *role* (e.g. "task-api")."""
    _registry[role] = factory


def build_app(role: str, **kwargs: Any) -> Any:
    """Invoke the registered factory for *role* and return the app.

    Falls back to a RuntimeError with an actionable message when the
    factory was never registered (e.g. missing install).
    """
    factory = _registry.get(role)
    if factory is None:
        raise RuntimeError(
            f"No app factory registered for role '{role}'. "
            f"Ensure the package that provides this role is installed."
        )
    return factory()
