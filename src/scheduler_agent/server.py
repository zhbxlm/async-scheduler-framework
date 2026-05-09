"""scheduler_agent.server — package entry point for the Node Agent server.

Re-exports the canonical implementation from src.agent.server and adds
the create_app alias required by the standalone package API contract.
"""
from __future__ import annotations

from src.agent.server import *  # noqa: F401,F403
from src.agent.server import create_agent_app  # noqa: F401


def create_app(node_id: str = "node-0", host: str = "0.0.0.0", agent_port: int = 8080, **kwargs) -> "FastAPI":  # type: ignore[name-defined]  # noqa: F821
    """Package-owned create_app alias for create_agent_app."""
    return create_agent_app(node_id=node_id, host=host, agent_port=agent_port, **kwargs)
