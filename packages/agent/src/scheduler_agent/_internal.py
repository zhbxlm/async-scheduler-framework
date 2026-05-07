"""scheduler_agent._internal — bridges public NodeAgent API to framework internals.

This module is an implementation detail. Users should never import from here directly.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scheduler_agent.node import NodeAgent

logger = logging.getLogger(__name__)

# Add the monorepo src/ to sys.path when running from a development checkout.
# In a proper pip install this isn't needed; the framework packages are on the path.
def _ensure_framework_on_path() -> None:
    candidate = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..", "src")
    candidate = os.path.normpath(candidate)
    if os.path.isdir(candidate) and candidate not in sys.path:
        sys.path.insert(0, os.path.dirname(candidate))


async def _start_agent(agent: "NodeAgent") -> None:
    _ensure_framework_on_path()
    try:
        from src.agent.server import NodeAgentServer
        from src.agent.config import AgentConfig
    except ImportError as exc:
        raise RuntimeError(
            "async-scheduler framework is not installed. "
            "Install it with: pip install async-scheduler-agent"
        ) from exc

    cfg = AgentConfig(
        scheduler_url=agent._scheduler_url,
        api_key=agent._api_key,
        capabilities=agent._capabilities,
        node_id=agent._node_id or "",
        max_concurrent_tasks=agent._max_concurrent,
        heartbeat_interval_seconds=agent._heartbeat_interval,
    )

    server = NodeAgentServer(cfg)
    agent._server = server
    logger.info(
        "NodeAgent starting: scheduler=%s capabilities=%s",
        agent._scheduler_url,
        agent._capabilities,
    )
    await server.start()
