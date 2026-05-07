"""scheduler_agent._internal — bridges NodeAgent to framework internals."""
from __future__ import annotations

import os, sys, logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scheduler_agent.node import NodeAgent

logger = logging.getLogger(__name__)


def _ensure_framework_on_path() -> None:
    candidate = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..", "src")
    )
    repo_root = os.path.dirname(candidate)
    if os.path.isdir(candidate) and repo_root not in sys.path:
        sys.path.insert(0, repo_root)


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
        scheduler_url=agent.scheduler_url,
        api_key=agent.api_key,
        capabilities=agent.capabilities,
        node_id=agent.node_id,
        max_concurrent_tasks=agent.max_concurrent_tasks,
        heartbeat_interval_seconds=agent.heartbeat_interval,
    )
    server = NodeAgentServer(cfg)
    agent._server = server
    logger.info("NodeAgent starting: %s caps=%s", agent.scheduler_url, agent.capabilities)
    await server.start()
