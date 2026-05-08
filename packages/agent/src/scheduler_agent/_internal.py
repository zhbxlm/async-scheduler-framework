"""scheduler_agent._internal — wires NodeAgent to the monorepo agent server."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from scheduler_runtime_core.logging_config import configure_logging

configure_logging()

if TYPE_CHECKING:
    from scheduler_agent.node import NodeAgent

logger = logging.getLogger(__name__)


async def _start_agent(agent: "NodeAgent") -> None:
    try:
        from src.agent.server import NodeAgentServer
        from src.agent.config import AgentConfig
    except ImportError as exc:
        raise RuntimeError(
            "async-scheduler agent framework is not installed. "
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
