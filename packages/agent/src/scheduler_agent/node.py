"""scheduler_agent.node — NodeAgent public interface."""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class NodeAgent:
    """Deploy a scheduler worker node.

    Example::

        agent = NodeAgent(
            scheduler_url="http://scheduler:8000",
            api_key="secret",
            capabilities=["image_resize", "data_pipeline"],
        )
        await agent.start()
    """

    def __init__(
        self,
        scheduler_url: str = "http://localhost:8000",
        api_key: str = "",
        capabilities: list[str] | None = None,
        *,
        node_id: str = "",
        max_concurrent_tasks: int = 8,
        heartbeat_interval: float = 30.0,
    ) -> None:
        self.scheduler_url = scheduler_url
        self.api_key = api_key
        self.capabilities = capabilities or []
        self.node_id = node_id
        self.max_concurrent_tasks = max_concurrent_tasks
        self.heartbeat_interval = heartbeat_interval
        self._server: Any = None

    async def start(self) -> None:
        """Start the node agent. Blocks until stopped."""
        from scheduler_agent._internal import _start_agent
        await _start_agent(self)

    async def stop(self) -> None:
        if self._server is not None:
            await self._server.stop()
