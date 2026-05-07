"""scheduler_agent.node — NodeAgent public interface.

Wraps the internal agent server; users only interact with this class.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class NodeAgent:
    """High-level interface for starting a scheduler worker node.

    Example::

        from scheduler_agent import NodeAgent

        agent = NodeAgent(
            scheduler_url="http://scheduler:8000",
            api_key="secret",
            capabilities=["image_resize", "data_pipeline"],
        )
        await agent.start()   # blocks; press Ctrl-C to stop
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
        self._scheduler_url = scheduler_url
        self._api_key = api_key
        self._capabilities = capabilities or []
        self._node_id = node_id
        self._max_concurrent = max_concurrent_tasks
        self._heartbeat_interval = heartbeat_interval
        self._server: Any = None

    async def start(self) -> None:
        """Start the node agent. Blocks until stopped."""
        # Lazy import keeps startup time fast and avoids importing heavy
        # framework internals (Ray, SQLAlchemy, etc.) until actually needed.
        from scheduler_agent._internal import _start_agent
        await _start_agent(self)

    async def stop(self) -> None:
        """Gracefully stop the node agent."""
        if self._server is not None:
            await self._server.stop()
