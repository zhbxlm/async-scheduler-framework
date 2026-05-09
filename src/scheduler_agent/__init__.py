# Package entry layer for scheduler-agent.
# Full agent implementation is in src/agent/; this package re-exports the public API.
# This package is built into a standalone wheel via scripts/bundle_package.py.
"""scheduler_agent — Node Agent for async-scheduler.

    from scheduler_agent import NodeAgent

    agent = NodeAgent(
        scheduler_url="http://scheduler:8000",
        capabilities=["image_resize"],
    )
    await agent.start()
"""
from scheduler_agent.node import NodeAgent

__all__ = ["NodeAgent"]
__version__ = "1.2.0"
