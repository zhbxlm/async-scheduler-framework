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
__version__ = "0.1.0"
