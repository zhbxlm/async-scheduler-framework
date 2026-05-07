"""scheduler_agent — Node Agent for async-scheduler.

Deploy a worker node with a single command:

    scheduler-agent start --scheduler-url http://scheduler:8000 \\
                          --capabilities image_resize,data_pipeline

No need to understand framework internals.
"""
from scheduler_agent.node import NodeAgent

__all__ = ["NodeAgent"]
__version__ = "0.1.0"
