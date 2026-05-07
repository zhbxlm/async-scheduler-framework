"""scheduler_sdk — Python client for async-scheduler.

    from scheduler_sdk import SchedulerClient

    async with SchedulerClient("http://scheduler:8000", api_key="...") as c:
        result = await c.submit_and_wait("my_dag", {"x": 1})
"""
from scheduler_sdk.client import SchedulerClient

__all__ = ["SchedulerClient"]
__version__ = "1.1.0"
