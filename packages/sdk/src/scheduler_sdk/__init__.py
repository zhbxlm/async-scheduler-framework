"""scheduler_sdk — Python client for async-scheduler.

Public API surface:

    from scheduler_sdk import SchedulerClient

    async with SchedulerClient("http://scheduler:8000", api_key="...") as client:
        task = await client.submit_task("my_dag", input_data={"x": 1})
        result = await client.wait_for_task(task["task_id"])
"""
from scheduler_sdk.client import SchedulerClient

__all__ = ["SchedulerClient"]
__version__ = "0.1.0"
