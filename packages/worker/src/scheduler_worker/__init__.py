"""scheduler_worker — base classes for building async-scheduler Workers.

    from scheduler_worker import BaseWorker, task_handler, WorkerDevKit

    class MyWorker(BaseWorker):
        capability = "image_resize"

        @task_handler
        async def handle(self, task_id, input_data):
            return {"done": True}
"""
from scheduler_worker.base import BaseWorker, task_handler
from scheduler_worker.devkit import WorkerDevKit

__all__ = ["BaseWorker", "task_handler", "WorkerDevKit"]
__version__ = "0.1.0"
