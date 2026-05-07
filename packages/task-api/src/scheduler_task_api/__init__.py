"""scheduler_task_api — Task Submission API service.

Start the server::

    scheduler-task-api --host 0.0.0.0 --port 8001

Or in Python::

    from scheduler_task_api import create_app
    import uvicorn
    uvicorn.run(create_app(), host="0.0.0.0", port=8001)
"""
from scheduler_task_api.app import create_app

__all__ = ["create_app"]
__version__ = "1.1.0"
