# Package entry layer for scheduler-task-api.
# Source of truth lives in src/task/ and src/{common,models,platform,services}/.
# This package is built into a standalone wheel via scripts/bundle_package.py.
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
__version__ = "1.2.0"
