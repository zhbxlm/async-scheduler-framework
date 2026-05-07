"""scheduler_ops_api — Ops/Admin API service for async-scheduler.

Start the server::

    scheduler-ops-api --host 0.0.0.0 --port 8000

Or in Python::

    from scheduler_ops_api import create_app
    import uvicorn
    uvicorn.run(create_app(), host="0.0.0.0", port=8000)
"""
from scheduler_ops_api.app import create_app

__all__ = ["create_app"]
__version__ = "1.1.0"
