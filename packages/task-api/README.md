# async-scheduler-task-api

Task submission API service for [async-scheduler](https://github.com/zhbxlm/async-scheduler-framework).

Exposes only the task-facing endpoints — no ops/admin routes.

## Install

```bash
pip install async-scheduler-task-api
```

## Start

```bash
# CLI
scheduler-task-api --host 0.0.0.0 --port 8001

# Docker / environment variables
TASK_API_HOST=0.0.0.0 TASK_API_PORT=8001 scheduler-task-api
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/tasks` | Submit a new task |
| `GET` | `/tasks/{task_id}` | Get task status / result |
| `DELETE` | `/tasks/{task_id}` | Cancel a task |
| `GET` | `/tasks` | List tasks (filterable) |
| `GET` | `/health` | Liveness + readiness |
| `GET` | `/metrics` | Prometheus metrics |

## Python API

```python
from scheduler_task_api import create_app
import uvicorn

app = create_app()
uvicorn.run(app, host="0.0.0.0", port=8001)
```
