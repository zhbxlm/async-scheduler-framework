# async-scheduler-task-api

Task submission API service for [async-scheduler](https://github.com/zhbxlm/async-scheduler-framework).

Exposes only the task-facing endpoints — no ops/admin routes.

## Runtime model

This package is currently a **thin launcher package**.

It provides:
- the task-api role identity
- CLI/server entrypoint
- package-local startup/wiring

It does **not** yet contain the full scheduler business implementation by itself.
In the current architecture it runs together with:
- shared runtime/bootstrap support from `async-scheduler-runtime-core`
- monorepo business/runtime implementation provided by the framework install

### Dependency semantics

- package `dependencies` = **source-level minimum dependencies**
- runnable deployment dependencies = use role-oriented runtime profiles (for example `async-scheduler-runtime-core[task-api]`)

## Install

### Minimal package install

```bash
pip install async-scheduler-task-api
```

### Runnable role profile (recommended)

```bash
pip install 'async-scheduler-runtime-core[task-api]' async-scheduler-task-api
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
