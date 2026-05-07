# Task API Reference

# Ray Async API Reference

## Overview

- **Ops API** (port 8000): Management endpoints
- **Task API** (port 8001): Task submission and query
- **Auth**: Bearer token in Authorization header
- **Content-Type**: application/json

## Task API (port 8001)

### POST /tasks
Create a new task.

Request:
```json
{
  "dag_id": "my_dag",
  "input_data": {"key": "value"},
  "priority": "normal",
  "tenant_id": "default",
  "idempotency_key": "optional",
  "callback_url": "https://example.com/callback"
}
```

Response 201:
```json
{
  "task_id": "task_abc123",
  "status": "pending",
  "accepted": true,
  "queue_position": 0
}
```

### GET /tasks
List tasks. Query: status, capability, dag_id, tenant_id, limit, offset.

Response 200:
```json
{
  "tasks": [{"task_id": "...", "status": "...", "created_at": "..."}],
  "total": 100
}
```

### GET /tasks/{task_id}
Task detail. Returns TaskInfo with status, dag_id, timestamps, output_data.

### GET /tasks/{task_id}/result
Get result. 202 if not ready, 200 with output_data when done.

### DELETE /tasks/{task_id}
Cancel pending/running task.

