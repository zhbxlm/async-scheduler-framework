# async-scheduler-worker

Base classes for building Workers for [async-scheduler](https://github.com/zhbxlm/async-scheduler-framework).

**Zero required dependencies.** No framework internals. No Ray unless you want it.

## Install

```bash
pip install async-scheduler-worker

# with Ray support for distributed execution:
pip install "async-scheduler-worker[ray]"
```

## Build a Worker

```python
from scheduler_worker import BaseWorker, task_handler

class ImageResizeWorker(BaseWorker):
    capability = "image_resize"   # matches the dag_id you submit to the API

    @task_handler
    async def handle(self, task_id: str, input_data: dict) -> dict:
        url = input_data["url"]
        width = input_data.get("width", 800)
        # ... your logic ...
        return {"output_url": "https://cdn.example.com/resized.jpg"}
```

## Test Locally (no infrastructure needed)

```python
from scheduler_worker import WorkerDevKit

async def test():
    kit = WorkerDevKit(ImageResizeWorker())
    result = await kit.run_task("task-1", {"url": "https://...", "width": 400})
    assert result["status"] == "completed"
```
