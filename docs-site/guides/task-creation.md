# Task Creation

See [Quick Start](../getting-started/quick-start.md) for a complete working example.

## Submit a Task via SDK

```python
from scheduler_sdk import SchedulerClient

async with SchedulerClient("http://localhost:8001", api_key="secret") as client:
    task = await client.submit_task(
        dag_id="train_v1",
        capability="gpu_training",
        input_data={"batch_size": 32},
        callback_url="http://my-service/callback",
    )
    print(task["task_id"])
```

## Submit via CLI

```bash
scheduler task submit \
  --dag-id train_v1 \
  --capability gpu_training \
  --input '{"batch_size": 32}'
```

## Poll for Result

```python
import asyncio

result = await client.wait_for_task(task["task_id"], poll_interval=2.0, timeout=300)
print(result["output"])
```
