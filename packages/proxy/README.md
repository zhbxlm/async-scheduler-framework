# async-scheduler-proxy

Embed fire-and-forget async task dispatch into any existing Python service.

**Only requires `redis`** — no scheduler infrastructure knowledge needed.

## Install

```bash
pip install async-scheduler-proxy
```

## Usage

```python
from scheduler_proxy import AsyncCommandProxy, AsyncServiceProxy

# Low-level: submit a raw command string to a Redis queue
proxy = AsyncCommandProxy(redis_url="redis://localhost:6379")
job = proxy.submit("resize_image url=https://... width=800")
result = await proxy.execute("resize_image", ["https://...", "800"])

# High-level: dispatch typed tasks to capability queues
proxy = AsyncServiceProxy(redis_url="redis://localhost:6379")
await proxy.call(
    capability="image_resize",
    task_id="task-001",
    input_data={"url": "https://...", "width": 800},
)
```
