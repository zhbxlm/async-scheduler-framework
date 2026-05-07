"""scheduler_proxy — embed async task dispatch into any existing service.

Public API surface:

    from scheduler_proxy import AsyncCommandProxy, AsyncServiceProxy

    # Fire-and-forget: submit a command to Redis queue
    proxy = AsyncCommandProxy(redis_url="redis://localhost:6379")
    job = proxy.submit("resize_image", args=["https://img.example.com/photo.jpg", "800x600"])
    result = await proxy.execute("resize_image", ["https://img.example.com/photo.jpg", "800x600"])

    # Service-level: route calls to capability queues
    proxy = AsyncServiceProxy(redis_url="redis://localhost:6379")
    await proxy.call("image_resize", task_id="t-001", input_data={"url": "..."})
"""
from scheduler_proxy.command import AsyncCommandProxy
from scheduler_proxy.service import AsyncServiceProxy

__all__ = ["AsyncCommandProxy", "AsyncServiceProxy"]
__version__ = "0.1.0"
