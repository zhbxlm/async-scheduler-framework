"""Example: async proxy worker."""
from src.workload.async_proxy_worker import AsyncProxyWorker
import asyncio


async def main():
    worker = AsyncProxyWorker(proxy_endpoint="http://localhost:8080")
    
    # Example task
    task = {
        "task_id": "example_001",
        "payload": {"command": "echo hello"},
    }
    
    result = await worker.execute_task(task)
    print(f"Result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
