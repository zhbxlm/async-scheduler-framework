"""Example: custom worker implementation."""
from src.workload.base_worker_actor import BaseWorkerActor
import asyncio


class CustomWorker(BaseWorkerActor):
    async def execute(self, payload: dict) -> dict:
        """Custom execution logic."""
        print(f"Executing custom task: {payload}")
        return {"status": "success", "output": f"Processed {payload.get('data')}"}


async def main():
    worker = CustomWorker()
    result = await worker.execute({"data": "test"})
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
