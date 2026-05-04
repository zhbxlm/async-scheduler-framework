"""Fixture: long-running worker for tests."""
import asyncio
import time


class LongRunningWorker:
    async def run(self, duration: float = 30.0) -> str:
        """Simulate long-running work."""
        for i in range(int(duration)):
            await asyncio.sleep(1)
            print(f"Working... {i+1}/{int(duration)}")
        return f"Completed {duration}s work"
