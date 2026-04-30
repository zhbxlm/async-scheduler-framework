"""Example workers for common task types."""

import asyncio
import logging
import random
from typing import Any

from async_scheduler.worker.base import TaskWorker

logger = logging.getLogger(__name__)


class EchoWorker(TaskWorker):
    """Worker that echoes the input payload."""

    def __init__(self, queue_manager, executor) -> None:
        super().__init__("echo", queue_manager, executor)

    async def process(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Echo the input payload."""
        logger.info(f"EchoWorker processing: {payload}")
        await asyncio.sleep(0.1)
        return {
            "worker": "echo",
            "input": payload,
            "output": payload,
        }


class ComputeWorker(TaskWorker):
    """Worker that performs mathematical computations."""

    def __init__(self, queue_manager, executor) -> None:
        super().__init__("compute", queue_manager, executor)

    async def process(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Perform mathematical operations."""
        operation = payload.get("operation", "add")
        a = payload.get("a", 0)
        b = payload.get("b", 0)

        logger.info(f"ComputeWorker processing: {operation}({a}, {b})")

        await asyncio.sleep(0.2)

        result: Any = None

        if operation == "add":
            result = a + b
        elif operation == "subtract":
            result = a - b
        elif operation == "multiply":
            result = a * b
        elif operation == "divide":
            result = a / b if b != 0 else float("inf")
        elif operation == "power":
            result = a ** b
        elif operation == "modulo":
            result = a % b
        elif operation == "factorial":
            result = 1
            for i in range(1, a + 1):
                result *= i
        else:
            result = {"error": f"Unknown operation: {operation}"}

        return {
            "worker": "compute",
            "operation": operation,
            "operands": [a, b],
            "result": result,
        }


class DataProcessingWorker(TaskWorker):
    """Worker for data processing tasks."""

    def __init__(self, queue_manager, executor) -> None:
        super().__init__("data_processing", queue_manager, executor)

    async def process(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Process data."""
        data = payload.get("data", [])
        operation = payload.get("operation", "transform")

        logger.info(f"DataProcessingWorker processing: {operation} on {len(data)} items")

        await asyncio.sleep(0.3)

        result: Any = None

        if operation == "sum":
            result = sum(data) if all(isinstance(x, (int, float)) for x in data) else None
        elif operation == "avg":
            result = sum(data) / len(data) if data and all(isinstance(x, (int, float)) for x in data) else None
        elif operation == "count":
            result = len(data)
        elif operation == "filter_positive":
            result = [x for x in data if x > 0]
        elif operation == "filter_negative":
            result = [x for x in data if x < 0]
        elif operation == "transform":
            factor = payload.get("factor", 2)
            result = [x * factor for x in data]
        else:
            result = {"error": f"Unknown operation: {operation}"}

        return {
            "worker": "data_processing",
            "operation": operation,
            "input_count": len(data),
            "result": result,
        }


class IOWorker(TaskWorker):
    """Worker that simulates I/O operations."""

    def __init__(self, queue_manager, executor) -> None:
        super().__init__("io", queue_manager, executor)

    async def process(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Simulate I/O operation."""
        operation = payload.get("operation", "read")
        duration = payload.get("duration", 1.0)

        logger.info(f"IOWorker processing: {operation} with duration {duration}s")

        await asyncio.sleep(duration)

        result = {
            "worker": "io",
            "operation": operation,
            "duration": duration,
        }

        if operation == "read":
            result["data"] = {"id": random.randint(1, 1000), "value": random.random()}
        elif operation == "write":
            result["written"] = True
            result["size"] = payload.get("size", 1024)
        elif operation == "download":
            result["downloaded"] = True
            result["bytes"] = payload.get("bytes", 1024)
        elif operation == "upload":
            result["uploaded"] = True
            result["bytes"] = payload.get("bytes", 1024)

        return result


class EmailWorker(TaskWorker):
    """Worker that simulates sending emails."""

    def __init__(self, queue_manager, executor) -> None:
        super().__init__("email", queue_manager, executor)

    async def process(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Simulate sending an email."""
        to = payload.get("to", "user@example.com")
        subject = payload.get("subject", "No subject")
        body = payload.get("body", "")

        logger.info(f"EmailWorker sending email to {to}")

        await asyncio.sleep(0.5)

        return {
            "worker": "email",
            "to": to,
            "subject": subject,
            "sent": True,
            "message_id": f"msg-{random.randint(10000, 99999)}",
        }


def create_default_workers(queue_manager, executor, num_workers: int = 2) -> list[TaskWorker]:
    """Create a default set of workers."""
    workers: list[TaskWorker] = []

    for i in range(num_workers):
        workers.extend([
            EchoWorker(queue_manager, executor),
            ComputeWorker(queue_manager, executor),
            DataProcessingWorker(queue_manager, executor),
            IOWorker(queue_manager, executor),
            EmailWorker(queue_manager, executor),
        ])

    return workers
