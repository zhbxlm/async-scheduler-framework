"""Task executor with retries, timeout, and cancellation support."""

import asyncio
import contextlib
import inspect
import random
from typing import Any, Callable

from async_scheduler.core.models import Task, TaskStatus


class ExecutionResult:
    """Result of a task execution."""

    def __init__(
        self,
        success: bool,
        result: Any | None = None,
        error: Exception | None = None,
        should_retry: bool = False,
    ) -> None:
        self.success = success
        self.result = result
        self.error = error
        self.should_retry = should_retry


class TaskExecutor:
    """Executes tasks with retries, timeouts, and cancellation support."""

    def __init__(self) -> None:
        """Initialize the task executor."""
        self._running_tasks: dict[str, asyncio.Task[ExecutionResult]] = {}
        self._cancellation_events: dict[str, asyncio.Event] = {}

    async def execute(
        self,
        task: Task,
        handler: Callable[[dict[str, Any]], Any],
        lease_lost_event: asyncio.Event | None = None,
    ) -> ExecutionResult:
        """Execute a task with retries and timeout handling."""
        result = ExecutionResult(success=False)
        _is_coro = inspect.iscoroutinefunction(handler)  # cache once per execute() call

        for attempt in range(task.max_retries + 1):
            if task.status == TaskStatus.CANCELLED:
                result.error = Exception("Task was cancelled")
                break

            # Update retry count for tracking
            task.retry_count = attempt

            # Create cancellation event for this execution
            cancel_event = asyncio.Event()

            async def _wrapped_handler() -> Any:
                try:
                    if _is_coro:
                        return await handler(task.payload)
                    rv = handler(task.payload)
                    if inspect.isawaitable(rv):
                        return await rv
                    return rv
                except Exception as e:
                    return e

            try:
                # Execute with timeout
                coro = _wrapped_handler()
                wait_task = asyncio.create_task(coro)

                # Store for potential cancellation
                self._running_tasks[task.id] = wait_task
                self._cancellation_events[task.id] = cancel_event

                cancel_wait_task = asyncio.create_task(cancel_event.wait())

                # P0-TODO-3: also watch for lease_lost_event
                lease_lost_wait_task: asyncio.Task | None = None
                if lease_lost_event is not None:
                    lease_lost_wait_task = asyncio.create_task(lease_lost_event.wait())
                    wait_set = [wait_task, cancel_wait_task, lease_lost_wait_task]
                else:
                    wait_set = [wait_task, cancel_wait_task]

                done, pending = await asyncio.wait(
                    wait_set,
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=task.timeout_seconds,
                )

                # Check lease_lost first
                if lease_lost_event is not None and lease_lost_event.is_set():
                    if not wait_task.done():
                        wait_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await wait_task
                    if lease_lost_wait_task and not lease_lost_wait_task.done():
                        lease_lost_wait_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await lease_lost_wait_task
                    result.error = RuntimeError("lease_lost")
                    result.should_retry = False
                    break

                if cancel_event.is_set() or cancel_wait_task in done:
                    if not wait_task.done():
                        wait_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await wait_task
                    if lease_lost_wait_task and not lease_lost_wait_task.done():
                        lease_lost_wait_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await lease_lost_wait_task
                    raise asyncio.CancelledError("Task was cancelled")

                if wait_task.done():
                    exec_result = wait_task.result()

                    if isinstance(exec_result, Exception):
                        raise exec_result

                    result.success = True
                    result.result = exec_result
                    if not cancel_wait_task.done():
                        cancel_wait_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await cancel_wait_task
                    if lease_lost_wait_task and not lease_lost_wait_task.done():
                        lease_lost_wait_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await lease_lost_wait_task
                    break

                # Timeout occurred
                if not wait_task.done():
                    wait_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await wait_task
                    if not cancel_wait_task.done():
                        cancel_wait_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await cancel_wait_task
                    if lease_lost_wait_task and not lease_lost_wait_task.done():
                        lease_lost_wait_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await lease_lost_wait_task

                    raise TimeoutError(f"Task timed out after {task.timeout_seconds} seconds")

            except asyncio.CancelledError:
                result.error = Exception("Task was cancelled")
                break

            except TimeoutError as e:
                result.error = e
                if attempt < task.max_retries:
                    base = min(2**attempt, 60); await asyncio.sleep(base + random.uniform(0, base * 0.2))  # Exponential backoff with jitter
                    continue
                break

            except Exception as e:
                result.error = e
                if attempt < task.max_retries:
                    base = min(2**attempt, 60); await asyncio.sleep(base + random.uniform(0, base * 0.2))  # Exponential backoff with jitter
                    continue
                break

            finally:
                if 'cancel_wait_task' in locals() and not cancel_wait_task.done():
                    cancel_wait_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await cancel_wait_task
                self._running_tasks.pop(task.id, None)
                self._cancellation_events.pop(task.id, None)

        if result.success:
            result.should_retry = False
        elif isinstance(result.error, asyncio.CancelledError) or (
            result.error is not None and "cancelled" in str(result.error).lower()
        ):
            result.should_retry = False
        else:
            # execute() already consumes the full retry budget internally.
            # When it returns a failed result, there is no remaining in-executor
            # retry opportunity, so the caller must not requeue solely based on
            # max_retries being configured.
            result.should_retry = False
        return result

    async def cancel(self, task_id: str) -> bool:
        """Cancel a running task."""
        if task_id in self._running_tasks:
            self._running_tasks[task_id].cancel()

        if task_id in self._cancellation_events:
            self._cancellation_events[task_id].set()

        return task_id in self._running_tasks

    def is_running(self, task_id: str) -> bool:
        """Check if a task is currently running."""
        return task_id in self._running_tasks

    def get_running_count(self) -> int:
        """Get the count of currently running tasks."""
        return len(self._running_tasks)


async def default_task_handler(payload: dict[str, Any]) -> Any:
    """Default task handler that simply returns the payload."""

    async def _inner() -> Any:
        # Simulate some work
        await asyncio.sleep(payload.get("sleep_seconds", 0))
        return {"status": "completed", "payload": payload}

    return await _inner()
