"""DAG engine for executing workflows with dependencies.

The DAGEngine orchestrates the execution of directed acyclic graphs (DAGs)
with support for:
- Topological sorting and dependency resolution
- Parallel execution of independent nodes
- Conditional execution and error handling
- Integration with StepExecutors for flexible execution modes

This is Batch 2 of the deepwiki distributed-alignment roadmap: tighter
integration with StepExecutors and more robust execution tracking.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime
from typing import Any, Callable

from async_scheduler.dag.step_executors import (
    ExecutionMode,
    StepExecutionContext,
    StepExecutors,
    StepExecutionResult,
)

from async_scheduler.core.models import (
    DAG,
    DAGExecutionStatus,
    DAGNode,
    DAGNodeExecution,
    TaskStatus,
)

logger = logging.getLogger(__name__)


class DAGEngine:
    """Engine for executing DAGs with topological sorting and parallel execution.

    The DAGEngine is the central orchestrator for DAG execution, managing:
    - Node dependency resolution and topological ordering
    - Parallel execution of independent nodes (up to max_parallelism)
    - Conditional execution based on DAG context
    - Error handling with skip/fallback strategies
    - Integration with StepExecutors for flexible node execution

    Args:
        step_executors: Optional StepExecutors instance. If None, creates a new one.
    """

    def __init__(
        self,
        step_executors: StepExecutors | None = None,
        queue_manager: Any | None = None,
    ) -> None:
        """Initialize the DAG engine.

        Args:
            step_executors: Optional StepExecutors instance.
            queue_manager: Optional QueueManager for capability concurrency slot control (G5).
        """
        self._running_executions: dict[str, asyncio.Task[None]] = {}
        self._cancellation_events: dict[str, asyncio.Event] = {}
        self._step_executors = step_executors or StepExecutors()
        self._execution_results: dict[str, dict[str, StepExecutionResult]] = {}  # dag_id -> {node_id: result}
        self._queue_manager = queue_manager  # G5: capability concurrency slot control

    def _build_node_index(self, dag: DAG) -> dict[str, DAGNode]:
        """Build a lookup dictionary for nodes by ID."""
        return {node.id: node for node in dag.nodes}

    def _topological_sort(self, dag: DAG) -> list[str]:
        """Return nodes in topological order using Kahn's algorithm."""
        node_index = self._build_node_index(dag)
        in_degree: dict[str, int] = defaultdict(int)
        adjacency_list: dict[str, set[str]] = defaultdict(set)

        # Build graph
        for node in dag.nodes:
            in_degree[node.id] = len(node.depends_on)
            for dep in node.depends_on:
                adjacency_list[dep].add(node.id)

        # Find nodes with no dependencies
        queue = deque([node_id for node_id in in_degree if in_degree[node_id] == 0])
        result: list[str] = []

        while queue:
            node_id = queue.popleft()
            result.append(node_id)

            for neighbor in adjacency_list[node_id]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(result) != len(dag.nodes):
            raise ValueError("DAG contains a cycle")

        return result

    def _get_ready_nodes(self, dag: DAG) -> list[DAGNode]:
        """Get nodes that are ready to execute (all dependencies satisfied)."""
        ready_nodes: list[DAGNode] = []

        for node in dag.nodes:
            execution = dag.node_executions.get(node.id)
            if execution and execution.status != TaskStatus.PENDING:
                continue  # Already executed or skipped

            all_deps_complete = True
            for dep_id in node.depends_on:
                dep_exec = dag.node_executions.get(dep_id, DAGNodeExecution(node_id=dep_id))
                dep_ok = dep_exec.status == TaskStatus.SUCCESS or dep_exec.skipped
                if not dep_ok:
                    all_deps_complete = False
                    break

            if all_deps_complete:
                ready_nodes.append(node)

        return ready_nodes

    def _should_execute_node(self, node: DAGNode, context: dict[str, Any]) -> bool:
        """Evaluate condition for conditional execution."""
        if not node.condition:
            return True

        try:
            # Safe evaluation of condition
            allowed_names = {"context": context, "True": True, "False": False, "None": None}
            return eval(node.condition, {"__builtins__": {}}, allowed_names)  # noqa: S307
        except Exception:
            return True  # Default to execute if condition fails

    async def _execute_node(
        self,
        dag: DAG,
        node: DAGNode,
        handler: Callable[[str, dict[str, Any]], Any],
        cancel_event: asyncio.Event,
    ) -> None:
        """Execute a single DAG node using StepExecutors.

        Args:
            dag: The DAG being executed.
            node: The node to execute.
            handler: The handler function for the task type.
            cancel_event: Event for checking cancellation.
        """
        execution = dag.node_executions.get(node.id)

        if execution is None:
            execution = DAGNodeExecution(node_id=node.id)
            dag.node_executions[node.id] = execution

        # Check if we should execute this node
        if not self._should_execute_node(node, dag.context):
            execution.skipped = True
            execution.skip_reason = f"Condition '{node.condition}' evaluated to False"
            execution.status = TaskStatus.SUCCESS
            execution.completed_at = datetime.utcnow()
            return

        # Check cancellation
        if cancel_event.is_set():
            execution.status = TaskStatus.CANCELLED
            execution.completed_at = datetime.utcnow()
            return

        # G5: acquire capability concurrency slot before execution
        # node.task_type is used as the capability key; falls back gracefully if qm absent
        node_capability = node.payload.get("capability", node.task_type or "default")
        slot_acquired = False
        if self._queue_manager is not None:
            try:
                slot_acquired = await self._acquire_capability_slot(node_capability)
            except Exception as _e:
                logger.warning("DAGEngine: capability slot acquire failed cap=%s: %s", node_capability, _e)

        execution.status = TaskStatus.RUNNING
        execution.started_at = datetime.utcnow()

        try:
            work_payload = {**node.payload, **dag.context}
            if cancel_event.is_set():
                raise asyncio.CancelledError()

            execution_mode = ExecutionMode(node.payload.get("execution_mode", "sync"))

            # Create step execution context
            step_ctx = StepExecutionContext(
                task_type=node.task_type,
                payload=work_payload,
                timeout_seconds=node.timeout_seconds,
                flask_url=node.payload.get("flask_url"),
                step_id=node.id,
                dag_id=dag.id,
            )

            # Execute using StepExecutors — with retry support (Bug1 fix)
            last_error: str | None = None
            result = None
            max_attempts = max(1, node.max_retries + 1)
            for attempt in range(max_attempts):
                try:
                    result = await self._step_executors.execute(
                        execution_mode,
                        step_ctx,
                        handler,
                    )
                    if result.status.value == "completed":
                        break  # success, stop retrying
                    if result.status.value in ("timeout", "cancelled"):
                        break  # non-retryable
                    # failed result — retry if attempts remain
                    last_error = result.error or "Unknown error"
                    if attempt < max_attempts - 1:
                        import random as _random
                        wait = min(2 ** attempt, 30) + _random.uniform(0, 0.5)
                        logger.warning(
                            "DAGEngine: node=%s attempt=%d/%d failed, retrying in %.1fs: %s",
                            node.id, attempt + 1, max_attempts, wait, last_error,
                        )
                        execution.retry_count = attempt + 1
                        await asyncio.sleep(wait)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    last_error = str(exc)
                    if attempt < max_attempts - 1:
                        import random as _random
                        wait = min(2 ** attempt, 30) + _random.uniform(0, 0.5)
                        logger.warning(
                            "DAGEngine: node=%s attempt=%d/%d raised, retrying in %.1fs: %s",
                            node.id, attempt + 1, max_attempts, wait, exc,
                        )
                        execution.retry_count = attempt + 1
                        await asyncio.sleep(wait)
                    else:
                        raise  # exhausted retries — propagate to outer except

            # Store execution result
            if dag.id not in self._execution_results:
                self._execution_results[dag.id] = {}  
            if result is not None:
                self._execution_results[dag.id][node.id] = result

            # Map StepExecutionResult to TaskStatus
            if result.status.value == "completed":
                execution.status = TaskStatus.SUCCESS
                execution.result = result.value if isinstance(result.value, dict) else {"value": result.value}
                execution.completed_at = datetime.utcnow()

                # Merge result into context
                if isinstance(result.value, dict):
                    dag.context.update(result.value)
            elif result.status.value == "timeout":
                execution.status = TaskStatus.TIMEOUT
                execution.error_message = f"Node timed out after {node.timeout_seconds} seconds"
                execution.completed_at = datetime.utcnow()

                if node.on_failure == "skip":
                    execution.skipped = True
                    execution.skip_reason = "Node failed with on_failure=skip"
            elif result.status.value == "cancelled":
                execution.status = TaskStatus.CANCELLED
                execution.completed_at = datetime.utcnow()
            else:  # failed
                execution.status = TaskStatus.FAILED
                execution.error_message = result.error or "Unknown error"
                execution.completed_at = datetime.utcnow()

                # Handle failure based on on_failure setting
                if node.on_failure == "skip":
                    execution.skipped = True
                    execution.skip_reason = "Node failed with on_failure=skip"
                elif node.on_failure == "fallback" and node.fallback_payload:
                    # Bug2 fix: fallback means "treat as success with fallback data"
                    # Mark as skipped (SUCCESS-equivalent) so downstream can proceed
                    dag.context.update(node.fallback_payload)
                    execution.result = node.fallback_payload
                    execution.status = TaskStatus.SUCCESS
                    execution.skipped = True
                    execution.skip_reason = f"fallback applied after {execution.error_message}"

        except asyncio.CancelledError:
            execution.status = TaskStatus.CANCELLED
            execution.completed_at = datetime.utcnow()
            logger.debug(f"Node {node.id} cancelled")

        except Exception as e:
            execution.status = TaskStatus.FAILED
            execution.error_message = str(e)
            execution.completed_at = datetime.utcnow()
            logger.exception(f"Node {node.id} execution failed: {e}")

            # Handle failure based on on_failure setting
            if node.on_failure == "skip":
                execution.skipped = True
                execution.skip_reason = "Node failed with on_failure=skip"
            elif node.on_failure == "fallback" and node.fallback_payload:
                # Bug2 fix: apply fallback data, mark as SUCCESS/skipped so downstream proceeds
                dag.context.update(node.fallback_payload)
                execution.result = node.fallback_payload
                execution.status = TaskStatus.SUCCESS
                execution.skipped = True
                execution.skip_reason = f"fallback applied after exception: {e}"

        finally:
            # G5: release capability concurrency slot and record result for circuit breaker
            if self._queue_manager is not None and slot_acquired:
                success = execution.status == TaskStatus.SUCCESS
                await self._release_capability_slot(node_capability, success=success)

    async def _acquire_capability_slot(self, capability: str) -> bool:
        """G5: Acquire a concurrency slot for a capability.

        Calls queue_manager.acquire_concurrency_slot if available,
        otherwise records a start via circuit breaker.
        Returns True if slot was acquired (should be released in finally).
        """
        if hasattr(self._queue_manager, 'acquire_concurrency_slot'):
            try:
                return await self._queue_manager.acquire_concurrency_slot(capability)
            except Exception:
                return False
        # Fallback: just mark circuit as being used (no-op if method absent)
        return False

    async def _release_capability_slot(
        self,
        capability: str,
        success: bool = True,
    ) -> None:
        """G5: Release capability concurrency slot and record result.

        Calls:
          - queue_manager.record_result(capability, success) for circuit breaker
          - queue_manager.try_recover_concurrent(capability) to restore slot
        """
        if self._queue_manager is None:
            return
        if hasattr(self._queue_manager, 'record_result'):
            try:
                await self._queue_manager.record_result(capability, success)
            except Exception as e:
                logger.debug("DAGEngine: record_result failed cap=%s: %s", capability, e)
        if hasattr(self._queue_manager, 'try_recover_concurrent'):
            try:
                await self._queue_manager.try_recover_concurrent(capability)
            except Exception as e:
                logger.debug("DAGEngine: try_recover_concurrent failed cap=%s: %s", capability, e)

    async def execute(
        self,
        dag: DAG,
        handler: Callable[[str, dict[str, Any]], Any],
    ) -> DAG:
        """Execute the DAG with parallel execution of ready nodes."""
        dag.status = DAGExecutionStatus.RUNNING
        dag.started_at = datetime.utcnow()

        # Initialize node executions
        for node in dag.nodes:
            if node.id not in dag.node_executions:
                dag.node_executions[node.id] = DAGNodeExecution(node_id=node.id)

        # Create cancellation event for this DAG
        cancel_event = asyncio.Event()
        self._cancellation_events[dag.id] = cancel_event

        try:
            self._topological_sort(dag)
            while True:
                if cancel_event.is_set():
                    for node in dag.nodes:
                        execution = dag.node_executions[node.id]
                        if execution.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
                            execution.status = TaskStatus.CANCELLED
                            execution.completed_at = datetime.utcnow()
                    break

                ready_nodes = self._get_ready_nodes(dag)

                if not ready_nodes:
                    # Check if we're done
                    pending_nodes = [
                        n for n in dag.nodes
                        if dag.node_executions[n.id].status in (TaskStatus.PENDING, TaskStatus.RUNNING)
                    ]

                    if not pending_nodes:
                        break  # All nodes processed

                    blocked_nodes = [
                        n for n in dag.nodes
                        if dag.node_executions[n.id].status == TaskStatus.PENDING
                    ]
                    if blocked_nodes:
                        for node in blocked_nodes:
                            dep_failed = False
                            for dep_id in node.depends_on:
                                dep_exec = dag.node_executions.get(dep_id)
                                if dep_exec and dep_exec.status in (
                                    TaskStatus.FAILED,
                                    TaskStatus.CANCELLED,
                                    TaskStatus.TIMEOUT,
                                ) and not dep_exec.skipped:
                                    dep_failed = True
                                    break
                            if dep_failed:
                                dag.node_executions[node.id].status = TaskStatus.CANCELLED
                                dag.node_executions[node.id].completed_at = datetime.utcnow()
                        continue

                    await asyncio.sleep(0.1)
                    continue

                # Execute ready nodes in parallel (up to max_parallelism)
                semaphore = asyncio.Semaphore(dag.max_parallelism)

                async def execute_with_semaphore(node: DAGNode) -> None:
                    async with semaphore:
                        await self._execute_node(dag, node, handler, cancel_event)

                await asyncio.gather(
                    *(execute_with_semaphore(node) for node in ready_nodes),
                    return_exceptions=False,
                )

                if cancel_event.is_set():
                    for node in dag.nodes:
                        execution = dag.node_executions[node.id]
                        if execution.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
                            execution.status = TaskStatus.CANCELLED
                            execution.completed_at = datetime.utcnow()
                    break

        finally:
            self._cancellation_events.pop(dag.id, None)

        # Determine final status
        all_success = all(
            dag.node_executions[n.id].status == TaskStatus.SUCCESS or
            dag.node_executions[n.id].skipped
            for n in dag.nodes
        )

        any_failed = any(
            dag.node_executions[n.id].status == TaskStatus.FAILED and not dag.node_executions[n.id].skipped
            for n in dag.nodes
        )

        any_cancelled = any(
            dag.node_executions[n.id].status == TaskStatus.CANCELLED
            for n in dag.nodes
        )

        if any_failed:
            dag.status = DAGExecutionStatus.FAILED
        elif any_cancelled:
            dag.status = DAGExecutionStatus.CANCELLED
        elif all_success:
            dag.status = DAGExecutionStatus.SUCCESS
        else:
            dag.status = DAGExecutionStatus.PARTIAL

        dag.completed_at = datetime.utcnow()
        dag.updated_at = datetime.utcnow()

        # Clear execution results to free memory
        self.clear_execution_results(dag.id)

        return dag

    async def cancel(self, dag_id: str) -> bool:
        """Cancel a running DAG execution."""
        if dag_id in self._cancellation_events:
            self._cancellation_events[dag_id].set()
            return True
        return False

    def is_running(self, dag_id: str) -> bool:
        """Check if a DAG is currently running."""
        return dag_id in self._cancellation_events

    def get_node_execution_result(self, dag_id: str, node_id: str) -> StepExecutionResult | None:
        """Get the StepExecutionResult for a specific node.

        Args:
            dag_id: ID of the DAG.
            node_id: ID of the node.

        Returns:
            The StepExecutionResult, or None if not found.
        """
        return self._execution_results.get(dag_id, {}).get(node_id)

    def get_dag_execution_results(self, dag_id: str) -> dict[str, StepExecutionResult]:
        """Get all StepExecutionResults for a DAG.

        Args:
            dag_id: ID of the DAG.

        Returns:
            Dict mapping node IDs to StepExecutionResults.
        """
        return self._execution_results.get(dag_id, {}).copy()

    def clear_execution_results(self, dag_id: str) -> None:
        """Clear execution results for a DAG.

        Args:
            dag_id: ID of the DAG.
        """
        self._execution_results.pop(dag_id, None)


async def default_dag_handler(task_type: str, payload: dict[str, Any]) -> Any:
    """Default DAG node handler."""

    async def _inner() -> Any:
        # Simulate work based on task type
        work_duration = payload.get("sleep_seconds", 1)
        if "fast" in task_type.lower():
            work_duration = 0.1
        elif "slow" in task_type.lower():
            work_duration = 2

        await asyncio.sleep(work_duration)

        return {
            "task_type": task_type,
            "result": f"Completed {task_type}",
            "value": payload.get("value", 42),
        }

    return await _inner()
