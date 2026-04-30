"""DAG engine for executing workflows with dependencies."""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from datetime import datetime
from typing import Any, Callable

from async_scheduler.dag.step_executors import ExecutionMode, StepExecutionContext, StepExecutors

from async_scheduler.core.models import (
    DAG,
    DAGExecutionStatus,
    DAGNode,
    DAGNodeExecution,
    TaskStatus,
)


class DAGEngine:
    """Engine for executing DAGs with topological sorting and parallel execution."""

    def __init__(self) -> None:
        """Initialize the DAG engine."""
        self._running_executions: dict[str, asyncio.Task[None]] = {}
        self._cancellation_events: dict[str, asyncio.Event] = {}
        self._step_executors = StepExecutors()

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
            in_degree[node.id] = len(node.dependencies)
            for dep in node.dependencies:
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
            for dep_id in node.dependencies:
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
        """Execute a single DAG node."""
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

        execution.status = TaskStatus.RUNNING
        execution.started_at = datetime.utcnow()

        try:
            work_payload = {**node.payload, **dag.context}
            if cancel_event.is_set():
                raise asyncio.CancelledError()

            execution_mode = ExecutionMode(node.payload.get("execution_mode", "sync"))
            result = await self._step_executors.execute(
                execution_mode,
                StepExecutionContext(
                    task_type=node.task_type,
                    payload=work_payload,
                    timeout_seconds=node.timeout_seconds,
                    flask_url=node.payload.get("flask_url"),
                ),
                handler,
            )

            execution.status = TaskStatus.SUCCESS
            execution.result = result if isinstance(result, dict) else {"value": result}
            execution.completed_at = datetime.utcnow()

            # Merge result into context
            if isinstance(result, dict):
                dag.context.update(result)

        except asyncio.TimeoutError:
            execution.status = TaskStatus.TIMEOUT
            execution.error_message = f"Node timed out after {node.timeout_seconds} seconds"
            execution.completed_at = datetime.utcnow()

            if node.on_failure == "skip":
                execution.skipped = True
                execution.skip_reason = "Node failed with on_failure=skip"

        except asyncio.CancelledError:
            execution.status = TaskStatus.CANCELLED
            execution.completed_at = datetime.utcnow()

        except Exception as e:
            execution.status = TaskStatus.FAILED
            execution.error_message = str(e)
            execution.completed_at = datetime.utcnow()

            # Handle failure based on on_failure setting
            if node.on_failure == "skip":
                execution.skipped = True
                execution.skip_reason = "Node failed with on_failure=skip"
            elif node.on_failure == "fallback" and node.fallback_payload:
                # Store fallback result in context
                dag.context.update(node.fallback_payload)

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
                            for dep_id in node.dependencies:
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

        if any_cancelled:
            dag.status = DAGExecutionStatus.CANCELLED
        elif any_failed:
            dag.status = DAGExecutionStatus.FAILED
        elif all_success:
            dag.status = DAGExecutionStatus.SUCCESS
        else:
            dag.status = DAGExecutionStatus.PARTIAL

        dag.completed_at = datetime.utcnow()
        dag.updated_at = datetime.utcnow()

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
