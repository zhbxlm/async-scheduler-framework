"""Tests for the DAG engine."""

import asyncio
from datetime import datetime

import pytest

from async_scheduler.core.models import (
    DAG,
    DAGCreate,
    DAGExecutionStatus,
    DAGNode,
    TaskStatus,
)
from async_scheduler.dag import DAGEngine, default_dag_handler


@pytest.mark.asyncio
class TestDAGEngine:
    """Tests for the DAG engine."""

    @pytest.fixture
    def engine(self):
        """Create a DAG engine instance."""
        return DAGEngine()

    @pytest.fixture
    def simple_dag(self):
        """Create a simple DAG with 3 sequential nodes."""
        return DAG(
            name="simple_dag",
            nodes=[
                DAGNode(id="1", name="task1", task_type="fast", payload={"step": 1}),
                DAGNode(
                    id="2",
                    name="task2",
                    task_type="fast",
                    payload={"step": 2},
                    dependencies=["1"],
                ),
                DAGNode(
                    id="3",
                    name="task3",
                    task_type="fast",
                    payload={"step": 3},
                    dependencies=["2"],
                ),
            ],
            max_parallelism=2,
        )

    @pytest.fixture
    def parallel_dag(self):
        """Create a DAG with parallel execution."""
        return DAG(
            name="parallel_dag",
            nodes=[
                DAGNode(id="1", name="task1", task_type="slow", payload={"step": 1}),
                DAGNode(id="2", name="task2", task_type="slow", payload={"step": 2}),
                DAGNode(
                    id="3",
                    name="task3",
                    task_type="fast",
                    payload={"step": 3},
                    dependencies=["1", "2"],
                ),
            ],
            max_parallelism=2,
        )

    @pytest.fixture
    def conditional_dag(self):
        """Create a DAG with conditional execution."""
        return DAG(
            name="conditional_dag",
            nodes=[
                DAGNode(
                    id="1",
                    name="task1",
                    task_type="fast",
                    payload={"step": 1, "value": 42},
                ),
                DAGNode(
                    id="2",
                    name="task2",
                    task_type="fast",
                    payload={"step": 2},
                    dependencies=["1"],
                    condition="context['value'] > 50",  # Should not execute
                ),
                DAGNode(
                    id="3",
                    name="task3",
                    task_type="fast",
                    payload={"step": 3},
                    dependencies=["1"],
                    condition="context['value'] < 50",  # Should execute
                ),
            ],
            max_parallelism=2,
        )

    @pytest.fixture
    def error_handling_dag(self):
        """Create a DAG with error handling configurations."""
        return DAG(
            name="error_handling_dag",
            nodes=[
                DAGNode(id="1", name="task1", task_type="fast", payload={"step": 1}),
                DAGNode(
                    id="2",
                    name="task2",
                    task_type="slow",
                    payload={"step": 2, "sleep_seconds": 5},  # Will timeout
                    dependencies=["1"],
                    timeout_seconds=1,
                    on_failure="skip",
                ),
                DAGNode(
                    id="3",
                    name="task3",
                    task_type="fast",
                    payload={"step": 3},
                    dependencies=["2"],
                ),
            ],
            max_parallelism=2,
        )

    async def test_topological_sort(self, engine, simple_dag):
        """Test topological sorting of DAG nodes."""
        result = engine._topological_sort(simple_dag)
        assert len(result) == 3
        assert "1" in result
        assert "2" in result
        assert "3" in result
        # Verify dependencies are before dependents
        assert result.index("1") < result.index("2")
        assert result.index("2") < result.index("3")

    async def test_get_ready_nodes(self, engine, simple_dag):
        """Test getting ready nodes from a DAG."""
        ready = engine._get_ready_nodes(simple_dag)
        assert len(ready) == 1
        assert ready[0].id == "1"

    async def test_simple_dag_execution(self, engine, simple_dag):
        """Test execution of a simple sequential DAG."""
        result = await engine.execute(simple_dag, default_dag_handler)

        assert result.status == DAGExecutionStatus.SUCCESS
        assert len(result.node_executions) == 3

        for node_id, execution in result.node_executions.items():
            assert execution.status == TaskStatus.SUCCESS
            assert execution.completed_at is not None

    async def test_parallel_dag_execution(self, engine, parallel_dag):
        """Test execution of a parallel DAG."""
        result = await engine.execute(parallel_dag, default_dag_handler)

        assert result.status == DAGExecutionStatus.SUCCESS
        assert len(result.node_executions) == 3

        # Node 1 and 2 should have started roughly at the same time (parallel)
        exec_1 = parallel_dag.node_executions["1"]
        exec_2 = parallel_dag.node_executions["2"]

        assert exec_1.started_at is not None
        assert exec_2.started_at is not None

    async def test_conditional_execution(self, engine, conditional_dag):
        """Test conditional execution of DAG nodes."""
        result = await engine.execute(conditional_dag, default_dag_handler)

        assert result.status == DAGExecutionStatus.SUCCESS

        # Node 2 should be skipped (condition evaluated to False)
        exec_2 = conditional_dag.node_executions["2"]
        assert exec_2.skipped is True
        assert exec_2.status == TaskStatus.SUCCESS  # Skipped nodes are marked SUCCESS

        # Node 3 should execute (condition evaluated to True)
        exec_3 = conditional_dag.node_executions["3"]
        assert exec_3.skipped is False
        assert exec_3.status == TaskStatus.SUCCESS

    async def test_error_handling_skip(self, engine, error_handling_dag):
        """Test error handling with skip policy."""
        result = await engine.execute(error_handling_dag, default_dag_handler)

        # Node 2 should timeout but be skipped
        exec_2 = error_handling_dag.node_executions["2"]
        assert exec_2.status == TaskStatus.TIMEOUT
        assert exec_2.skipped is True
        assert "timed out" in exec_2.error_message.lower()

        # Node 3 should still execute
        exec_3 = error_handling_dag.node_executions["3"]
        assert exec_3.status == TaskStatus.SUCCESS

    async def test_dag_cancellation(self, engine, parallel_dag):
        """Test cancellation of a running DAG."""
        # Start execution in background
        task = asyncio.create_task(engine.execute(parallel_dag, default_dag_handler))

        # Wait a bit then cancel
        await asyncio.sleep(0.2)
        cancelled = await engine.cancel(parallel_dag.id)

        assert cancelled is True

        # Get result
        result = await task
        assert result.status == DAGExecutionStatus.CANCELLED

    async def test_dag_context_updates(self, engine, simple_dag):
        """Test that DAG context is updated with node results."""
        result = await engine.execute(simple_dag, default_dag_handler)

        # Context should contain results from executed nodes
        assert "task_type" in result.context
        assert "result" in result.context
        assert "value" in result.context

    async def test_cycle_detection(self, engine):
        """Test detection of cycles in DAG."""
        dag_with_cycle = DAG(
            name="cycle_dag",
            nodes=[
                DAGNode(id="1", name="task1", task_type="fast", payload={"step": 1}),
                DAGNode(
                    id="2",
                    name="task2",
                    task_type="fast",
                    payload={"step": 2},
                    dependencies=["1"],
                ),
                DAGNode(
                    id="3",
                    name="task3",
                    task_type="fast",
                    payload={"step": 3},
                    dependencies=["2"],
                ),
                DAGNode(
                    id="4",
                    name="task4",
                    task_type="fast",
                    payload={"step": 4},
                    dependencies=["3"],  # Would create cycle if we add dependency from 1 to 4
                ),
            ],
        )

        # Create a cycle by modifying dependencies
        dag_with_cycle.nodes[0].dependencies = ["3"]

        with pytest.raises(ValueError, match="cycle"):
            engine._topological_sort(dag_with_cycle)

    async def test_empty_dag(self, engine):
        """Test execution of an empty DAG."""
        empty_dag = DAG(
            name="empty_dag",
            nodes=[],
            max_parallelism=1,
        )

        result = await engine.execute(empty_dag, default_dag_handler)

        assert result.status == DAGExecutionStatus.SUCCESS
        assert len(result.node_executions) == 0


if __name__ == "__main__":
    # Import asyncio for the module
    import asyncio
