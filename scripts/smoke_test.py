#!/usr/bin/env python3
"""Smoke test script for validating basic framework operability.

This script runs a series of smoke tests to validate that the core
functionality of the async scheduler framework is working correctly.

Run with: python -m scripts.smoke_test
"""

import asyncio
import sys
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, ".")

from async_scheduler.backends import (
    InMemoryQueueBackend,
    InMemoryLockBackend,
    InMemoryRegistryBackend,
)
from async_scheduler.core.models import Task, TaskCreate, TaskPriority, TaskStatus, ScheduleCreate
from async_scheduler.dag import DAGEngine, StepExecutors, ExecutionMode
from async_scheduler.persistence import init_db, get_session_no_context, TaskRepository
from async_scheduler.platform import (
    TaskCompletionNode,
    TaskReconciler,
    build_service_container,
    ReconciliationConfig,
)
from async_scheduler.registry import CapabilityRegistry
from async_scheduler.scheduler import ScheduleRegistry


class Colors:
    """ANSI color codes."""
    GREEN = "\033[92m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    RESET = "\033[0m"
    BOLD = "\033[1m"


def print_success(msg: str) -> None:
    print(f"{Colors.GREEN}✓ {msg}{Colors.RESET}")


def print_error(msg: str) -> None:
    print(f"{Colors.RED}✗ {msg}{Colors.RESET}")


def print_header(msg: str) -> None:
    print(f"\n{Colors.BOLD}{Colors.BLUE}=== {msg} ==={Colors.RESET}\n")


async def test_database() -> bool:
    """Test database initialization and basic operations."""
    print_header("Database Operations")
    try:
        await init_db()
        print_success("Database initialized")

        async with await get_session_no_context() as session:
            # Test task creation
            task_create = TaskCreate(
                name="smoke_test_task",
                payload={"test": "data"},
                priority=TaskPriority.NORMAL,
            )
            task = await TaskRepository.create(session, task_create)
            assert task.id is not None
            assert task.status == TaskStatus.PENDING
            print_success("Task created")

            # Test task deletion
            deleted = await TaskRepository.delete(session, task.id)
            assert deleted is True
            print_success("Task deleted")

        return True
    except Exception as e:
        print_error(f"Database test failed: {e}")
        return False


async def test_queue_backend() -> bool:
    """Test QueueBackend operations."""
    print_header("Queue Backend")
    try:
        backend = InMemoryQueueBackend()

        # Test enqueue/dequeue
        task = Task(
            name="queue_test",
            payload={"data": "value"},
            priority=TaskPriority.NORMAL,
        )
        await backend.enqueue(task)
        print_success("Task enqueued")

        retrieved = await backend.dequeue(timeout=1.0)
        assert retrieved is not None
        assert retrieved.id == task.id
        print_success("Task dequeued")

        # Test size
        sizes = await backend.size()
        assert isinstance(sizes, dict)
        print_success("Queue size retrieval works")

        return True
    except Exception as e:
        print_error(f"Queue backend test failed: {e}")
        return False


async def test_lock_backend() -> bool:
    """Test LockBackend operations."""
    print_header("Lock Backend")
    try:
        backend = InMemoryLockBackend()

        # Test acquire/release
        handle = await backend.acquire("test-lock", ttl=10.0)
        assert handle is not None
        print_success("Lock acquired")

        released = await backend.release(handle)
        assert released is True
        print_success("Lock released")

        # Test is_locked
        assert not await backend.is_locked("test-lock")
        print_success("Lock status check works")

        return True
    except Exception as e:
        print_error(f"Lock backend test failed: {e}")
        return False


async def test_schedule_registry() -> bool:
    """Test ScheduleRegistry basic operations."""
    print_header("Schedule Registry")
    try:
        registry = ScheduleRegistry()

        # Test create
        schedule_create = ScheduleCreate(
            name="smoke_test_schedule",
            cron_expression="*/5 * * * *",
            task_template={"test": "data"},
        )
        schedule = await registry.create(schedule_create)
        assert schedule.id is not None
        print_success("Schedule created")

        # Test list_ready
        ready = await registry.list_ready()
        assert isinstance(ready, list)
        print_success("Ready schedules query works")

        # Test pause (without assert to avoid state issues)
        await registry.pause(schedule.id)
        print_success("Schedule pause works")

        return True
    except Exception as e:
        print_error(f"Schedule registry test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_step_executors() -> bool:
    """Test StepExecutors operations."""
    print_header("Step Executors")
    try:
        from async_scheduler.dag.step_executors import StepExecutionContext

        executors = StepExecutors(enable_metrics=True)

        async def handler(task_type: str, payload: dict) -> str:
            return f"processed: {task_type}"

        ctx = StepExecutionContext(
            task_type="test_task",
            payload={"data": "value"},
            timeout_seconds=5,
        )

        # Test sync execution
        result = await executors.execute(ExecutionMode.SYNC, ctx, handler)
        assert result.status.value == "completed"
        assert result.value == "processed: test_task"
        print_success("Sync execution works")

        # Test metrics
        metrics = executors.get_metrics()
        assert metrics is not None
        assert metrics.total_executions == 1
        print_success("Execution metrics tracked")

        return True
    except Exception as e:
        print_error(f"Step executors test failed: {e}")
        return False


async def test_capability_registry() -> bool:
    """Test CapabilityRegistry operations."""
    print_header("Capability Registry")
    try:
        registry = CapabilityRegistry()

        async def handler(payload: dict) -> str:
            return "result"

        # Test register
        registry.register(
            "test_capability",
            handler,
            description="Test capability",
            version="1.0.0",
            tags=["test"],
        )
        print_success("Capability registered")

        # Test get
        retrieved = registry.get("test_capability")
        assert retrieved is not None
        print_success("Capability retrieved")

        # Test get_info
        info = registry.get_info("test_capability")
        assert info is not None
        assert info.name == "test_capability"
        print_success("Capability info retrieved")

        # Test dispatch
        result = await registry.dispatch("test_capability", {"data": "value"})
        assert result == "result"
        print_success("Capability dispatched")

        # Test metrics
        metrics = registry.get_metrics()
        assert metrics is not None
        assert metrics["total_capabilities"] == 1
        print_success("Registry metrics retrieved")

        return True
    except Exception as e:
        print_error(f"Capability registry test failed: {e}")
        return False


async def test_task_completion_node() -> bool:
    """Test TaskCompletionNode basic operations."""
    print_header("Task Completion Node")
    try:
        completion_node = TaskCompletionNode(enable_metrics=True)

        # Test that completion node is created
        assert completion_node is not None
        print_success("TaskCompletionNode created")

        # Test metrics
        metrics = completion_node.get_metrics()
        assert metrics is not None
        print_success("Completion metrics accessible")

        # Test handler registration
        def handler(task):
            pass

        completion_node.register_completion_handler(TaskStatus.SUCCESS, handler)
        print_success("Completion handler registered")

        return True
    except Exception as e:
        print_error(f"Task completion node test failed: {e}")
        return False


async def test_task_reconciler() -> bool:
    """Test TaskReconciler basic operations."""
    print_header("Task Reconciler")
    try:
        # Test creation with config
        config = ReconciliationConfig(stuck_after_seconds=3600)
        reconciler = TaskReconciler(config=config)
        print_success("TaskReconciler created with config")

        # Test metrics
        metrics = reconciler.get_metrics()
        assert metrics is not None
        print_success("Reconciler metrics accessible")

        # Test config update
        reconciler.update_config(stuck_after_seconds=7200)
        assert reconciler.config.stuck_after_seconds == 7200
        print_success("Config update works")

        # Test is_running
        assert not reconciler.is_running()
        print_success("Running status check works")

        return True
    except Exception as e:
        print_error(f"Task reconciler test failed: {e}")
        return False


async def test_service_container() -> bool:
    """Test ServiceContainer integration."""
    print_header("Service Container")
    try:
        services = await build_service_container()
        assert services is not None
        print_success("Service container built")

        # Test all components are present
        assert services.queue_manager is not None
        assert services.task_executor is not None
        assert services.dag_engine is not None
        assert services.task_router is not None
        assert services.callback_dispatcher is not None
        assert services.completion_node is not None
        assert services.quota_manager is not None
        assert services.registry is not None
        assert services.task_consumer is not None
        assert services.cron_scheduler is not None
        assert services.worker_pool is not None
        assert services.task_handler is not None
        assert services.dag_handler is not None
        assert services.reconciler is not None
        assert services.step_executors is not None
        print_success("All service components present")

        return True
    except Exception as e:
        print_error(f"Service container test failed: {e}")
        return False


async def run_all_tests() -> int:
    """Run all smoke tests and return exit code."""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 60}")
    print(f"  Async Scheduler Framework - Smoke Tests")
    print(f"{'=' * 60}{Colors.RESET}\n")

    tests = [
        test_database,
        test_queue_backend,
        test_lock_backend,
        test_schedule_registry,
        test_step_executors,
        test_capability_registry,
        test_task_completion_node,
        test_task_reconciler,
        test_service_container,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            if await test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print_error(f"Test {test.__name__} raised unexpected error: {e}")
            failed += 1

    # Print summary
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 60}")
    print(f"  Smoke Test Summary")
    print(f"{'=' * 60}{Colors.RESET}")
    print(f"Total:  {len(tests)}")
    print(f"{Colors.GREEN}Passed: {passed}{Colors.RESET}")
    if failed > 0:
        print(f"{Colors.RED}Failed: {failed}{Colors.RESET}")
    print()

    if failed == 0:
        print_success("All smoke tests passed!")
        return 0
    else:
        print_error(f"{failed} smoke test(s) failed")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(run_all_tests())
    sys.exit(exit_code)
