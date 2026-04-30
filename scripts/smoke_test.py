#!/usr/bin/env python3
"""Smoke test script for validating basic framework operability.

Run directly:
    python scripts/smoke_test.py

This file is intentionally a runnable script rather than a pytest test module.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from async_scheduler.backends import InMemoryLockBackend, InMemoryQueueBackend, InMemoryRegistryBackend
from async_scheduler.core.models import ScheduleCreate, TaskCreate, TaskPriority, TaskStatus
from async_scheduler.dag import DAGEngine
from async_scheduler.persistence import TaskRepository, get_session_no_context, init_db
from async_scheduler.platform import TaskCompletionNode, TaskReconciler, build_service_container
from async_scheduler.registry import CapabilityRegistry
from async_scheduler.scheduler import ScheduleRegistry

__test__ = False


class Colors:
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


async def smoke_database() -> bool:
    print_header("Database Operations")
    try:
        await init_db()
        print_success("Database initialized")

        async with await get_session_no_context() as session:
            task = await TaskRepository.create(
                session,
                TaskCreate(name="smoke_test_task", payload={"test": "data"}, priority=TaskPriority.NORMAL),
            )
            assert task.id is not None
            assert task.status == TaskStatus.PENDING
            print_success("Task created")

            deleted = await TaskRepository.delete(session, task.id)
            assert deleted is True
            print_success("Task deleted")
        return True
    except Exception as e:
        print_error(f"Database smoke failed: {e}")
        return False


async def smoke_queue_backend() -> bool:
    print_header("Queue Backend")
    try:
        backend = InMemoryQueueBackend()
        from async_scheduler.core.models import Task

        task = Task(name="queue_test", payload={"data": "value"}, priority=TaskPriority.NORMAL)
        await backend.enqueue(task)
        print_success("Task enqueued")
        retrieved = await backend.dequeue(timeout=1.0)
        assert retrieved is not None and retrieved.id == task.id
        print_success("Task dequeued")
        return True
    except Exception as e:
        print_error(f"Queue backend smoke failed: {e}")
        return False


async def smoke_lock_backend() -> bool:
    print_header("Lock Backend")
    try:
        backend = InMemoryLockBackend()
        handle = await backend.acquire("test-lock", ttl=10.0)
        assert handle is not None
        print_success("Lock acquired")
        released = await backend.release(handle)
        assert released is True
        print_success("Lock released")
        return True
    except Exception as e:
        print_error(f"Lock backend smoke failed: {e}")
        return False


async def smoke_registry_backend() -> bool:
    print_header("Registry Backend")
    try:
        backend = InMemoryRegistryBackend()
        active = await backend.list_active(limit=10)
        assert isinstance(active, list)
        print_success("Registry backend list_active works")
        return True
    except Exception as e:
        print_error(f"Registry backend smoke failed: {e!r}")
        return False


async def smoke_schedule_registry() -> bool:
    print_header("Schedule Registry")
    try:
        registry = ScheduleRegistry()
        schedule = await registry.create(
            ScheduleCreate(name="smoke_schedule", cron_expression="*/5 * * * *", task_template={"msg": "tick"})
        )
        assert schedule.id is not None
        print_success("Schedule created")
        await registry.pause(schedule.id)
        print_success("Schedule paused")
        await registry.resume(schedule.id)
        print_success("Schedule resumed")
        return True
    except Exception as e:
        print_error(f"Schedule registry smoke failed: {e}")
        return False


async def smoke_capability_registry() -> bool:
    print_header("Capability Registry")
    try:
        registry = CapabilityRegistry()

        async def handler(payload: dict) -> str:
            return "ok"

        registry.register("demo", handler, description="demo capability", tags=["demo"])
        result = await registry.dispatch("demo", {"x": 1})
        assert result == "ok"
        print_success("Capability dispatch works")
        return True
    except Exception as e:
        print_error(f"Capability registry smoke failed: {e}")
        return False


async def smoke_completion_node() -> bool:
    print_header("Task Completion Node")
    try:
        node = TaskCompletionNode()
        assert node is not None
        print_success("Completion node usable")
        return True
    except Exception as e:
        print_error(f"Completion node smoke failed: {e}")
        return False


async def smoke_reconciler() -> bool:
    print_header("Task Reconciler")
    try:
        reconciler = TaskReconciler()
        metrics = reconciler.get_metrics()
        assert metrics is not None
        print_success("Reconciler metrics accessible")
        return True
    except Exception as e:
        print_error(f"Reconciler smoke failed: {e}")
        return False


async def smoke_service_container() -> bool:
    print_header("Service Container")
    try:
        services = await build_service_container()
        assert services.queue_manager is not None
        assert services.reconciler is not None
        assert services.task_consumer is not None
        print_success("Service container built")
        return True
    except Exception as e:
        print_error(f"Service container smoke failed: {e}")
        return False


async def smoke_dag_engine() -> bool:
    print_header("DAG Engine")
    try:
        engine = DAGEngine()
        assert engine is not None
        print_success("DAG engine constructed")
        return True
    except Exception as e:
        print_error(f"DAG engine smoke failed: {e}")
        return False


async def run_all() -> int:
    tests = [
        smoke_database,
        smoke_queue_backend,
        smoke_lock_backend,
        smoke_registry_backend,
        smoke_schedule_registry,
        smoke_capability_registry,
        smoke_completion_node,
        smoke_reconciler,
        smoke_service_container,
        smoke_dag_engine,
    ]

    passed = 0
    failed = 0
    for test in tests:
        if await test():
            passed += 1
        else:
            failed += 1

    print(f"\nPassed: {passed}  Failed: {failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run_all()))
