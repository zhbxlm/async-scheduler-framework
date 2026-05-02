from __future__ import annotations

from datetime import datetime, timedelta

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from async_scheduler.api.app import app
from async_scheduler.core.models import ExecutionAttemptCreate, ExecutionAttemptStatus, TaskCreate, TaskStatus
from async_scheduler.distributed.worker_registry import WorkerInfo
from async_scheduler.persistence import (
    ExecutionAttemptRepository,
    TaskRepository,
    close_db,
    drop_db,
    get_session,
    init_db,
)
from async_scheduler.platform.reconciler import ReconciliationConfig, RepairStrategy, TaskReconciler


@pytest.mark.asyncio
class TestObservabilityApi:
    @pytest_asyncio.fixture(autouse=True)
    async def init_database(self):
        await drop_db()
        await init_db()
        yield
        await close_db()

    async def test_get_latest_attempt_endpoint_returns_latest_attempt(self) -> None:
        transport = ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            async with get_session() as session:
                task = await TaskRepository.create(session, TaskCreate(name="obs-task", payload={}))
                await ExecutionAttemptRepository.create(
                    session,
                    ExecutionAttemptCreate(
                        task_id=task.id,
                        worker_id="worker-a",
                        retry_index=0,
                        lease_token="lease-a",
                    ),
                )
                second = await ExecutionAttemptRepository.create(
                    session,
                    ExecutionAttemptCreate(
                        task_id=task.id,
                        worker_id="worker-b",
                        retry_index=1,
                        lease_token="lease-b",
                    ),
                )
                await ExecutionAttemptRepository.update(
                    session,
                    second.id,
                    started_at=datetime.utcnow() - timedelta(seconds=1),
                )

            async with AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.get(f"/tasks/{task.id}/attempts/latest")

        assert response.status_code == 200
        body = response.json()
        assert body["task_id"] == task.id
        assert body["worker_id"] == "worker-b"
        assert body["retry_index"] == 1

    async def test_list_attempts_endpoint_returns_attempt_history(self) -> None:
        transport = ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            async with get_session() as session:
                task = await TaskRepository.create(session, TaskCreate(name="obs-task-list", payload={}))
                first = await ExecutionAttemptRepository.create(
                    session,
                    ExecutionAttemptCreate(
                        task_id=task.id,
                        worker_id="worker-a",
                        retry_index=0,
                        lease_token="lease-a",
                    ),
                )
                second = await ExecutionAttemptRepository.create(
                    session,
                    ExecutionAttemptCreate(
                        task_id=task.id,
                        worker_id="worker-b",
                        retry_index=1,
                        lease_token="lease-b",
                    ),
                )
                await ExecutionAttemptRepository.update(session, first.id, started_at=datetime.utcnow() - timedelta(seconds=2))
                await ExecutionAttemptRepository.update(session, second.id, started_at=datetime.utcnow() - timedelta(seconds=1))

            async with AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.get(f"/tasks/{task.id}/attempts")

        assert response.status_code == 200
        body = response.json()
        assert len(body) == 2
        assert body[0]["worker_id"] == "worker-b"
        assert body[1]["worker_id"] == "worker-a"

    async def test_workers_endpoint_returns_registered_workers(self) -> None:
        transport = ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            from async_scheduler.api.app import services

            assert services is not None
            assert services.worker_registry is not None
            await services.worker_registry.register(WorkerInfo(worker_id="worker-observe", name="observe"))

            async with AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.get("/workers")
                detail_response = await client.get("/workers/worker-observe")

        assert response.status_code == 200
        body = response.json()
        assert body["count"] >= 1
        assert any(item["worker_id"] == "worker-observe" for item in body["items"])
        assert detail_response.status_code == 200
        assert detail_response.json()["worker_id"] == "worker-observe"

    async def test_reconciler_history_endpoint_returns_repair_audit(self) -> None:
        transport = ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            from async_scheduler.api.app import services

            assert services is not None
            services.reconciler = TaskReconciler(
                config=ReconciliationConfig(stuck_after_seconds=0, repair_strategy=RepairStrategy.REQUEUE),
                queue_manager=services.queue_manager,
                lock_backend=services.task_consumer._lock_backend,
                worker_registry=services.worker_registry,
            )

            services.reconciler._record_repair(
                task_id="task-audit",
                action="requeue",
                attempt_id="attempt-audit",
                attempt_status="abandoned",
                task_status=TaskStatus.QUEUED.value,
                error_message="audit",
            )

            async with AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.get("/reconciler/history")

        assert response.status_code == 200
        body = response.json()
        assert body["count"] == 1
        assert body["items"][0]["task_id"] == "task-audit"
        assert body["items"][0]["action"] == "requeue"

    async def test_reconciler_history_supports_action_and_task_filters(self) -> None:
        transport = ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            from async_scheduler.api.app import services

            assert services is not None
            services.reconciler._record_repair(
                task_id="task-a",
                action="requeue",
                attempt_id="attempt-a",
                attempt_status="abandoned",
                task_status=TaskStatus.QUEUED.value,
                error_message="a",
            )
            services.reconciler._record_repair(
                task_id="task-b",
                action="mark_failed",
                attempt_id="attempt-b",
                attempt_status="abandoned",
                task_status=TaskStatus.FAILED.value,
                error_message="b",
            )

            async with AsyncClient(transport=transport, base_url="http://testserver") as client:
                action_response = await client.get("/reconciler/history?action=requeue")
                task_response = await client.get("/reconciler/history?task_id=task-b")

        assert action_response.status_code == 200
        action_body = action_response.json()
        assert action_body["count"] >= 1
        assert all(item["action"] == "requeue" for item in action_body["items"])

        assert task_response.status_code == 200
        task_body = task_response.json()
        assert task_body["count"] == 1
        assert task_body["items"][0]["task_id"] == "task-b"

    async def test_worker_detail_returns_404_for_unknown_worker(self) -> None:
        transport = ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            async with AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.get("/workers/does-not-exist")

        assert response.status_code == 404

    async def test_debug_lease_endpoint_returns_lock_and_attempt_state(self) -> None:
        transport = ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            from async_scheduler.api.app import services

            assert services is not None
            assert services.lock_backend is not None

            async with get_session() as session:
                task = await TaskRepository.create(session, TaskCreate(name="lease-debug-task", payload={}))
                await TaskRepository.update(session, task.id, status=TaskStatus.RUNNING)
                await ExecutionAttemptRepository.create(
                    session,
                    ExecutionAttemptCreate(
                        task_id=task.id,
                        worker_id="worker-lease-debug",
                        retry_index=0,
                        lease_token="lease-debug-token",
                    ),
                )

            await services.lock_backend.acquire(f"task:{task.id}", ttl=30)

            async with AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.get(f"/debug/leases/{task.id}")

        assert response.status_code == 200
        body = response.json()
        assert body["task_id"] == task.id
        assert body["task_status"] == TaskStatus.RUNNING.value
        assert body["lease"] is not None
        assert body["lease"]["locked"] is True
        assert body["lease"]["key"] == f"task:{task.id}"
        assert body["latest_attempt"] is not None
        assert body["latest_attempt"]["worker_id"] == "worker-lease-debug"
        assert body["latest_attempt"]["lease_token"] == "lease-debug-token"

    async def test_debug_leases_list_and_worker_filter_endpoints(self) -> None:
        transport = ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            from async_scheduler.api.app import services

            assert services is not None
            assert services.lock_backend is not None

            async with get_session() as session:
                task_a = await TaskRepository.create(session, TaskCreate(name="lease-list-a", payload={}))
                task_b = await TaskRepository.create(session, TaskCreate(name="lease-list-b", payload={}))
                await TaskRepository.update(session, task_a.id, status=TaskStatus.RUNNING)
                await TaskRepository.update(session, task_b.id, status=TaskStatus.RUNNING)
                await ExecutionAttemptRepository.create(
                    session,
                    ExecutionAttemptCreate(
                        task_id=task_a.id,
                        worker_id="worker-list-a",
                        retry_index=0,
                        lease_token="lease-list-a",
                    ),
                )
                await ExecutionAttemptRepository.create(
                    session,
                    ExecutionAttemptCreate(
                        task_id=task_b.id,
                        worker_id="worker-list-b",
                        retry_index=1,
                        lease_token="lease-list-b",
                    ),
                )

            await services.lock_backend.acquire(f"task:{task_a.id}", ttl=30)
            await services.lock_backend.acquire(f"task:{task_b.id}", ttl=30)

            async with AsyncClient(transport=transport, base_url="http://testserver") as client:
                leases_response = await client.get("/debug/leases")
                worker_response = await client.get("/workers/worker-list-a/leases")
                filtered_response = await client.get(
                    f"/debug/leases?worker_id=worker-list-a&task_status={TaskStatus.RUNNING.value}&locked_only=true"
                )
                # Debug endpoint to see the item before locked_only filter
                worker_only_response = await client.get(
                    f"/debug/leases?worker_id=worker-list-a&task_status={TaskStatus.RUNNING.value}"
                )

        assert leases_response.status_code == 200
        leases_body = leases_response.json()
        assert leases_body["count"] >= 2
        assert any(item["task_id"] == task_a.id for item in leases_body["items"])
        assert any(item["task_id"] == task_b.id for item in leases_body["items"])

        assert worker_response.status_code == 200
        worker_body = worker_response.json()
        assert worker_body["worker_id"] == "worker-list-a"
        assert worker_body["count"] == 1
        assert worker_body["items"][0]["task_id"] == task_a.id
        assert worker_body["items"][0]["latest_attempt"]["worker_id"] == "worker-list-a"

        # Check worker+task_status filter works
        assert worker_only_response.status_code == 200
        worker_only_body = worker_only_response.json()
        # Should have exactly 1 item
        assert worker_only_body["count"] == 1, f"Expected 1 item with worker+task_status filter, got {worker_only_body['count']}. Items: {worker_only_body['items']}"
        item = worker_only_body["items"][0]
        assert item["task_id"] == task_a.id
        # Ensure lease info present and locked is True
        assert item["lease"] is not None, "Lease info missing"
        assert item["lease"]["locked"] is True, f"Lease not locked: {item['lease']}"

        assert filtered_response.status_code == 200
        filtered_body = filtered_response.json()
        # Debug print for troubleshooting
        # print('filtered_body', filtered_body)
        # print('items', filtered_body["items"])
        # if filtered_body["count"] == 0:
        #     # examine unfiltered list to see what's missing
        #     pass
        assert filtered_body["count"] == 1, f"Expected 1 filtered item, got {filtered_body['count']}. Items: {filtered_body['items']}"
        assert filtered_body["items"][0]["task_id"] == task_a.id
        assert filtered_body["filters"]["worker_id"] == "worker-list-a"
        assert filtered_body["filters"]["locked_only"] is True

    async def test_debug_lease_anomalies_endpoint_lists_suspicious_states(self) -> None:
        transport = ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            from async_scheduler.api.app import services

            assert services is not None
            assert services.lock_backend is not None

            async with get_session() as session:
                task = await TaskRepository.create(session, TaskCreate(name="anomaly-task", payload={}))
                await TaskRepository.update(session, task.id, status=TaskStatus.RUNNING)
                attempt = await ExecutionAttemptRepository.create(
                    session,
                    ExecutionAttemptCreate(
                        task_id=task.id,
                        worker_id="worker-anomaly",
                        retry_index=0,
                        lease_token="lease-anomaly",
                    ),
                )
                await ExecutionAttemptRepository.update(
                    session,
                    attempt.id,
                    status=ExecutionAttemptStatus.ABANDONED,
                )

            async with AsyncClient(transport=transport, base_url="http://testserver") as client:
                response = await client.get("/debug/leases/anomalies")

        assert response.status_code == 200
        body = response.json()
        assert body["count"] >= 1
        matching = [item for item in body["items"] if item["task_id"] == task.id]
        assert matching
        assert "running_without_lock" in matching[0]["anomaly_types"]
        assert "abandoned_but_running" in matching[0]["anomaly_types"]

    async def test_health_and_queue_stats_include_observability_counts(self) -> None:
        transport = ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            from async_scheduler.api.app import services

            assert services is not None
            assert services.worker_registry is not None
            await services.worker_registry.register(WorkerInfo(worker_id="worker-health", name="health"))
            services.reconciler._record_repair(
                task_id="task-health",
                action="mark_failed",
                attempt_id="attempt-health",
                attempt_status="abandoned",
                task_status=TaskStatus.FAILED.value,
                error_message="health-audit",
            )

            async with AsyncClient(transport=transport, base_url="http://testserver") as client:
                health_response = await client.get("/health")
                queue_response = await client.get("/queue/stats")
                debug_response = await client.get("/debug/summary")
                empty_history_response = await client.get("/reconciler/history?offset=10")

        assert health_response.status_code == 200
        health_body = health_response.json()
        assert health_body["worker_count"] >= 1
        assert health_body["repair_history_count"] >= 1

        assert queue_response.status_code == 200
        queue_body = queue_response.json()
        assert queue_body["worker_count"] >= 1
        assert "reconciler_running" in queue_body

        assert debug_response.status_code == 200
        debug_body = debug_response.json()
        assert debug_body["workers"]["count"] >= 1
        assert len(debug_body["reconciler"]["recent_history"]) >= 1
        assert "queue" in debug_body
        assert "health" in debug_body
        assert "leases" in debug_body
        assert "locked_count" in debug_body["leases"]
        assert "running_without_lock_count" in debug_body["leases"]
        assert "locked_but_terminal_count" in debug_body["leases"]
        assert "abandoned_but_running_count" in debug_body["leases"]
        assert "stale_lease_count" in debug_body["leases"]

        assert empty_history_response.status_code == 200
        empty_history_body = empty_history_response.json()
        assert empty_history_body["count"] == 0
        assert empty_history_body["items"] == []
