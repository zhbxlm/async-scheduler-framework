from __future__ import annotations

from datetime import datetime, timedelta

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from async_scheduler.api.app import app
from async_scheduler.core.models import ExecutionAttemptCreate, TaskCreate, TaskStatus
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

        assert empty_history_response.status_code == 200
        empty_history_body = empty_history_response.json()
        assert empty_history_body["count"] == 0
        assert empty_history_body["items"] == []
