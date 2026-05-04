"""TaskRouter — aligned with docs/deepwiki-reference/RayData 集成.md

Resolves dispatch mode and routes tasks:
1. Explicit dispatch_mode on request
2. task_type in settings.task.raydata.task_types → RAYDATA_NATIVE
3. Default: DAG_ORCHESTRATED
"""
from __future__ import annotations

import logging
from typing import Any

from src.models.task import TaskCreate, TaskDispatchMode

logger = logging.getLogger(__name__)


class TaskRouter:
    """Determine and execute task dispatch routing."""

    def __init__(
        self,
        queue_manager: Any | None = None,
        raydata_client: Any | None = None,
        cluster_registry: Any | None = None,
        raydata_task_types: list[str] | None = None,
    ) -> None:
        self._queue = queue_manager
        self._raydata = raydata_client
        self._clusters = cluster_registry
        self._raydata_task_types: set[str] = set(raydata_task_types or [])

    def _resolve_dispatch_mode(self, req: TaskCreate) -> TaskDispatchMode:
        """Resolve dispatch mode with three-tier priority."""
        # 1. Explicit on request
        if req.dispatch_mode is not None:
            return req.dispatch_mode
        # 2. task_type in raydata list
        if req.task_type in self._raydata_task_types:
            return TaskDispatchMode.RAYDATA_NATIVE
        # 3. Default
        return TaskDispatchMode.DAG_ORCHESTRATED

    async def route(self, req: TaskCreate, task_record: Any) -> dict[str, Any]:
        """Route *task_record* based on resolved dispatch mode.

        Returns a dict with routing decision metadata.
        """
        mode = self._resolve_dispatch_mode(req)
        task_id = getattr(task_record, "task_id", str(task_record))

        if mode == TaskDispatchMode.RAYDATA_NATIVE:
            return await self._route_raydata(req, task_record, task_id)
        else:
            return await self._route_dag(req, task_record, task_id)

    async def _route_dag(
        self, req: TaskCreate, task_record: Any, task_id: str
    ) -> dict[str, Any]:
        """Enqueue task for DAG-orchestrated execution."""
        if self._queue is not None:
            await self._queue.enqueue(task_id, priority=req.priority, payload=req.input_data)
        logger.debug("TaskRouter: DAG_ORCHESTRATED task_id=%s", task_id)
        return {
            "task_id": task_id,
            "dispatch_mode": TaskDispatchMode.DAG_ORCHESTRATED.value,
            "cluster_id": "",
        }

    async def _route_raydata(
        self, req: TaskCreate, task_record: Any, task_id: str
    ) -> dict[str, Any]:
        """Submit task directly to Ray cluster."""
        cluster_id = ""
        submission_id = ""

        if self._clusters is not None and self._raydata is not None:
            from src.models.task import TaskInfo, TaskStatus, TaskPriority
            task_info = TaskInfo(
                task_id=task_id,
                task_type=req.task_type,
                input_data=req.input_data,
                metadata_json=req.metadata,
                callback_url=req.callback_url or "",
                artifact_url=req.artifact_url or "",
                artifact_sha256=req.artifact_sha256 or "",
                status=TaskStatus.RUNNING,
                priority=req.priority or TaskPriority.NORMAL,
            )
            cluster = await self._clusters.select_cluster(capability=req.task_type)
            if cluster:
                cluster_id = cluster.cluster_id
                result = self._raydata.submit_task(cluster, task_info)
                submission_id = result.get("submission_id", "")
                logger.info(
                    "TaskRouter: RAYDATA_NATIVE task_id=%s cluster=%s submission=%s",
                    task_id, cluster_id, submission_id,
                )
            else:
                logger.warning("TaskRouter: no active cluster for RAYDATA_NATIVE task_id=%s", task_id)

        return {
            "task_id": task_id,
            "dispatch_mode": TaskDispatchMode.RAYDATA_NATIVE.value,
            "cluster_id": cluster_id,
            "current_step": "raydata_submitted",
            "submission_id": submission_id,
        }
