from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException


@dataclass
class ClusterService:
    registry: Any

    async def list_clusters(self, tenant_id: str) -> list[dict]:
        ids = await self.registry.list(tenant_id)
        clusters = []
        for cluster_id in ids:
            cluster = await self.registry.get(tenant_id, cluster_id)
            if cluster:
                clusters.append(cluster)
        return clusters

    async def get_cluster(self, tenant_id: str, cluster_id: str) -> dict:
        cluster = await self.registry.get(tenant_id, cluster_id)
        if cluster is None:
            raise HTTPException(status_code=404, detail=f"Cluster {cluster_id!r} not found")
        return cluster

    async def register_cluster(self, tenant_id: str, body: dict) -> dict:
        cluster_id = body.get("cluster_id")
        if not cluster_id:
            raise HTTPException(status_code=422, detail="cluster_id required")
        await self.registry.set(tenant_id, cluster_id, body)
        return {"cluster_id": cluster_id, "registered": True}

    async def update_cluster(self, tenant_id: str, cluster_id: str, body: dict) -> dict:
        existing = await self.get_cluster(tenant_id, cluster_id)
        merged = {**existing, **body, "cluster_id": cluster_id}
        await self.registry.set(tenant_id, cluster_id, merged)
        return merged

    async def delete_cluster(self, tenant_id: str, cluster_id: str) -> None:
        await self.registry.delete(tenant_id, cluster_id)

    async def get_cluster_resources(self, tenant_id: str, cluster_id: str) -> dict:
        cluster = await self.get_cluster(tenant_id, cluster_id)
        return {
            "cluster_id": cluster_id,
            "observed": cluster.get("observed_resources", {}),
            "planned": cluster.get("planned_resources", {}),
        }


@dataclass
class NodeService:
    registry: Any

    async def list_nodes(self, tenant_id: str, cluster_id: str = "") -> list[dict]:
        ids = await self.registry.list(tenant_id)
        nodes = []
        for node_id in ids:
            node = await self.registry.get(tenant_id, node_id)
            if not node:
                continue
            if cluster_id and node.get("cluster_id") != cluster_id:
                continue
            nodes.append(node)
        return nodes

    async def get_node(self, tenant_id: str, node_id: str) -> dict:
        node = await self.registry.get(tenant_id, node_id)
        if node is None:
            raise HTTPException(status_code=404, detail=f"Node {node_id!r} not found")
        return node

    async def invite_node(self, tenant_id: str, node_id: str, body: dict) -> dict:
        node = await self.get_node(tenant_id, node_id)
        cluster_id = body.get("cluster_id", "")
        node["cluster_id"] = cluster_id
        node["state"] = "joining"
        await self.registry.set(tenant_id, node_id, node)
        return {"node_id": node_id, "cluster_id": cluster_id, "state": "joining"}

    async def drain_node(self, tenant_id: str, node_id: str, body: dict) -> dict:
        node = await self.get_node(tenant_id, node_id)
        deadline_seconds = body.get("deadline_seconds", 300)
        node["state"] = "draining"
        await self.registry.set(tenant_id, node_id, node)
        return {"node_id": node_id, "state": "draining", "deadline_seconds": deadline_seconds}

    async def release_node(self, tenant_id: str, node_id: str) -> dict:
        node = await self.get_node(tenant_id, node_id)
        node["state"] = "idle"
        node["cluster_id"] = ""
        await self.registry.set(tenant_id, node_id, node)
        return {"node_id": node_id, "state": "idle"}

    async def delete_node(self, tenant_id: str, node_id: str) -> None:
        await self.registry.delete(tenant_id, node_id)


@dataclass
class DagService:
    loader: Any
    redis: Any | None = None

    async def list_dags(self, tenant_id: str) -> list[dict]:
        if self.redis is None:
            return []
        pattern = f"dag_def:{tenant_id}:*"
        dags = []
        async for key in self.redis.scan_iter(pattern):
            key_str = key.decode() if isinstance(key, bytes) else key
            dag_id = key_str.split(":")[-1]
            dags.append({"dag_id": dag_id, "tenant_id": tenant_id})
        return dags

    async def get_dag(self, dag_id: str, tenant_id: str) -> dict:
        dag = await self.loader.load(dag_id, tenant_id)
        if dag is None:
            raise HTTPException(status_code=404, detail=f"DAG {dag_id!r} not found")
        return dag

    async def register_dag(self, tenant_id: str, body: dict) -> dict:
        dag_id = body.get("dag_id")
        if not dag_id:
            raise HTTPException(status_code=422, detail="dag_id required")
        await self.loader.register(dag_id, tenant_id, body)
        return {"dag_id": dag_id, "tenant_id": tenant_id, "registered": True}

    async def update_dag(self, dag_id: str, tenant_id: str, body: dict) -> dict:
        existing = await self.get_dag(dag_id, tenant_id)
        merged = {**existing, **body, "dag_id": dag_id}
        await self.loader.register(dag_id, tenant_id, merged)
        return merged

    async def delete_dag(self, dag_id: str, tenant_id: str) -> None:
        await self.loader.delete(dag_id, tenant_id)


@dataclass
class ScheduleService:
    registry: Any

    async def list_schedules(self, tenant_id: str) -> list[dict]:
        ids = await self.registry.list(tenant_id)
        schedules = []
        for schedule_id in ids:
            schedule = await self.registry.get(tenant_id, schedule_id)
            if schedule:
                schedules.append(schedule)
        return schedules

    async def get_schedule(self, tenant_id: str, schedule_id: str) -> dict:
        schedule = await self.registry.get(tenant_id, schedule_id)
        if schedule is None:
            raise HTTPException(status_code=404, detail=f"Schedule {schedule_id!r} not found")
        return schedule

    async def create_schedule(self, tenant_id: str, body: dict) -> dict:
        cron_expr = body.get("cron_expr")
        if not cron_expr:
            raise HTTPException(status_code=422, detail="cron_expr required")
        schedule_id = body.get("schedule_id") or str(uuid.uuid4())
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        record = {
            **body,
            "schedule_id": schedule_id,
            "tenant_id": tenant_id,
            "enabled": body.get("enabled", True),
            "created_at": now_iso,
            "updated_at": now_iso,
            "last_triggered_at": None,
            "next_fire_at": None,
        }
        await self.registry.set(tenant_id, schedule_id, record)
        return record

    async def update_schedule(self, tenant_id: str, schedule_id: str, body: dict) -> dict:
        existing = await self.get_schedule(tenant_id, schedule_id)
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        merged = {**existing, **body, "schedule_id": schedule_id, "updated_at": now_iso}
        await self.registry.set(tenant_id, schedule_id, merged)
        return merged

    async def toggle_schedule(self, tenant_id: str, schedule_id: str, body: dict) -> dict:
        enabled = bool(body.get("enabled", True))
        ok = await self.registry.toggle(tenant_id, schedule_id, enabled)
        if not ok:
            raise HTTPException(status_code=404, detail=f"Schedule {schedule_id!r} not found")
        return {"schedule_id": schedule_id, "enabled": enabled}

    async def delete_schedule(self, tenant_id: str, schedule_id: str) -> None:
        await self.registry.delete(tenant_id, schedule_id)
