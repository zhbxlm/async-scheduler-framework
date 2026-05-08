from __future__ import annotations

from fastapi import Depends

from src.api.auth import authenticate
from src.api.routes.ops_shared import DbFactory, Redis, router
from src.services.access_policy import resolve_tenant_id
from src.services.force_operations import ForceOperationService


@router.post("/tasks/{task_id}/force-lease-eviction", summary="Force evict a stale execution lease (admin only)")
async def force_lease_eviction(
    task_id: str,
    db_factory=DbFactory,
    redis=Redis,
    _auth: dict = Depends(authenticate),
    reason: str | None = None,
) -> dict:
    actor = resolve_tenant_id(_auth, fallback="operator")
    actor_role = _auth.get("role", "operator")

    async def _evict() -> dict:
        lock_key = f"task_lock:{task_id}"
        deleted = await redis.delete(lock_key)
        return {"evicted": bool(deleted), "key": lock_key}

    result = await ForceOperationService(session_factory=db_factory).execute_force_operation(
        operation="force_lease_eviction",
        actor=actor,
        actor_role=actor_role,
        reason=reason,
        target_type="task",
        target_id=task_id,
        task_id=task_id,
        executor=_evict,
    )
    if not result["ok"]:
        return {"ok": False, "task_id": task_id, "error": result.get("error"), "reasons": result}
    return {"ok": True, "task_id": task_id, **result.get("side_effect", {})}
