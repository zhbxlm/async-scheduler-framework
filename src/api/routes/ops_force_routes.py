from __future__ import annotations

from fastapi import Depends, Request

from src.api.auth import authenticate
from src.api.routes.ops_shared import router
from src.services.force_operations import ForceOperationService


@router.post("/tasks/{task_id}/force-lease-eviction", summary="Force evict a stale execution lease (admin only)")
async def force_lease_eviction(
    task_id: str,
    request: Request,
    _auth: dict = Depends(authenticate),
    reason: str | None = None,
) -> dict:
    db_factory = getattr(request.app.state, "async_session_factory", None)
    redis = getattr(request.app.state, "redis", None)
    actor = _auth.get("tenant_id", "operator")
    actor_role = _auth.get("role", "operator")

    async def _evict() -> dict:
        if redis is None:
            return {"evicted": False, "reason": "redis not available"}
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
