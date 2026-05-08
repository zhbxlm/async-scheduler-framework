from __future__ import annotations

from fastapi import Depends, Request

from src.api.auth import authenticate
from src.api.routes.ops_shared import router
from src.services.callback_ops import CallbackOpsService
from src.services.callback_replay_policy import CallbackReplayPolicyService
from src.services.task_audit_queries import TaskAuditQueryService


@router.get("/callbacks/summary", summary="Callback outbox summary")
async def callback_summary(request: Request, _auth: dict = Depends(authenticate)) -> dict:
    db_factory = getattr(request.app.state, "async_session_factory", None)
    return await TaskAuditQueryService(db_factory).get_callback_summary()


@router.get("/callbacks/dead-letters", summary="List callback dead letters")
async def callback_dead_letters(request: Request, _auth: dict = Depends(authenticate), limit: int = 100) -> dict:
    db_factory = getattr(request.app.state, "async_session_factory", None)
    rows = await CallbackOpsService(db_factory).list_dead_letters(limit=limit)
    return {
        "items": [
            {
                "id": r.id,
                "task_id": r.task_id,
                "callback_url": r.callback_url,
                "status": getattr(r.delivery_status, "value", r.delivery_status),
                "attempt_count": r.attempt_count,
                "last_error": r.last_error,
            }
            for r in rows
        ]
    }


@router.post("/callbacks/dead-letters/{outbox_id}/ack", summary="Acknowledge a dead-letter callback")
async def ack_dead_letter(
    outbox_id: int,
    request: Request,
    _auth: dict = Depends(authenticate),
    reason: str | None = None,
) -> dict:
    db_factory = getattr(request.app.state, "async_session_factory", None)
    actor = _auth.get("tenant_id", "operator")
    ok = await CallbackOpsService(db_factory).acknowledge_dead_letter(outbox_id, actor=actor, reason=reason)
    return {"ok": ok, "outbox_id": outbox_id}


@router.post("/callbacks/dead-letters/{outbox_id}/replay", summary="Replay a dead-letter callback")
async def replay_dead_letter(
    outbox_id: int,
    request: Request,
    _auth: dict = Depends(authenticate),
    reason: str | None = None,
) -> dict:
    db_factory = getattr(request.app.state, "async_session_factory", None)
    actor = _auth.get("tenant_id", "operator")
    actor_role = _auth.get("role", "operator")
    policy = await CallbackReplayPolicyService(session_factory=db_factory).check_callback_replay_allowed(
        outbox_id,
        reason=reason,
        actor_role=actor_role,
    )
    if not policy["allowed"]:
        return {
            "ok": False,
            "outbox_id": outbox_id,
            "error": "replay not allowed",
            "reasons": policy["reasons"],
        }
    ok = await CallbackOpsService(db_factory).replay_dead_letter(outbox_id, actor=actor, reason=reason)
    return {"ok": ok, "outbox_id": outbox_id}
