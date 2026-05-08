from __future__ import annotations

from fastapi import Depends, Request

from src.api.auth import authenticate
from src.api.routes.ops_shared import DbFactory, router
from src.services.access_policy import resolve_tenant_id
from src.services.callback_ops import CallbackOpsService
from src.services.callback_replay_policy import CallbackReplayPolicyService
from src.services.task_audit_queries import TaskAuditQueryService


@router.get("/callbacks/summary", summary="Callback outbox summary")
async def callback_summary(
    db_factory=DbFactory,
    _auth: dict = Depends(authenticate),
) -> dict:
    return await TaskAuditQueryService(db_factory).get_callback_summary()


@router.get("/callbacks/dead-letters", summary="List callback dead letters")
async def callback_dead_letters(
    db_factory=DbFactory,
    _auth: dict = Depends(authenticate),
    limit: int = 100,
) -> dict:
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
    db_factory=DbFactory,
    _auth: dict = Depends(authenticate),
    reason: str | None = None,
) -> dict:
    actor = resolve_tenant_id(_auth, fallback="operator")
    ok = await CallbackOpsService(db_factory).acknowledge_dead_letter(outbox_id, actor=actor, reason=reason)
    return {"ok": ok, "outbox_id": outbox_id}


@router.post("/callbacks/dead-letters/{outbox_id}/replay", summary="Replay a dead-letter callback")
async def replay_dead_letter(
    outbox_id: int,
    db_factory=DbFactory,
    _auth: dict = Depends(authenticate),
    reason: str | None = None,
) -> dict:
    actor = resolve_tenant_id(_auth, fallback="operator")
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
