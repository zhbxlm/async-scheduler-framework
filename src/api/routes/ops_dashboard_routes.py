from __future__ import annotations

from fastapi import Depends, Request

from src.api.auth import authenticate
from src.api.routes.ops_shared import router
from src.services.operator_dashboard import OperatorDashboardService
from src.services.operator_ux import OperatorUXService


@router.get("/dashboard/summary", summary="Operator dashboard summary")
async def operator_dashboard_summary(request: Request, _auth: dict = Depends(authenticate)) -> dict:
    db_factory = getattr(request.app.state, "async_session_factory", None)
    return await OperatorDashboardService(db_factory).summary()


@router.get("/dashboard/stale-queue", summary="Stale task run queue")
async def stale_task_queue(request: Request, _auth: dict = Depends(authenticate), limit: int = 50) -> dict:
    db_factory = getattr(request.app.state, "async_session_factory", None)
    items = await OperatorUXService(db_factory).stale_task_queue(limit=limit)
    return {"items": items}


@router.get("/dashboard/replay-queue", summary="Replay-requested task run queue")
async def replay_queue(request: Request, _auth: dict = Depends(authenticate), limit: int = 50) -> dict:
    db_factory = getattr(request.app.state, "async_session_factory", None)
    items = await OperatorUXService(db_factory).replay_queue(limit=limit)
    return {"items": items}


@router.get("/dashboard/dead-letter-backlog", summary="Dead-letter callback backlog")
async def dead_letter_backlog(request: Request, _auth: dict = Depends(authenticate), limit: int = 50) -> dict:
    db_factory = getattr(request.app.state, "async_session_factory", None)
    items = await OperatorUXService(db_factory).dead_letter_backlog(limit=limit)
    return {"items": items}


@router.get("/dashboard/recent-actions", summary="Recent operator actions")
async def recent_operator_actions(
    request: Request,
    _auth: dict = Depends(authenticate),
    limit: int = 20,
    action_type: str | None = None,
) -> dict:
    db_factory = getattr(request.app.state, "async_session_factory", None)
    items = await OperatorUXService(db_factory).recent_operator_actions(
        limit=limit,
        action_type=action_type,
    )
    return {"items": items}
