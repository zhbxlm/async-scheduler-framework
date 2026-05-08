from __future__ import annotations

from fastapi import Depends, Request

from src.api.auth import authenticate
from src.api.routes.ops_shared import DbFactory, router
from src.services.access_policy import resolve_tenant_id
from src.services.operator_queries import OperatorQueryService
from src.services.recovery_explainer import RecoveryExplainerService
from src.services.replay_chain_queries import ReplayChainQueryService
from src.services.replay_lineage import ReplayLineageService
from src.services.replay_policy import ReplayPolicyService
from src.services.run_centric_queries import RunCentricQueryService
from src.services.task_audit_queries import TaskAuditQueryService


@router.get("/tasks/{task_id}/timeline", summary="Task timeline events")
async def task_timeline(
    task_id: str,
    db_factory=DbFactory,
    _auth: dict = Depends(authenticate),
    limit: int = 100,
) -> dict:
    rows = await TaskAuditQueryService(db_factory).get_task_timeline(task_id, limit=limit)
    return {
        "task_id": task_id,
        "items": [
            {
                "id": r.id,
                "event_type": r.event_type,
                "run_key": r.run_key,
                "event_payload": r.event_payload,
                "created_at": r.created_at.isoformat() if getattr(r, "created_at", None) else None,
            }
            for r in rows
        ],
    }


@router.post("/tasks/{task_id}/replay", summary="Request task replay lineage")
async def task_replay(
    task_id: str,
    request: Request,
    db_factory=DbFactory,
    _auth: dict = Depends(authenticate),
    reason: str | None = None,
    from_run_key: str | None = None,
) -> dict:
    # redis is optional for replay policy; tolerate None
    redis = getattr(request.app.state, "redis", None)
    actor = resolve_tenant_id(_auth, fallback="operator")
    policy = await ReplayPolicyService(redis_client=redis, session_factory=db_factory).check_task_replay_allowed(
        task_id,
        reason=reason,
        actor_role=_auth.get("role", "operator"),
    )
    if not policy["allowed"]:
        return {
            "ok": False,
            "task_id": task_id,
            "error": "replay not allowed",
            "reasons": policy["reasons"],
        }
    return await ReplayLineageService(db_factory).replay_task(
        task_id=task_id,
        actor=actor,
        reason=reason,
        from_run_key=from_run_key,
    )


@router.get("/operator-actions", summary="List operator actions")
async def operator_actions(
    db_factory=DbFactory,
    _auth: dict = Depends(authenticate),
    target_type: str | None = None,
    target_id: str | None = None,
    action_type: str | None = None,
    limit: int = 100,
) -> dict:
    rows = await OperatorQueryService(db_factory).list_actions(
        target_type=target_type,
        target_id=target_id,
        action_type=action_type,
        limit=limit,
    )
    return {
        "items": [
            {
                "id": r.id,
                "actor": r.actor,
                "action_type": r.action_type,
                "target_type": r.target_type,
                "target_id": r.target_id,
                "reason": r.reason,
                "payload_json": r.payload_json,
                "created_at": r.created_at.isoformat() if getattr(r, "created_at", None) else None,
            }
            for r in rows
        ]
    }


@router.get("/tasks/{task_id}/recovery-explanation", summary="Explain stale/recovery state for a task")
async def task_recovery_explanation(
    task_id: str,
    request: Request,
    db_factory=DbFactory,
    _auth: dict = Depends(authenticate),
) -> dict:
    # redis is optional for recovery explainer; tolerate None
    redis = getattr(request.app.state, "redis", None)
    return await RecoveryExplainerService(redis_client=redis, session_factory=db_factory).explain_task(task_id)


@router.get("/tasks/{task_id}/replay-chain", summary="Replay lineage chain for a task")
async def task_replay_chain(
    task_id: str,
    db_factory=DbFactory,
    _auth: dict = Depends(authenticate),
    limit: int = 50,
) -> dict:
    chain = await ReplayChainQueryService(db_factory).get_replay_chain(task_id, limit=limit)
    return {"task_id": task_id, "chain": chain}


@router.get("/tasks/{task_id}/runs", summary="List task runs")
async def task_runs(
    task_id: str,
    db_factory=DbFactory,
    _auth: dict = Depends(authenticate),
    limit: int = 100,
) -> dict:
    runs = await RunCentricQueryService(db_factory).list_task_runs(task_id, limit=limit)
    return {
        "task_id": task_id,
        "items": [
            {
                "id": r.id,
                "run_key": r.run_key,
                "status": r.status,
                "created_at": r.created_at.isoformat() if getattr(r, "created_at", None) else None,
            }
            for r in runs
        ],
    }


@router.get("/tasks/{task_id}/runs/{run_key}/events", summary="List events for a specific run")
async def task_run_events(
    task_id: str,
    run_key: str,
    db_factory=DbFactory,
    _auth: dict = Depends(authenticate),
    limit: int = 100,
) -> dict:
    events = await RunCentricQueryService(db_factory).list_task_run_events(task_id, run_key=run_key, limit=limit)
    return {
        "task_id": task_id,
        "run_key": run_key,
        "items": [
            {
                "id": e.id,
                "event_type": e.event_type,
                "event_payload": e.event_payload,
                "created_at": e.created_at.isoformat() if getattr(e, "created_at", None) else None,
            }
            for e in events
        ],
    }


@router.get("/dags/{dag_id}/runs", summary="List dag runs")
async def dag_runs(
    dag_id: str,
    db_factory=DbFactory,
    _auth: dict = Depends(authenticate),
    limit: int = 100,
) -> dict:
    runs = await RunCentricQueryService(db_factory).list_dag_runs(dag_id, limit=limit)
    return {
        "dag_id": dag_id,
        "items": [
            {
                "id": r.id,
                "run_key": r.run_key,
                "status": r.status,
                "created_at": r.created_at.isoformat() if getattr(r, "created_at", None) else None,
            }
            for r in runs
        ],
    }


@router.get("/tasks/{task_id}/debug", summary="Debug information for a specific task")
async def task_debug(task_id: str, request: Request, _auth: dict = Depends(authenticate)) -> dict:
    """Return comprehensive debug information for a task."""
    # redis, qm, db_session are all optional in debug; tolerate None gracefully
    redis = getattr(request.app.state, "redis", None)
    qm = getattr(request.app.state, "queue_manager", None)
    db_session = getattr(request.app.state, "async_session_factory", None)

    result = {"task_id": task_id, "sources": {}}

    if db_session:
        from sqlalchemy import select

        from src.models.task import TaskRecord

        async with db_session() as session:
            query = select(TaskRecord).where(TaskRecord.task_id == task_id)
            task_record = (await session.execute(query)).scalar_one_or_none()
            if task_record:
                result["sources"]["mysql"] = {
                    "task_id": task_record.task_id,
                    "status": task_record.status.value,
                    "created_at": task_record.created_at.isoformat() if task_record.created_at else None,
                    "updated_at": task_record.updated_at.isoformat() if task_record.updated_at else None,
                    "tenant_id": task_record.tenant_id,
                    "task_type": task_record.task_type,
                    "priority": task_record.priority.value if task_record.priority else None,
                }
            else:
                result["sources"]["mysql"] = {"found": False}
    else:
        result["sources"]["mysql"] = {"available": False}

    if redis and qm:
        capabilities = await redis.smembers("queue:capabilities:registry")
        redis_status = {"capabilities": [], "found_in": []}
        for cap in capabilities:
            pending_key = f"queue:{cap}:pending"
            running_key = f"queue:{cap}:running"
            completed_key = f"queue:{cap}:stats"

            pending_score = await redis.zscore(pending_key, task_id)
            if pending_score is not None:
                rank = await redis.zrank(pending_key, task_id)
                redis_status["found_in"].append(
                    {
                        "queue": "pending",
                        "capability": cap,
                        "score": pending_score,
                        "position": rank + 1 if rank is not None else None,
                    }
                )

            running_score = await redis.zscore(running_key, task_id)
            if running_score is not None:
                redis_status["found_in"].append(
                    {
                        "queue": "running",
                        "capability": cap,
                        "score": running_score,
                    }
                )

            completed_count = await redis.hget(completed_key, "total_completed")
            if completed_count:
                redis_status["capabilities"].append(cap)

        result["sources"]["redis"] = redis_status
    else:
        result["sources"]["redis"] = {"available": False}

    recommendations = []
    mysql_found = result["sources"].get("mysql", {}).get("found", True) is not False
    redis_found = result["sources"].get("redis", {}).get("found_in", [])

    if not mysql_found and not redis_found:
        recommendations.append("Task not found in MySQL or Redis. May have been cleaned up.")
    elif redis_found:
        for entry in redis_found:
            if entry["queue"] == "pending":
                recommendations.append(
                    f"Task is pending in {entry['capability']} queue (position {entry.get('position', 'unknown')})."
                )
            elif entry["queue"] == "running":
                recommendations.append(f"Task is currently running in {entry['capability']}.")

    if recommendations:
        result["recommendations"] = recommendations

    return result
