from __future__ import annotations

import inspect
from typing import Any


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


class RecoveryExplainerService:
    def __init__(self, *, redis_client: Any | None = None, session_factory: Any | None = None) -> None:
        self._redis = redis_client
        self._db = session_factory

    async def explain_task(self, task_id: str) -> dict:
        explanation = {
            "task_id": task_id,
            "lock": {"present": False},
            "task_cache": {"present": False},
            "mysql": {"present": False},
            "recommended_action": "inspect",
            "reasons": [],
        }

        # Redis lock state
        if self._redis is not None:
            lock_key = f"task_lock:{task_id}"
            task_key = f"task:{task_id}"
            lock_exists = await _maybe_await(self._redis.exists(lock_key))
            cache_raw = await _maybe_await(self._redis.get(task_key))
            explanation["lock"]["present"] = bool(lock_exists)
            explanation["task_cache"]["present"] = cache_raw is not None
            if not lock_exists:
                explanation["reasons"].append("execution lease missing")

        # Durable DB state
        if self._db is not None:
            try:
                from src.models.task import TaskRecord
                async with self._db() as session:
                    task = await _maybe_await(session.get(TaskRecord, task_id))
                    if task is not None:
                        explanation["mysql"] = {
                            "present": True,
                            "status": getattr(task.status, "value", task.status),
                            "attempt": getattr(task, "attempt", 0),
                            "max_retries": getattr(task, "max_retries", 0),
                        }
                        if getattr(task.status, "value", task.status) == "running" and not explanation["lock"]["present"]:
                            explanation["reasons"].append("task marked running but lease missing")
                            explanation["recommended_action"] = "replay_or_repair"
                        elif getattr(task.status, "value", task.status) in ("failed", "completed"):
                            explanation["recommended_action"] = "inspect_or_replay_callback"
                        else:
                            explanation["recommended_action"] = "inspect"
            except Exception as exc:
                explanation["mysql"] = {"present": False, "error": str(exc)}

        if not explanation["reasons"]:
            explanation["reasons"].append("no obvious inconsistency detected")
        return explanation
