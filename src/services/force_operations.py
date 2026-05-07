from __future__ import annotations

from src.common.db_utils import maybe_await

from typing import Any

from src.services.governance import GovernanceService
from src.services.operator_actions import OperatorActionService




class ForceOperationService:
    """Enforce strong audit requirements for high-risk force operations."""

    def __init__(self, *, session_factory: Any | None = None) -> None:
        self._db = session_factory
        self._governance = GovernanceService(session_factory=session_factory)
        self._ops = OperatorActionService(session_factory=session_factory) if session_factory else None

    async def execute_force_operation(
        self,
        *,
        operation: str,
        actor: str,
        actor_role: str,
        reason: str | None,
        target_type: str,
        target_id: str,
        payload: dict | None = None,
        task_id: str | None = None,
        executor: Any | None = None,
    ) -> dict:
        allowed_roles = self._governance.allowed_role_categories(operation)
        is_high_risk = self._governance.is_high_risk_operation(operation)

        if actor_role not in allowed_roles:
            return {
                "ok": False,
                "operation": operation,
                "error": "insufficient role",
                "required_roles": allowed_roles,
                "actor_role": actor_role,
            }

        if is_high_risk and not reason:
            return {
                "ok": False,
                "operation": operation,
                "error": "reason required for high-risk force operation",
            }

        if self._ops:
            await self._ops.record(
                actor=actor,
                action_type=f"force_{operation}",
                target_type=target_type,
                target_id=target_id,
                reason=reason,
                payload={"operation": operation, "actor_role": actor_role, **(payload or {})},
                task_id=task_id,
            )

        # Execute the side-effect if provided (e.g. Redis key deletion)
        side_effect_result = None
        if executor is not None:
            side_effect_result = await executor()

        return {
            "ok": True,
            "operation": operation,
            "actor": actor,
            "target_type": target_type,
            "target_id": target_id,
            "is_high_risk": is_high_risk,
            "side_effect": side_effect_result,
        }
