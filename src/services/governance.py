from __future__ import annotations

from typing import Any


class GovernanceService:
    def __init__(self, *, session_factory: Any | None = None) -> None:
        self._db = session_factory

    def is_high_risk_operation(self, operation: str) -> bool:
        high_risk_ops = {
            "force_replay_on_active_lease",
            "force_replay_on_running_task",
            "force_replay_without_reason",
            "force_acknowledge_dead_letter_without_review",
            "force_lease_eviction",
        }
        return operation in high_risk_ops

    def requires_strong_audit(self, operation: str) -> bool:
        return self.is_high_risk_operation(operation)

    def allowed_role_categories(self, operation: str) -> list[str]:
        base = ["operator", "admin"]
        if self.is_high_risk_operation(operation):
            return ["admin"]
        return base
