from __future__ import annotations

from typing import Any

_DEFAULT_HIGH_RISK_OPS: frozenset[str] = frozenset({
    "force_replay_on_active_lease",
    "force_replay_on_running_task",
    "force_replay_without_reason",
    "force_acknowledge_dead_letter_without_review",
    "force_lease_eviction",
})


class GovernanceService:
    def __init__(self, *, config: dict | None = None, session_factory: Any | None = None) -> None:
        self._db = session_factory
        if config and "high_risk_operations" in config:
            self._high_risk = frozenset(config["high_risk_operations"])
        else:
            self._high_risk = _DEFAULT_HIGH_RISK_OPS

    def is_high_risk_operation(self, operation: str) -> bool:
        return operation in self._high_risk

    def requires_strong_audit(self, operation: str) -> bool:
        return self.is_high_risk_operation(operation)

    def allowed_role_categories(self, operation: str) -> list[str]:
        base = ["operator", "admin"]
        if self.is_high_risk_operation(operation):
            return ["admin"]
        return base
