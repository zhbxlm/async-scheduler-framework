"""Simple in-process tenant quota enforcement."""

from __future__ import annotations

from collections import defaultdict


class QuotaExceededError(RuntimeError):
    pass


class TenantQuotaManager:
    """Tracks queue/running limits per tenant.

    This is intentionally local-memory MVP logic; a future production version
    can move this into Redis or the database.
    """

    def __init__(self, default_max_queued: int = 100, default_max_running: int = 10) -> None:
        self.default_max_queued = default_max_queued
        self.default_max_running = default_max_running
        self._queued: dict[str, int] = defaultdict(int)
        self._running: dict[str, int] = defaultdict(int)
        self._config: dict[str, dict[str, int]] = {}

    def configure_tenant(
        self,
        tenant_id: str,
        *,
        max_queued: int | None = None,
        max_running: int | None = None,
    ) -> None:
        self._config[tenant_id] = {
            "max_queued": max_queued or self.default_max_queued,
            "max_running": max_running or self.default_max_running,
        }

    def _limits(self, tenant_id: str | None) -> tuple[int, int]:
        key = tenant_id or "default"
        cfg = self._config.get(
            key,
            {
                "max_queued": self.default_max_queued,
                "max_running": self.default_max_running,
            },
        )
        return cfg["max_queued"], cfg["max_running"]

    def admit_queue(self, tenant_id: str | None) -> None:
        max_queued, _ = self._limits(tenant_id)
        key = tenant_id or "default"
        if self._queued[key] >= max_queued:
            raise QuotaExceededError(f"tenant queued quota exceeded: {key}")
        self._queued[key] += 1

    def release_queue(self, tenant_id: str | None) -> None:
        key = tenant_id or "default"
        self._queued[key] = max(0, self._queued[key] - 1)

    def admit_running(self, tenant_id: str | None) -> None:
        _, max_running = self._limits(tenant_id)
        key = tenant_id or "default"
        if self._running[key] >= max_running:
            raise QuotaExceededError(f"tenant running quota exceeded: {key}")
        self._running[key] += 1

    def release_running(self, tenant_id: str | None) -> None:
        key = tenant_id or "default"
        self._running[key] = max(0, self._running[key] - 1)

    def stats(self) -> dict[str, dict[str, int]]:
        tenant_keys = set(self._queued) | set(self._running) | set(self._config)
        out: dict[str, dict[str, int]] = {}
        for tenant in tenant_keys:
            max_queued, max_running = self._limits(tenant)
            out[tenant] = {
                "queued": self._queued[tenant],
                "running": self._running[tenant],
                "max_queued": max_queued,
                "max_running": max_running,
            }
        return out
