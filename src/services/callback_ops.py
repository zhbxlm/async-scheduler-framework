from __future__ import annotations

import inspect
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from src.models.callback_outbox import CallbackOutboxRecord, CallbackDeliveryStatus


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


class CallbackOpsService:
    def __init__(self, session_factory: Any | None = None) -> None:
        self._db = session_factory

    async def list_dead_letters(self, limit: int = 100) -> list[CallbackOutboxRecord]:
        if self._db is None:
            return []
        async with self._db() as session:
            result = await _maybe_await(session.execute(
                select(CallbackOutboxRecord).where(
                    CallbackOutboxRecord.delivery_status == CallbackDeliveryStatus.DEAD_LETTER
                ).limit(limit)
            ))
            return list(result.scalars().all())

    async def acknowledge_dead_letter(self, outbox_id: int, *, actor: str = "system", reason: str | None = None) -> bool:
        if self._db is None:
            return False
        async with self._db() as session:
            row = await _maybe_await(session.get(CallbackOutboxRecord, outbox_id))
            if row is None:
                return False
            row.acknowledged_by = actor
            row.acknowledged_at = datetime.now(timezone.utc)
            await _maybe_await(session.commit())
        try:
            from src.services.operator_actions import OperatorActionService
            await OperatorActionService(self._db).record(
                actor=actor,
                action_type="dead_letter_acknowledged",
                target_type="callback_outbox",
                target_id=str(outbox_id),
                reason=reason,
                payload={"outbox_id": outbox_id},
                task_id=getattr(row, "task_id", None),
            )
        except Exception:
            pass
        return True

    async def replay_dead_letter(self, outbox_id: int, *, actor: str = "system", reason: str | None = None) -> bool:
        if self._db is None:
            return False
        async with self._db() as session:
            row = await _maybe_await(session.get(CallbackOutboxRecord, outbox_id))
            if row is None:
                return False
            row.delivery_status = CallbackDeliveryStatus.PENDING
            row.next_attempt_at = None
            row.last_error = None
            row.acknowledged_by = None
            row.acknowledged_at = None
            await _maybe_await(session.commit())
        try:
            from src.services.operator_actions import OperatorActionService
            await OperatorActionService(self._db).record(
                actor=actor,
                action_type="callback_replay_requested",
                target_type="callback_outbox",
                target_id=str(outbox_id),
                reason=reason,
                payload={"outbox_id": outbox_id},
                task_id=getattr(row, "task_id", None),
            )
        except Exception:
            pass
        return True
