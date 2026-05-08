from __future__ import annotations

from typing import Any

from sqlalchemy import select

from src.common.db_utils import maybe_await
from src.models.operator_action import OperatorActionRecord


class OperatorQueryService:
    def __init__(self, session_factory: Any | None = None) -> None:
        self._db = session_factory

    async def list_actions(
        self,
        *,
        target_type: str | None = None,
        target_id: str | None = None,
        action_type: str | None = None,
        limit: int = 100,
    ) -> list[OperatorActionRecord]:
        if self._db is None:
            return []
        async with self._db() as session:
            stmt = select(OperatorActionRecord).order_by(OperatorActionRecord.created_at.desc()).limit(limit)
            if target_type:
                stmt = stmt.where(OperatorActionRecord.target_type == target_type)
            if target_id:
                stmt = stmt.where(OperatorActionRecord.target_id == target_id)
            if action_type:
                stmt = stmt.where(OperatorActionRecord.action_type == action_type)
            result = await maybe_await(session.execute(stmt))
            return list(result.scalars().all())
