"""Tenant repository."""

from __future__ import annotations

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from async_scheduler.core.models import Tenant, TenantCreate
from async_scheduler.persistence.models import TenantORM


class TenantRepository:
    @staticmethod
    async def create(session: AsyncSession, tenant: TenantCreate) -> Tenant:
        data = tenant.model_dump()
        db_tenant = TenantORM(**data)
        session.add(db_tenant)
        await session.flush()
        return Tenant.model_validate(db_tenant)

    @staticmethod
    async def get(session: AsyncSession, tenant_id: str) -> Tenant | None:
        result = await session.execute(select(TenantORM).where(TenantORM.id == tenant_id))
        row = result.scalar_one_or_none()
        return Tenant.model_validate(row) if row else None

    @staticmethod
    async def list_all(session: AsyncSession, limit: int = 100) -> list[Tenant]:
        result = await session.execute(select(TenantORM).order_by(TenantORM.created_at.desc()).limit(limit))
        return [Tenant.model_validate(row) for row in result.scalars().all()]

    @staticmethod
    async def update_config(session: AsyncSession, tenant_id: str, config: dict) -> Tenant | None:
        await session.execute(update(TenantORM).where(TenantORM.id == tenant_id).values(config=config))
        return await TenantRepository.get(session, tenant_id)

    @staticmethod
    async def merge_config(session: AsyncSession, tenant_id: str, config: dict) -> Tenant | None:
        current = await TenantRepository.get(session, tenant_id)
        if current is None:
            return None
        merged = {**current.config, **config}
        await session.execute(update(TenantORM).where(TenantORM.id == tenant_id).values(config=merged))
        return await TenantRepository.get(session, tenant_id)
