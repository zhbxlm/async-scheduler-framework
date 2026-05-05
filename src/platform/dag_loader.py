"""DagLoader — aligned with docs/deepwiki-reference/DAG 编排.md

Load DAG definitions from:
1. YAML files in config/dags/
2. Redis (tenant-scoped hot store)
3. MySQL (cold store, loaded on cache miss)
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import orjson

logger = logging.getLogger(__name__)

_REDIS_KEY = "dag_def:{tenant_id}:{dag_id}"
_DEFAULT_CONFIG_DIR = "config/dags"


class DagLoader:
    """Load DagDefinition dicts from YAML, Redis, or MySQL."""

    def __init__(
        self,
        config_dir: str = _DEFAULT_CONFIG_DIR,
        redis_client: Any | None = None,
        db_session_factory: Any | None = None,
    ) -> None:
        self._config_dir = Path(config_dir)
        self._r = redis_client
        self._db = db_session_factory

    # ------------------------------------------------------------------
    # Primary public API
    # ------------------------------------------------------------------

    async def load(self, dag_id: str, tenant_id: str = "default") -> dict | None:
        """Load dag definition: Redis → YAML → MySQL, or None."""
        # 1. Try Redis hot cache
        if self._r is not None:
            data = await self._load_from_redis(dag_id, tenant_id)
            if data:
                return data

        # 2. Try local YAML
        data = self._load_from_yaml(dag_id)
        if data:
            # Warm Redis cache
            if self._r is not None:
                await self._cache_in_redis(dag_id, tenant_id, data)
            return data

        # 3. Try MySQL
        if self._db is not None:
            data = await self._load_from_db(dag_id, tenant_id)
            if data:
                if self._r is not None:
                    await self._cache_in_redis(dag_id, tenant_id, data)
                return data

        return None

    async def register(self, dag_id: str, tenant_id: str, definition: dict) -> bool:
        """Store a DAG definition in Redis (and optionally MySQL)."""
        if self._r is None:
            raise RuntimeError("DagLoader: redis_client required for register()")
        await self._cache_in_redis(dag_id, tenant_id, definition)
        logger.info("DagLoader: registered dag_id=%s tenant_id=%s", dag_id, tenant_id)
        return True

    async def delete(self, dag_id: str, tenant_id: str) -> bool:
        """Remove a DAG definition from Redis."""
        if self._r is None:
            return False
        key = _REDIS_KEY.format(tenant_id=tenant_id, dag_id=dag_id)
        removed = await self._r.delete(key)
        return bool(removed)

    # ------------------------------------------------------------------
    # Internal loaders
    # ------------------------------------------------------------------

    def _load_from_yaml(self, dag_id: str) -> dict | None:
        """Load from config/dags/<dag_id>.yaml (tenant-agnostic)."""
        try:
            import yaml
        except ImportError:
            logger.warning("DagLoader: PyYAML not installed, skipping YAML load")
            return None

        for candidate in [
            self._config_dir / f"{dag_id}.yaml",
            self._config_dir / f"{dag_id}.yml",
        ]:
            if candidate.exists():
                try:
                    with candidate.open() as f:
                        data = yaml.safe_load(f)
                    logger.debug("DagLoader: loaded %s from YAML", dag_id)
                    return data
                except Exception as exc:
                    logger.error("DagLoader: YAML load error %s: %s", candidate, exc)
        return None

    async def _load_from_redis(self, dag_id: str, tenant_id: str) -> dict | None:
        key = _REDIS_KEY.format(tenant_id=tenant_id, dag_id=dag_id)
        raw = await self._r.get(key)
        if not raw:
            return None
        try:
            return orjson.loads(raw)
        except orjson.JSONDecodeError:
            return None

    async def _cache_in_redis(
        self, dag_id: str, tenant_id: str, data: dict, ttl: int = 259200
    ) -> None:
        key = _REDIS_KEY.format(tenant_id=tenant_id, dag_id=dag_id)
        await self._r.set(key, orjson.dumps(data), ex=ttl)

    async def _load_from_db(self, dag_id: str, tenant_id: str) -> dict | None:
        """Load from MySQL dag_definitions table."""
        try:
            async with self._db() as session:
                from sqlalchemy import select, text
                # Flexible: try raw text query if model isn't imported
                result = await session.execute(
                    text(
                        "SELECT definition FROM dag_definitions "
                        "WHERE dag_id = :dag_id AND tenant_id = :tid LIMIT 1"
                    ),
                    {"dag_id": dag_id, "tid": tenant_id},
                )
                row = result.fetchone()
                if row and row[0]:
                    return orjson.loads(row[0])
        except Exception as exc:
            logger.warning("DagLoader: db load failed dag_id=%s: %s", dag_id, exc)
        return None
