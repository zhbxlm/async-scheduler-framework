from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/ops/v1", tags=["ops"])


def get_db_factory(request: Request):
    factory = getattr(request.app.state, "async_session_factory", None)
    if factory is None:
        raise HTTPException(status_code=503, detail="DB session factory not initialised")
    return factory


def get_redis(request: Request):
    redis = getattr(request.app.state, "redis", None)
    if redis is None:
        raise HTTPException(status_code=503, detail="Redis not initialised")
    return redis


DbFactory = Depends(get_db_factory)
Redis = Depends(get_redis)
