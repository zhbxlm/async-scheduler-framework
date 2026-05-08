from __future__ import annotations

import logging

from fastapi import APIRouter

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/ops/v1", tags=["ops"])
