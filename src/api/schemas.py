"""Standardised API response schemas.

All list endpoints return PaginatedResponse; all mutations return
OperationResponse. This gives consumers a consistent shape to parse
regardless of the resource type.
"""
from __future__ import annotations

from typing import Generic, TypeVar, Any
from pydantic import BaseModel, Field

T = TypeVar("T")


class PaginatedResponse(BaseModel, Generic[T]):
    """Cursor-based paginated list response.

    - items: the page's items
    - total: total count (optional — omit if too expensive)
    - next_cursor: pass as ?cursor= to fetch next page; null = last page
    - page_size: items returned in this page
    """
    items: list[T] = Field(default_factory=list)
    total: int | None = None
    next_cursor: str | None = None
    page_size: int = 0

    def model_post_init(self, __context: Any) -> None:
        if not self.page_size:
            object.__setattr__(self, "page_size", len(self.items))


class OperationResponse(BaseModel):
    """Standard mutation response."""
    success: bool
    message: str = ""
    data: dict[str, Any] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    """Standard error response body."""
    error: str
    code: str = "INTERNAL_ERROR"
    details: dict[str, Any] = Field(default_factory=dict)
    request_id: str = ""
