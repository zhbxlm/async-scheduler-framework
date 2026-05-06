"""Task validation and data transformation services.

Moves business logic out of Pydantic models to follow single responsibility principle.
"""
from __future__ import annotations

from typing import Any


def validate_task_artifact(artifact_url: str | None, artifact_sha256: str | None) -> None:
    """Validate artifact URL and SHA256 consistency.
    
    Args:
        artifact_url: Optional artifact URL.
        artifact_sha256: Optional SHA256 hash.
    
    Raises:
        ValueError: If validation fails.
    """
    if artifact_url and artifact_sha256:
        if len(artifact_sha256) != 64:
            raise ValueError("artifact_sha256 must be a 64-char hex string")


def validate_task_scheduling(
    scheduled_at: str | None, 
    delay_seconds: int | None
) -> None:
    """Validate task scheduling parameters.
    
    Args:
        scheduled_at: Optional ISO timestamp.
        delay_seconds: Optional delay in seconds.
    
    Raises:
        ValueError: If both scheduled_at and delay_seconds are provided.
    """
    if scheduled_at is not None and delay_seconds is not None:
        raise ValueError(
            "Cannot specify both scheduled_at and delay_seconds; use only one."
        )


def fix_lua_cjson_empty_tables(values: dict[str, Any]) -> dict[str, Any]:
    """Convert empty lists → empty dicts (Lua cjson serialises {} as []).
    
    Args:
        values: Dictionary of task data.
    
    Returns:
        Modified dictionary with fixed empty tables.
    """
    if isinstance(values, dict):
        for key in ("input_data", "output_data", "metadata_json"):
            if isinstance(values.get(key), list) and len(values[key]) == 0:
                values[key] = {}
    return values