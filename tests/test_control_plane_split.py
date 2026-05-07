from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_service_container_exposes_build_control_plane():
    from src.platform.container import ServiceContainer
    assert hasattr(ServiceContainer, "build_control_plane")


def test_task_api_docstring_mentions_control_plane_split():
    import src.main_tasks as main_tasks
    assert "control-plane worker" in (main_tasks.__doc__ or "")
