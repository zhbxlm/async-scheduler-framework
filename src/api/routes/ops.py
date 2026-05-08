"""ops routes — /ops/v1  (operational control)
aligned with docs/deepwiki-reference/API 参考.md

Compatibility aggregation module.
Concrete endpoint implementations are split across smaller route modules,
but imported here so existing `from src.api.routes.ops import router` users
continue to work unchanged.
"""
from __future__ import annotations

# Import side-effect modules to register endpoints onto the shared router.
from src.api.routes import ops_callback_routes as _ops_callback_routes  # noqa: F401
from src.api.routes import ops_dashboard_routes as _ops_dashboard_routes  # noqa: F401
from src.api.routes import ops_force_routes as _ops_force_routes  # noqa: F401
from src.api.routes import ops_overview_routes as _ops_overview_routes  # noqa: F401
from src.api.routes import ops_replay_routes as _ops_replay_routes  # noqa: F401
from src.api.routes.ops_shared import router as router  # re-export shared router
