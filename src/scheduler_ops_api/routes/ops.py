"""ops routes — package-owned aggregation module (transitional)."""
from __future__ import annotations

from scheduler_ops_api.routes import ops_overview_routes as _ops_overview_routes  # noqa: F401
from scheduler_ops_api.routes import ops_callback_routes as _ops_callback_routes  # noqa: F401
from scheduler_ops_api.routes import ops_force_routes as _ops_force_routes  # noqa: F401
from scheduler_ops_api.routes import ops_dashboard_routes as _ops_dashboard_routes  # noqa: F401
from scheduler_ops_api.routes import ops_replay_routes as _ops_replay_routes  # noqa: F401
from scheduler_ops_api.routes.ops_shared import router
