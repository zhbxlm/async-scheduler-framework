# Phase 3 Architecture Summary — 2026-05-08

Project: async-scheduler-framework
Status: completed
Theme: boundary normalization and assembly standardization

## Executive summary

Phase 3 focused on reducing architectural variance rather than adding new product surface.
The work standardized three layers that had still been evolving unevenly after earlier phases:

1. **resource-oriented route layering**,
2. **process entrypoint/runtime assembly**,
3. **container builder internals**.

This phase did not change public API paths or product scope.
Instead, it made the codebase more uniform, easier to reason about, easier to test, and safer to extend.

---

## Why Phase 3 was needed

After P0–P9 and the phase-2 cleanup wave, the codebase was already functionally strong, but it still had architectural shape mismatch in three places:

- some route families (`clusters`, `nodes`, `dags`, `schedules`) still used an older “route directly orchestrates runtime object” style,
- process entrypoints (`main.py`, `main_tasks.py`, `main_control.py`) repeated similar startup/shutdown logic in slightly different ways,
- `ServiceContainer` builders still duplicated common infra wiring.

None of these were immediate production bugs, but together they increased:
- maintenance cost,
- refactor risk,
- mental overhead for future contributors,
- probability of boundary drift across runtime roles.

Phase 3 addressed exactly those issues.

---

## Scope delivered

### 3A — Resource route normalization

Added:
- `src/services/resource_application.py`
- resource-oriented service classes:
  - `ClusterService`
  - `NodeService`
  - `DagService`
  - `ScheduleService`

Extended:
- `src/api/dependencies.py`
  - `get_cluster_registry`
  - `get_node_registry`
  - `get_schedule_registry`
  - `get_dag_loader`
  - `get_redis`
  - typed dependency aliases

Migrated route modules:
- `src/api/routes/clusters.py`
- `src/api/routes/nodes.py`
- `src/api/routes/dags.py`
- `src/api/routes/schedules.py`

Testing:
- `tests/test_resource_routes.py`

Outcome:
- resource routes now follow the same layering direction as `tasks` and split `ops` routes,
- route layer focuses on request/auth/response wiring,
- orchestration logic moved below the route layer,
- direct `app.state` access was reduced and standardized.

---

### 3B — Entrypoint/runtime normalization

Added:
- `src/api/app_runtime.py`

Shared helpers introduced:
- `mount_container_state(...)`
- `start_container_lifecycle(...)`
- `stop_container_lifecycle(...)`
- `cache_auth_settings(...)`
- `register_control_plane_resources(...)`

Updated entrypoints:
- `src/main.py`
- `src/main_tasks.py`
- `src/main_control.py`

Testing:
- `tests/test_app_runtime.py`
- control-plane contract coverage extended via helper-level tests

Outcome:
- ops-api, task-api, and control-plane now follow a more uniform assembly pattern,
- control-plane background resource registration is now an explicit, testable contract,
- startup/shutdown logic is less duplicated and easier to review.

---

### 3C — Container builder normalization

Updated:
- `src/platform/container.py`

Shared builder helpers introduced:
- `_new_container()`
- `_init_redis_client(settings)`
- `_init_async_db(settings)`
- `_init_queue_stack(container)`
- `_init_schedule_registry(container)`
- `_init_tenant_registry(container)`
- `_init_task_runtime_components(container, db_factory)`

Testing:
- `tests/test_container_builders.py`

Outcome:
- common infra assembly is no longer repeated across all builders,
- shared role components are clearer,
- builder methods remain explicit about ownership while carrying less duplicated plumbing.

---

## Files added

### Design / planning
- `docs/plans/2026-05-08-p3-resource-route-normalization-design.md`
- `docs/plans/2026-05-08-p3-entrypoint-container-normalization-design.md`
- `docs/plans/2026-05-08-p3-container-builder-normalization-design.md`

### Code
- `src/api/app_runtime.py`
- `src/services/resource_application.py`

### Tests
- `tests/test_app_runtime.py`
- `tests/test_container_builders.py`
- `tests/test_resource_routes.py`

---

## Files changed

- `src/api/dependencies.py`
- `src/api/routes/clusters.py`
- `src/api/routes/dags.py`
- `src/api/routes/nodes.py`
- `src/api/routes/schedules.py`
- `src/main.py`
- `src/main_control.py`
- `src/main_tasks.py`
- `src/platform/container.py`

---

## What did not change

Phase 3 intentionally did **not**:
- change public API paths,
- change API response contracts materially,
- add new runtime services,
- redesign scheduler/reconciler behavior,
- widen product scope.

This was an internal quality and maintainability phase.

---

## Testing summary

Focused verification across the phase included:
- resource route tests,
- runtime helper tests,
- control-plane split/lifecycle tests,
- task route tests,
- integration e2e tests,
- container builder tests.

Representative focused runs during execution:
- 34 passed
- 38 passed
- 40 passed
- 75 passed
- 34 passed
- 34 passed

The exact composition varied by wave, but all targeted regression batches stayed green after fixes.

---

## Architectural value delivered

### 1. Lower boundary drift
Different runtime roles now express their boundaries in more uniform ways.
That reduces the chance that future work accidentally reintroduces cross-role leakage.

### 2. Lower refactor risk
Because more behavior is now expressed through helpers and service-layer seams, future changes can be made in fewer places with clearer tests.

### 3. Better testability
Previously implicit assembly behavior is now explicit and directly testable.
This is especially important for control-plane startup and container ownership rules.

### 4. Better contributor ergonomics
The codebase now has a more obvious “shape”:
- routes delegate,
- runtime helpers assemble,
- container builders describe role ownership.

That lowers onboarding and review friction.

---

## Recommended next step

Phase 3 is a good stopping point.
The recommended next action is **release closure**, not another refactor wave:
- changelog update,
- PR description,
- release note / architecture summary,
- optional full regression run before merge.
