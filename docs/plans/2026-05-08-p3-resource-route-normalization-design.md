# P3 Resource Route Normalization Design

Date: 2026-05-08
Project: async-scheduler-framework
Status: proposed

## Context

Phase 1/2 already cleaned up the most painful architectural issues:
- lifecycle ownership is container-local,
- oversized ops routes were split,
- task routes now delegate more logic to service-layer code,
- access policy primitives were extracted.

However, a remaining route family still follows the earlier architecture style:
- `src/api/routes/clusters.py`
- `src/api/routes/nodes.py`
- `src/api/routes/dags.py`
- `src/api/routes/schedules.py`

These modules still:
- read runtime objects directly from `request.app.state`,
- perform registry iteration / mutation inline,
- mix transport, access, orchestration, and persistence-facing logic in route functions,
- use tenant-resolution patterns that are not fully uniform.

This is the best target for Phase 3 because it is:
- high architectural value,
- lower risk than changing scheduler internals,
- aligned with the direction already established by `tasks.py` and the split ops routes.

## Goal

Normalize resource-oriented route families to the same layering model:
- route layer = request parsing, auth context, response wiring,
- dependency layer = runtime object retrieval from app state,
- service layer = registry/list/get/update/delete orchestration,
- access policy = shared tenant resolution rules.

## Non-goals

This phase does **not**:
- redesign registry storage schemas,
- change external API response shapes,
- merge all registries behind a generic abstraction,
- refactor scheduler/control-plane business logic.

## Design principles

1. Small, reversible refactors.
2. Preserve API behavior and test expectations.
3. Reduce repeated `request.app.state` lookups.
4. Prefer explicit service classes over generic metaprogramming.
5. Keep route modules boring and thin.

## Proposed target structure

### 1. Add route-level resource dependencies

In `src/api/dependencies.py`, add explicit dependency helpers for runtime resources:
- `get_cluster_registry()`
- `get_node_registry()`
- `get_schedule_registry()`
- `get_dag_loader()`
- optional helper aliases using `Annotated[..., Depends(...)]`

This mirrors the `DbSession` pattern and centralizes 503 initialization checks.

### 2. Add resource application services

Create `src/services/resource_application.py` with focused service classes:
- `ClusterService`
- `NodeService`
- `DagService`
- `ScheduleService`

Responsibilities:
- list/get/register/update/delete resource records,
- encapsulate registry iteration,
- apply simple mutation semantics currently embedded in routes,
- keep transport-specific HTTP handling minimal and stable.

### 3. Thin route modules

Update resource route modules so they:
- receive registry/loader objects via dependency injection,
- resolve tenant consistently through `resolve_tenant_id`,
- delegate behavior to service methods,
- keep existing endpoint paths and payload semantics unchanged.

### 4. Preserve low-risk behavior compatibility

Do not attempt in this phase to:
- genericize all CRUD flows,
- alter 404 vs 403 behavior beyond current semantics,
- introduce new schema models unless existing tests require them.

## Execution plan

### Task 1 — Dependency extraction
- Add runtime-resource dependencies in `src/api/dependencies.py`
- No route behavior change yet
- Add/adjust focused tests if needed

### Task 2 — Service extraction
- Add `src/services/resource_application.py`
- Port inline registry/list/mutation logic into services
- Keep service methods simple and explicit

### Task 3 — Route migration
- Migrate `clusters.py`, `nodes.py`, `dags.py`, `schedules.py`
- Remove route-private `_registry()` / `_loader()` helpers
- Switch to dependency injection + service delegation

### Task 4 — Verification
- Run focused route/control-plane tests first
- Run broader regression suite if focused tests pass

## Risks

### Risk 1: subtle response-shape drift
Mitigation: preserve exact dict assembly currently returned by routes.

### Risk 2: missing runtime initialization handling
Mitigation: centralize 503 checks in dependency helpers and keep exact messages where feasible.

### Risk 3: over-abstracting too early
Mitigation: use four explicit service classes instead of a generic CRUD base.

## Success criteria

- Resource routes no longer read `request.app.state.<resource>` directly.
- Registry/list/get/update/delete orchestration lives in service-layer code.
- Tenant resolution is consistent across resource routes.
- Existing tests remain green.
- No external API path or payload contract changes.

## Recommended next step after this design

Implement Tasks 1–3 as one small wave, then reassess whether Phase 3B should tackle entrypoint/container normalization or stop after route normalization if the codebase already feels coherent enough.
