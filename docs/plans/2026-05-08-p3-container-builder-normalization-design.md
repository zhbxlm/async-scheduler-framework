# P3 Container Builder Normalization Design

Date: 2026-05-08
Project: async-scheduler-framework
Status: proposed

## Context

After Phase 3A (resource-route normalization) and Phase 3B (entrypoint/runtime normalization), the next repeated architecture pattern lives inside `src/platform/container.py`.

The three container builders:
- `build_task_api()`
- `build_control_plane()`
- `build_ops_api()`

still duplicate several assembly steps:
- create `ServiceContainer` with `LifecycleManager`,
- create Redis client from settings,
- optionally initialize async DB engine/session factory,
- derive `db_factory`,
- create `CircuitBreaker`,
- create `QueueManager`.

This duplication is now the main remaining internal standardization gap.

## Goal

Reduce repeated container assembly logic while keeping builder responsibilities explicit.

## Non-goals

This phase does **not**:
- replace explicit builders with configuration-driven magic,
- hide role-specific object ownership,
- change runtime boundaries between task-api / ops-api / control-plane.

## Recommended approach

Extract a few explicit internal helper methods on `ServiceContainer`:
- `_new_container()`
- `_init_redis()`
- `_init_async_db()`
- `_init_queue_stack()`

Optionally store `db_factory` on the container if that simplifies reuse, but avoid broadening public surface unless it clearly helps.

## Design principles

- Keep role-specific builder methods readable.
- Hide only repeated plumbing, not business ownership.
- Preserve current attribute names and runtime contracts.
- Prefer private class/static helpers over generic factory frameworks.

## Proposed implementation

### Shared helpers

- `_new_container()`
  - returns `ServiceContainer(lifecycle_manager=LifecycleManager())`

- `_init_redis(settings)`
  - centralizes Redis URL extraction + `create_redis_client`

- `_init_async_db(settings)`
  - centralizes optional MySQL engine/session factory/db_factory creation
  - returns `(engine, session_factory, db_factory)`

- `_init_queue_stack(c)`
  - centralizes `CircuitBreaker` + `QueueManager`

### Builder responsibilities remain explicit

- `build_task_api()` still explicitly chooses:
  - `TenantRegistry`
  - `ScheduleRegistry`
  - `TaskCreator`
  - `TaskCompletionNode`

- `build_control_plane()` still explicitly chooses:
  - `ScheduleRegistry`
  - `TaskCreator`
  - `CompensationService`
  - `TaskReconciler`
  - `TaskCompletionNode`
  - `CallbackDispatchService`
  - `CronScheduler`

- `build_ops_api()` still explicitly chooses:
  - `CapabilityRegistry`
  - `ClusterRegistry`
  - `NodeRegistry`
  - `ScheduleRegistry`
  - `TenantRegistry`
  - `DagLoader`

## Verification plan

Add focused tests that verify:
- `_new_container()` still provides a lifecycle manager through builders,
- builders still attach expected common infra (`redis_client`, `queue_manager`, `circuit_breaker`),
- builders preserve role-specific object ownership.

## Success criteria

- shared infra assembly code is no longer repeated across all three builders,
- builder methods become shorter and easier to compare,
- all focused tests remain green,
- no public behavior change.
