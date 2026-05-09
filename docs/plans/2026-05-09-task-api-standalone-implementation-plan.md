# Task API Standalone Implementation Plan

Date: 2026-05-09  
Status: In Progress  
Depends on: `docs/plans/2026-05-09-standalone-services-migration-design.md`

## Goal

Turn `packages/task-api` into the first canonical standalone service pattern by moving task-api runtime ownership out of monorepo `src.*` and into package-owned modules, while keeping behavior stable.

## Task 1 — Package-owned route/runtime ownership
- Move task/health/metrics route modules into `scheduler_task_api.routes`
- Add `scheduler_task_api.runtime`
- Rewire package `_internal.py` to use package-owned route/runtime modules
- Verify targeted tests pass

## Task 2 — Package-owned lifespan module
- Add `scheduler_task_api.lifespan`
- Move task-api lifespan logic out of `runtime-core` shared service wiring
- Keep startup/shutdown behavior unchanged
- Verify health + startup tests pass

## Task 3 — Package-owned container composition shim
- Add `scheduler_task_api.container`
- Move task-api-specific container build orchestration behind package-owned API
- Minimize direct dependence on monorepo `ServiceContainer.build_task_api`
- Verify task-api app creation still passes tests

## Task 4 — Make runtime-core stop owning task-api service composition
- Simplify/remove `scheduler_runtime_core.task_api` service-specific composition ownership
- Keep only reusable primitives in shared layer
- Verify targeted tests and imports pass

## Task 5 — Add standalone smoke test
- Add a package-level smoke test proving package app imports from package namespace
- Assert no dependency on monorepo app entrypoint import path
- Verify targeted test suite passes

## Task 6 — Documentation + progress note
- Update migration design / package runtime docs with actual progress state
- Record task-api as reference standalone-service pattern (phase-2 partial completion)
- Verify docs and code are consistent

## Verification
- `python -m compileall packages/task-api/src`
- `pytest -q tests/test_health_api.py`
- `pytest -q tests/test_control_plane_split.py`
- package-specific new smoke tests


## Progress Update — Phase 2 / End-State Migration (2026-05-09)

### What is effectively complete for task-api
Task-api now owns, under `packages/task-api/src/scheduler_task_api/`, the canonical implementations or package-local ownership entrypoints for:

- app / lifespan / runtime / container composition
- routes
- auth / dependencies
- task models / task state machine
- application services / validation
- middleware / metrics / error handling
- async DB helpers / db utils / lifecycle
- base registry / tenant registry / schedule registry
- circuit breaker / queue manager / task creator / task completion node
- deeper dependency entrypoints for transaction / callback outbox / run tracking / task timeline

### Remaining tail dependencies
The remaining gaps are mostly deep shared implementation tails and compatibility glue, not primary service ownership gaps:

- redis client implementation body still transitional wrapper
- transaction / callback outbox / run tracking / task timeline still transitional wrappers
- container global `set_container` compatibility path remains

### Status decision
For migration planning purposes, `task-api` is now considered:

- **phase-complete as the reference standalone-service pattern**, and
- suitable to use as the template for `ops-api` end-state migration

Further task-api cleanup remains valuable, but now has lower ROI than starting `ops-api` replication.
