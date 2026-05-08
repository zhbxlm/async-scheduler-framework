# PR Description — Phase 3 architecture normalization

## Summary

This PR completes the Phase 3 architecture cleanup wave for `async-scheduler-framework`.

The focus is not new user-facing features; it is **boundary normalization** and **assembly standardization** across three layers:

1. resource-oriented route modules,
2. runtime entrypoints,
3. service container builders.

The net effect is a codebase that is more uniform, easier to extend, and easier to test, while preserving existing API/runtime behavior.

---

## What changed

### 1) Resource route normalization

Added a dedicated resource application layer:
- `src/services/resource_application.py`

Refactored these route modules to delegate orchestration into services:
- `src/api/routes/clusters.py`
- `src/api/routes/nodes.py`
- `src/api/routes/dags.py`
- `src/api/routes/schedules.py`

Extended dependency extraction in:
- `src/api/dependencies.py`

Result:
- route handlers are thinner,
- `app.state` access is more standardized,
- resource routes now follow the same layering direction as `tasks` and split `ops` routes.

### 2) Entrypoint/runtime normalization

Added shared runtime helpers in:
- `src/api/app_runtime.py`

Refactored:
- `src/main.py`
- `src/main_tasks.py`
- `src/main_control.py`

Result:
- startup/shutdown flow is less duplicated,
- app.state mounting is more explicit,
- control-plane background resource registration is now a clearer, testable contract.

### 3) Container builder normalization

Refactored:
- `src/platform/container.py`

Introduced shared private helpers for repeated builder plumbing:
- container creation
- redis init
- async db init
- queue stack init
- schedule/tenant/task runtime component init

Result:
- common infra assembly is no longer copy-pasted across builders,
- role-specific ownership remains explicit.

---

## New tests

Added:
- `tests/test_resource_routes.py`
- `tests/test_app_runtime.py`
- `tests/test_container_builders.py`

These tests cover the newly explicit contracts introduced by the refactor.

---

## Why this matters

Before this PR, the system worked, but architectural patterns still varied between route families, entrypoints, and builders.
That inconsistency increased maintenance cost and made future boundary changes riskier.

This PR reduces that risk by making the codebase more regular:
- routes delegate more consistently,
- runtime assembly follows shared helpers,
- container builders share infra plumbing without obscuring role ownership.

---

## Behavior impact

Expected user-facing impact:
- **none**

Expected operator/runtime impact:
- **none intended**

This is an internal cleanup / maintainability PR.

---

## Validation

Focused regression batches passed throughout implementation, including:
- resource route tests,
- runtime helper tests,
- container builder tests,
- task route tests,
- control-plane split/lifecycle tests,
- integration e2e tests.

---

## Follow-up

Recommended next step after this PR:
- release closure / changelog update / summary materials,
- optionally a full regression run before merge.
