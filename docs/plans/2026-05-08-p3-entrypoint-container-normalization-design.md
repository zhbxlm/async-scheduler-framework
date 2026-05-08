# P3 Entrypoint / Container Normalization Design

Date: 2026-05-08
Project: async-scheduler-framework
Status: proposed

## Context

After resource-route normalization, the next remaining architectural inconsistency sits at the process entrypoint layer:
- `src/main.py` (ops-api)
- `src/main_tasks.py` (task-api)
- `src/main_control.py` (control-plane)

These files all implement overlapping startup/shutdown concerns:
- configure logging/tracing,
- initialize shared HTTP client,
- build a `ServiceContainer`,
- mount container-owned objects onto `app.state` or lifecycle manager,
- start/stop lifecycle-managed resources,
- close tracing / DB / HTTP clients.

The current code works, but the assembly pattern is duplicated and slightly divergent.
This makes future boundary changes riskier because each entrypoint must be edited independently.

## Goal

Normalize process assembly so that:
- each entrypoint declares its role clearly,
- common startup/shutdown patterns are shared,
- container-to-app-state mounting is explicit and standardized,
- task-api / ops-api / control-plane boundaries are easier to verify.

## Non-goals

This phase does **not**:
- merge the three entrypoints into one runtime,
- redesign `ServiceContainer` into a framework-level DI system,
- alter which runtime objects belong to task-api vs ops-api vs control-plane,
- change public API behavior.

## Observed issues

### 1. Repeated startup/shutdown scaffolding
`main.py` and `main_tasks.py` duplicate:
- init/close shared HTTP client,
- build container,
- set global container,
- mount app.state,
- start/stop lifecycle manager,
- shutdown tracing.

### 2. App-state mounting is ad hoc
Each entrypoint manually assigns a custom set of state fields.
This is correct, but not standardized or centrally described.

### 3. Control-plane lifecycle wiring is special-case but implicit
`main_control.py` must register background resources with `LifecycleManager`, but the relationship between container-owned objects and manager-owned runtime resources is not expressed through a reusable helper.

### 4. Boundary verification is harder than necessary
Because the mounting/wiring pattern is open-coded, tests have to infer intent from implementation details instead of checking a normalized contract.

## Recommended approach

### Option A — Extract shared entrypoint helpers (recommended)
Add a small helper module, for example:
- `src/api/app_runtime.py`

Responsibilities:
- initialize/close shared HTTP client,
- build container and set global container,
- mount named attributes from container onto `app.state`,
- start/stop lifecycle manager when present,
- optionally dispose async engine,
- keep control-plane-specific resource registration separate but compatible.

Why this is best:
- low risk,
- removes duplication immediately,
- preserves current architecture,
- provides a stable place for future boundary assertions.

### Option B — Push mounting logic into `ServiceContainer`
Example: `container.mount_task_api_state(app.state)` / `container.mount_ops_api_state(app.state)`.

Why not now:
- stronger coupling between container and FastAPI runtime objects,
- less clear separation between dependency assembly and framework integration,
- slightly harder to test in isolation.

### Option C — Introduce full runtime descriptor objects
Example: `TaskApiRuntime`, `OpsApiRuntime`, `ControlPlaneRuntime`.

Why not now:
- over-designed for the current codebase,
- unnecessary before the assembly contract itself is standardized.

## Decision

Use **Option A**.

Keep `ServiceContainer` focused on assembling dependencies.
Use a small runtime helper layer for:
- app-state mounting,
- common startup/shutdown orchestration,
- entrypoint contract normalization.

## Proposed implementation

### 1. Add shared runtime helper module
Create `src/api/app_runtime.py` with helpers such as:
- `init_shared_runtime()`
- `shutdown_shared_runtime()`
- `mount_container_state(app, container, names)`
- `start_container_lifecycle(container)`
- `stop_container_lifecycle(container, drain_timeout=None)`

Keep them explicit and minimal.

### 2. Standardize app.state mounting lists
Represent mounted state names as explicit tuples in each entrypoint.
Example:
- ops-api mounts registries/loaders/queue manager
- task-api mounts queue/task/db/auth settings

This makes boundary review simple and visible.

### 3. Keep control-plane special behavior explicit
Add one helper for registering control-plane lifecycle resources from a built container, but keep the actual resource list visible in `main_control.py`.
This balances reuse and readability.

### 4. Add tests for normalized runtime contract
Add focused tests ensuring:
- helper mounts only declared attributes,
- task-api and ops-api continue exposing expected state names,
- control-plane helper registers the expected resource types under the right settings.

## Execution plan

### Task 1 — Shared helper extraction
- Add `src/api/app_runtime.py`
- Move common container/app-state/lifecycle startup logic there
- No behavior change intended

### Task 2 — Entrypoint adoption
- Refactor `main.py` and `main_tasks.py` to use shared helpers
- Keep visible mount lists in each file
- Preserve MySQL fail-fast in task-api

### Task 3 — Control-plane normalization
- Extract helper for background resource registration/start/stop flow
- Refactor `main_control.py` to use it

### Task 4 — Verification
- Add focused tests for helper/module contracts
- Run focused regression suite

## Success criteria

- `main.py` and `main_tasks.py` no longer duplicate shared runtime startup/shutdown scaffolding.
- Mounted app.state attributes are declared explicitly and consistently.
- Control-plane lifecycle registration is clearer and easier to test.
- No public runtime/API behavior changes.
- Existing and new focused tests remain green.
