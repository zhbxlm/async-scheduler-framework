# Architecture Remediation Backlog — 2026-05-08

Project: async-scheduler-framework
Status: in progress
Owner: OpenClaw agent session

## Goals

Continue architecture cleanup without destabilizing the current production-ready split:
- `ops-api`
- `task-api`
- `control-plane`

Primary focus:
1. tighten runtime boundaries
2. reduce hidden global state
3. move orchestration responsibility out of oversized route files
4. formalize optional/runtime-enhancement capabilities

---

## Principles

- Prefer small, reversible refactors.
- Keep full test suite green after every meaningful step.
- Do not widen behavior changes unless they are required for boundary correctness.
- Document architectural intent before and during implementation.

---

## Backlog

### P1.1 LifecycleManager: remove global singleton

**Problem**
`LifecycleManager` was process-global via `get_lifecycle_manager()`, which blurred app/process boundaries and weakened test isolation.

**Target state**
Each runtime entrypoint owns its own lifecycle manager via `ServiceContainer`.

**Implementation steps**
- [x] Remove global singleton from `src/common/lifecycle.py`
- [x] Add `lifecycle_manager` field to `ServiceContainer`
- [x] Instantiate one manager per container builder
- [x] Update `main.py`, `main_tasks.py`, `main_control.py` to use container-local manager
- [x] Update singleton-oriented tests
- [x] Run targeted + full regression suite

**Result**
Completed on 2026-05-08. Full suite remains green.

---

### P1.2 Task API container: remove control-plane-only runtime objects

**Problem**
`build_task_api()` still assembles background/control-plane class graph (`CronScheduler`, `TaskReconciler`, `CompensationService`, etc.), even when `task-api` should only serve synchronous request/response responsibilities.

**Target state**
`task-api` container includes only request-serving dependencies.
`control-plane` owns long-running background loops and their dependent services.

**Expected keepers in task-api**
- Redis client
- DB engine / async session factory
- tenant registry
- schedule registry only if request-serving paths truly need it
- queue manager
- task creator
- task completion / task query dependencies only if request-serving paths depend on them

**Expected removals from task-api (unless proven necessary)**
- [x] `CronScheduler`
- [x] `TaskReconciler`
- [x] `CompensationService`
- [x] `CallbackDispatchService`

**Execution plan**
- [x] Map actual task-api runtime usages from `app.state` and route/service paths
- [x] Remove unused background objects from `build_task_api()`
- [x] Remove corresponding lifespan registration logic from `main_tasks.py`
- [ ] Verify task API routes still function under tests
- [ ] Run targeted + full regression suite

**Stop/confirm condition**
Pause only if a background object is required by a request-serving path and boundary change would alter externally visible behavior.

---

### P2.1 Split oversized `ops.py`

**Problem**
`src/api/routes/ops.py` aggregates too many concerns:
- overview/stats
- dashboard
- callbacks/dead letters
- replay
- force operations
- audit/query helpers

**Target state**
Smaller routers grouped by operational concern.

**Planned split**
- [ ] `ops_overview_routes.py`
- [ ] `ops_dashboard_routes.py`
- [ ] `ops_callback_routes.py`
- [ ] `ops_replay_routes.py`
- [ ] `ops_force_routes.py`

**Execution plan**
- [x] Move endpoints without semantic changes
- [x] Keep prefixes and schemas stable
- [x] Rewire imports in `main.py`
- [ ] Run targeted + full regression suite

---

### P2.2 Slim down `tasks.py`

**Problem**
`src/api/routes/tasks.py` still contains query logic, mapping logic, and direct ORM access that should live below the route layer.

**Target state**
Route layer handles only:
- request parsing
- auth/tenant context
- service delegation
- response wiring

**Planned extraction**
- [ ] `TaskQueryService`
- [ ] `TaskAccessPolicy` (if needed)
- [ ] `TaskPresenter` / mapping helpers

**Execution plan**
- [x] Extract list/get/result/cancel read paths first
- [x] Keep API schema and behavior unchanged
- [ ] Run targeted + full regression suite

---

### P3.1 Formalize optional capabilities

**Problem**
Capabilities like tracing and proxy HTTP server support are coded as optional/degradable but are not yet fully formalized in dependency/documentation policy.

**Target state**
Every optional capability is clearly categorized:
- core required
- optional extra
- runtime integration add-on

**Execution plan**
- [x] Inventory optional runtime features
- [x] Decide dependency ownership per package
- [x] Align docs + package metadata + runtime fallback behavior

---

### P4.1 Explicit shutdown phases

**Problem**
`LifecycleManager.stop_all()` currently infers shutdown order from resource names/types heuristically.

**Target state**
Each resource explicitly declares shutdown phase/order.

**Execution plan**
- [ ] Add explicit phase/priority field to `ManagedResource`
- [ ] Preserve current behavior as baseline
- [ ] Update tests to assert explicit ordering

---

## Execution order

1. P1.1 LifecycleManager singleton removal
2. P1.2 Task API container boundary tightening
3. P2.1 Split `ops.py`
4. P2.2 Slim down `tasks.py`
5. P3.1 Optional capability formalization
6. P4.1 Explicit shutdown phases

---

## Progress log

### 2026-05-08
- Created backlog document.
- Completed P1.1 LifecycleManager singleton removal.
- Next item: P1.2 Task API container boundary tightening.
