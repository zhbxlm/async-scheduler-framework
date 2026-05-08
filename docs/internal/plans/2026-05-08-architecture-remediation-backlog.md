# Architecture Remediation Backlog — 2026-05-08

Project: async-scheduler-framework
Status: phase-1 complete
Owner: OpenClaw agent session

## Scope of this document

This document began as the execution backlog for the first architecture-cleanup wave.
That first wave is now considered complete.

The document is retained as:
1. a record of what was changed,
2. a checkpoint for future architecture work,
3. a staging area for second-phase candidate items.

---

## Goals of phase 1

Stabilize and simplify the codebase without changing external behavior:
- tighten runtime boundaries,
- reduce hidden global state,
- move orchestration responsibility out of oversized route files,
- formalize optional/runtime-enhancement capabilities.

---

## Principles

- Prefer small, reversible refactors.
- Keep full test suite green after every meaningful step.
- Do not widen behavior changes unless they are required for boundary correctness.
- Document architectural intent before and during implementation.

---

## Phase 1 completed items

### P1.1 LifecycleManager: remove global singleton

**Problem**
`LifecycleManager` was process-global via `get_lifecycle_manager()`, which blurred app/process boundaries and weakened test isolation.

**Target state**
Each runtime entrypoint owns its own lifecycle manager via `ServiceContainer`.

**Completed work**
- [x] Remove global singleton from `src/common/lifecycle.py`
- [x] Add `lifecycle_manager` field to `ServiceContainer`
- [x] Instantiate one manager per container builder
- [x] Update `main.py`, `main_tasks.py`, `main_control.py` to use container-local manager
- [x] Update singleton-oriented tests
- [x] Run targeted + full regression suite

**Result**
Completed on 2026-05-08. Full suite remained green.

---

### P1.2 Task API container: remove control-plane-only runtime objects

**Problem**
`build_task_api()` assembled background/control-plane class graph (`CronScheduler`, `TaskReconciler`, `CompensationService`, etc.), even when `task-api` should only serve synchronous request/response responsibilities.

**Target state**
`task-api` container includes only request-serving dependencies.
`control-plane` owns long-running background loops and their dependent services.

**Completed work**
- [x] Map actual task-api runtime usages from `app.state` and route/service paths
- [x] Remove unused background objects from `build_task_api()`
- [x] Remove corresponding lifespan registration logic from `main_tasks.py`
- [x] Verify task API routes still function under tests
- [x] Run targeted + full regression suite

**Removed from task-api container**
- [x] `CronScheduler`
- [x] `TaskReconciler`
- [x] `CompensationService`
- [x] `CallbackDispatchService`

---

### P2.1 Split oversized `ops.py`

**Problem**
`src/api/routes/ops.py` aggregated too many concerns:
- overview/stats
- dashboard
- callbacks/dead letters
- replay
- force operations
- audit/query helpers

**Target state**
Smaller routers grouped by operational concern.

**Completed work**
- [x] `ops_overview_routes.py`
- [x] `ops_dashboard_routes.py`
- [x] `ops_callback_routes.py`
- [x] `ops_replay_routes.py`
- [x] `ops_force_routes.py`
- [x] Move endpoints without semantic changes
- [x] Keep prefixes and schemas stable
- [x] Rewire imports through compatibility aggregation
- [x] Run targeted + full regression suite

**Result**
The old `ops.py` now acts as a compatibility aggregation module while endpoint definitions live in smaller route files.

---

### P2.2 Slim down `tasks.py`

**Problem**
`src/api/routes/tasks.py` contained query logic, mapping logic, and direct ORM access that should live below the route layer.

**Target state**
Route layer handles only:
- request parsing,
- auth/tenant context,
- service delegation,
- response wiring.

**Completed work**
- [x] `TaskQueryService`
- [x] `TaskAccessPolicy` (lightweight initial extraction)
- [x] `TaskPresenter` / mapping helpers (implemented as service-layer helper extraction)
- [x] Extract list/get/result/cancel read paths first
- [x] Keep API schema and behavior unchanged
- [x] Run targeted + full regression suite

**Current note**
A thin compatibility shell remains in `tasks.py` for `_to_json` / `_serialize` because current tests import those helpers directly.

---

### P3.1 Formalize optional capabilities

**Problem**
Capabilities like tracing and proxy HTTP server support were coded as optional/degradable but were not fully formalized in dependency/documentation policy.

**Target state**
Every optional capability is clearly categorized:
- core required,
- optional extra,
- runtime integration add-on.

**Completed work**
- [x] Inventory optional runtime features
- [x] Decide dependency ownership per package
- [x] Align docs + package metadata + runtime fallback behavior

**Result**
- Root package now declares tracing as an optional `tracing` extra.
- README documents tracing as optional with graceful degradation.
- `packages/proxy` explicitly declares `flask`.
- `packages/task-api` and `packages/ops-api` align tracing as an optional extra rather than incomplete hard dependency.

---

### P4.1 Explicit shutdown phases

**Problem**
`LifecycleManager.stop_all()` previously inferred shutdown order from resource names/types heuristically.

**Target state**
Each resource explicitly declares shutdown phase/order.

**Completed work**
- [x] Add explicit phase/priority field to `ManagedResource`
- [x] Preserve current behavior as baseline
- [x] Update tests to assert explicit ordering

**Result**
Implementation now uses explicit `shutdown_phase` first, with heuristic fallback retained for compatibility.

---

## Phase 1 completion summary

Phase 1 is considered complete because it achieved all high-value, low-risk architecture cleanup items originally targeted:
- runtime boundaries tightened,
- global lifecycle state removed,
- oversized route modules split,
- task route orchestration thinned,
- optional capability policy clarified,
- shutdown ordering made explicit.

All major steps were validated by targeted regression and full test-suite runs.

---

## Phase 2 candidate work items

These are not required to claim phase 1 success. They are candidates for future work.

### Phase 2A — Access policy unification

**Why**
Access and tenant/super-admin checks still exist across multiple route families (`tasks`, `tenants`, some ops entrypoints).

**Candidate items**
- [ ] Extract broader access policy primitives beyond task access
- [ ] Unify tenant access rules across `tenants.py` and other route modules
- [ ] Decide and document 403 vs 404 masking policy consistently

**Priority**
High, if auth/tenant complexity grows.

---

### Phase 2B — Route/service layering normalization

**Why**
The main oversized modules were reduced, but service/presenter layering could still become more uniform across route families.

**Candidate items**
- [ ] Reduce `request.app.state` lookup boilerplate in route modules
- [ ] Normalize query-service / presenter patterns across `ops` and `tasks`
- [ ] Decide whether route-private helper compatibility shims should remain or be removed

**Priority**
Medium.

---

### Phase 2C — Packaging and release model formalization

**Why**
Root package and subpackages are now cleaner, but product/release boundaries can be further clarified.

**Candidate items**
- [ ] Define root package vs subpackage support expectations explicitly
- [ ] Publish capability/install matrix (core vs extras vs subpackage-only)
- [ ] Prepare release notes / changelog / distribution guidance

**Priority**
Medium-high for release preparation.

---

### Phase 2D — Observability strategy refinement

**Why**
Tracing is now optional and documented, but broader observability strategy can be refined if needed.

**Candidate items**
- [ ] Decide whether subpackages should all expose the same optional tracing contract
- [ ] Document observability profiles (minimal / production / tracing-enabled)
- [ ] Review whether instrumentation hooks should be centralized further

**Priority**
Medium.

---

## Recommended order for phase 2

1. Access policy unification
2. Route/service layering normalization
3. Packaging and release model formalization
4. Observability strategy refinement

---

## Progress log

### 2026-05-08
- Created backlog document.
- Completed P1.1 LifecycleManager singleton removal.
- Completed P1.2 task-api container boundary tightening.
- Completed P2.1 first-round `ops.py` route split into overview/dashboard/callback/replay/force modules.
- Completed P2.2 first-round `tasks.py` slimming: query paths and mapping helpers moved below route layer while preserving route compatibility shims used by tests.
- Completed P3.1 first-round optional capability formalization for tracing via `tracing` extra + README guidance.
- Completed P4.1 explicit shutdown phase introduction with compatibility fallback.
- Marked phase 1 complete and converted remaining work into phase-2 candidate items.
