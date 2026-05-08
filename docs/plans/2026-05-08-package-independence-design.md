# Package Independence Design (Plan B)

Date: 2026-05-08
Project: async-scheduler-framework
Status: proposed

## Goal

Convert deployable sub-packages from bridge wrappers into **truly independent distributable packages**.

That means each package must:
- stop importing `src.main*` or other monorepo-only entrypoints at runtime,
- own its own application/runtime factory and startup wiring,
- declare only the dependencies required by its actual runtime role,
- be installable and runnable without the repository root being present on `sys.path`.

## Why the current bridge model is insufficient

Current deployable packages (`task-api`, `ops-api`, `control-plane`, `agent`) are not truly independent.
They expose their own package names and CLI entrypoints, but at runtime they bridge back to monorepo internals such as:
- `src.main`
- `src.main_tasks`
- `src.main_control`
- `src.agent.server`

This causes two structural problems:
1. dependency declarations are difficult to minimize accurately,
2. the package is not truly standalone because it still depends on monorepo-local code layout.

## Non-goals

This phase does not attempt to:
- fully split the repository into separate repos,
- eliminate code reuse across runtime roles,
- redesign platform architecture.

## Core design decision

Use a **shared reusable runtime library layer** plus **independent package-local entrypoints**, instead of bridge wrappers.

### Meaning in practice

We will not duplicate full logic into each package.
Instead we will:
1. identify reusable runtime-building modules under shared code,
2. move entrypoint-specific startup wiring into reusable factories/functions,
3. let each package import those reusable runtime modules through its own namespace/package layout,
4. remove `sys.path` hacks and bridge-to-`src.main*` entrypoints.

## Recommended migration order

### Step 1 — control-plane (first target)

Why first:
- clearest runtime boundary,
- no public HTTP API surface,
- easiest place to validate package independence.

Target outcome:
- `packages/control-plane` owns its own runtime startup function and CLI,
- no import of `src.main_control`,
- package dependencies align to actual control-plane runtime only.

### Step 2 — task-api

Why second:
- single HTTP service role,
- cleaner than ops-api,
- already reasonably isolated from control-plane loops.

Target outcome:
- `packages/task-api` owns its own app factory + server startup,
- no import of `src.main_tasks`.

### Step 3 — ops-api

Why third:
- broadest route surface,
- depends on more registries and ops modules,
- highest chance of hidden coupling.

Target outcome:
- `packages/ops-api` owns its own app factory + server startup,
- no import of `src.main`.

### Step 4 — agent

Why last:
- can likely reuse more direct agent-side logic,
- but it still bridges into monorepo runtime pieces today.

Target outcome:
- `packages/agent` runs without `src.agent.server` bridge imports.

## Refactoring pattern

For each independent runtime package:

1. Extract reusable shared runtime setup into importable modules
   - logging/tracing/bootstrap helpers
   - container building helpers
   - route registration helpers
   - lifecycle helpers

2. Create package-local entrypoint implementation
   - `scheduler_<role>.app`
   - `scheduler_<role>.server`
   - optional package-local runtime bootstrap module

3. Remove `_internal.py` bridge layer
   - no `sys.path` modification
   - no importing monorepo `src.main*`

4. Recompute dependencies from actual imports

## Architecture options

### Option A — duplicate full entrypoint logic into packages

Pros:
- straightforward
- minimal shared refactor upfront

Cons:
- high duplication
- drift risk
- future maintenance tax

### Option B — shared runtime modules + package-local entrypoints (recommended)

Pros:
- preserves reuse
- produces true standalone packages
- dependency boundaries easier to reason about

Cons:
- requires deliberate extraction work first

## First-wave implementation recommendation

Only do **control-plane independence** in the first execution wave.
That gives the team a proven pattern before touching task-api/ops-api.

### Control-plane wave tasks

1. Design shared runtime bootstrap contract for non-HTTP workers
2. Move reusable control-plane startup wiring into shared importable module(s)
3. Rebuild `packages/control-plane` to call that runtime directly
4. Remove package bridge `_internal.py`
5. Recompute dependencies
6. Build + smoke-test the package

## Success criteria

### For control-plane package
- no import of `src.main_control`
- no `sys.path` injection hack
- package builds successfully
- package CLI starts the intended runtime role
- dependency declaration matches actual runtime needs more closely

### For overall plan
- establish a repeatable pattern to migrate task-api and ops-api next
- avoid large duplication explosion

## Risks

1. hidden imports through shared modules may keep dependency sets larger than expected
2. rushing task-api/ops-api in the same wave would create too much surface area
3. partial extraction without tests may create package/runtime drift

## Recommendation

Proceed with **Plan B**, but do it incrementally:
- first make `control-plane` truly independent,
- validate pattern,
- then migrate `task-api` and `ops-api` using the same structure.
