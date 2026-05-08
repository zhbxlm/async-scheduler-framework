# Package Independence Shared Core Design (Plan B, next step)

Date: 2026-05-08
Project: async-scheduler-framework
Status: proposed

## Conclusion from control-plane migration waves

The control-plane package has already completed two important decoupling steps:
1. it no longer bridges to `src.main_control`,
2. it no longer bridges to `src.runtime_control_plane`.

However, its package-local runtime still depends on shared monorepo layers:
- `config.settings_pydantic`
- `src.api.app_runtime`
- `src.common.*`
- `src.platform.*`
- `src.services.*`

This shows that continuing to push package independence by copying more `src.*`
modules into the package would quickly become an uncontrolled duplication effort.

## Decision

For Plan B to scale correctly, the next architectural step should be:

> introduce a **shared publishable runtime/core package layer**
> instead of continuing package-local vendoring of framework internals.

## Why this is necessary

### Without shared core package
If we continue package-by-package vendoring:
- control-plane will need more of `src.common`, `src.platform`, `src.services`, `config`
- task-api and ops-api will require even larger slices
- code drift becomes likely
- dependency declarations become fragmented and harder to reason about

### With shared core package
We can separate concerns cleanly:
- **shared core/runtime library** = reusable implementation
- **role packages** = thin, truly independent runtime entrypoints + role-specific dependency surface

This is the only scalable Plan B path.

## Proposed package model

### Shared package (new)
Candidate name:
- `async-scheduler-runtime-core`

Contains reusable modules needed by independent role packages:
- settings/config loading
- logging/tracing bootstrap
- app/runtime lifecycle helpers
- container builders
- registries/services/platform primitives
- shared models/common utilities

### Role packages (depend on core)
- `async-scheduler-control-plane`
- `async-scheduler-task-api`
- `async-scheduler-ops-api`
- `async-scheduler-agent`

Each role package contains:
- package-local runtime/app wiring
- CLI/server entrypoints
- only the role-specific top-level dependency declarations and extras

## Migration strategy

### Phase B1 — create shared core package
Initial scope:
- package enough shared implementation so that `control-plane` can stop importing `src.*` and `config.*`
- do not migrate task-api / ops-api yet

### Phase B2 — migrate control-plane to depend on shared core
Target:
- no `src.*` imports in `packages/control-plane`
- no `config.*` imports in `packages/control-plane`
- imports come from shared core package namespace only

### Phase B3 — migrate task-api
### Phase B4 — migrate ops-api
### Phase B5 — migrate agent

## First extraction candidate set for shared core

Minimal first extraction to support control-plane:
- settings loader (`config.settings_pydantic`)
- lifecycle helpers from `src.api.app_runtime`
- logging/tracing/http-client/bootstrap helpers from `src.common`
- async DB helpers from `src.common.async_db`
- lifecycle manager/adapters from `src.common.lifecycle`
- container builders and direct dependencies from `src.platform.container`
- control-plane services and platform loops used by `build_control_plane`

## Risk

The shared core package will initially be fairly large.
That is acceptable.
The goal of this step is not “tiny core” — it is “reusable publishable implementation layer”.
Dependency minimization at role packages becomes realistic only after this split exists.

## Success criteria

- a shared publishable core/runtime package exists,
- control-plane package imports shared core package modules instead of monorepo `src.*` / `config.*`,
- control-plane remains buildable and runnable,
- the pattern is reusable for task-api / ops-api next.

## Recommendation

Do **not** continue ad-hoc vendoring into `packages/control-plane`.
Proceed by creating a shared core/runtime package first, then re-point control-plane to it.
