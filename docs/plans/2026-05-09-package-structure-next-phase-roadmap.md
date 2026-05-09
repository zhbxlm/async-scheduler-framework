# Package Structure Next-Phase Roadmap (Post v1.2.0)

Date: 2026-05-09
Status: Proposed
Scope: async-scheduler package architecture after method-B completion and v1.2.0 release

---

## Executive Summary

v1.2.0 completed the method-B split milestone:

- `runtime-core` now owns shared bootstrap/runtime support code
- `control-plane` uses `configure()` to decouple from direct `src.*` runtime imports
- `task-api` / `ops-api` / `agent` removed sys.path hacks
- package dependencies were minimized to match package-level direct imports
- all tests and CI are green

However, several structural asymmetries remain. The current state is:

- **Engineering-wise**: good and shippable
- **Packaging-wise**: improved and cleaner
- **Product semantics-wise**: still incomplete

The most important remaining issue is that several role packages are still **thin launchers over monorepo runtime implementations**, rather than truly independent runnable products.

---

## Current Problems

### Problem A — Role packages are still thin launchers, not fully independent products

Affected packages:
- `task-api`
- `ops-api`
- `agent`
- partially `control-plane`

Current reality:
- package source trees are now clean and thin
- but runtime still depends on monorepo implementation entrypoints or wiring points:
  - `src.main`
  - `src.main_tasks`
  - `src.agent.server`
  - `src.platform.container`

Consequence:
- package names suggest independent installable products
- actual behavior is closer to "thin role wrapper + monorepo implementation layer"

This is technically acceptable, but semantically misleading.

---

### Problem B — “minimum dependency” currently means source-level minimum, not product-level runnable minimum

We already minimized dependencies according to direct package imports.
That is correct from a packaging hygiene perspective.

But users may interpret package dependencies as:

> "if pip install succeeds, this role is runnable as a product"

This is not always true.

There are now two dependency concepts:

1. **source-level minimum dependency**
   - only what the package source directly imports
2. **product runnable dependency**
   - everything needed for a fully working role in real deployment

These are not yet explicitly separated in package UX.

---

### Problem C — `control-plane` is structurally ahead of the other role packages

Current maturity:

- `control-plane`
  - has shared runtime implementation in `runtime-core`
  - has `configure()`-based decoupling
  - already moved beyond thin bridge-only state

- `task-api` / `ops-api` / `agent`
  - still rely on app/server late import bridges
  - do not yet have role-specific shared runtime implementation in `runtime-core`

Consequence:
- architectural asymmetry
- uneven maintainability
- future evolution pressure will concentrate in the remaining thin-bridge packages

---

### Problem D — `runtime-core` naming is broader than its actual scope

Current contents of `runtime-core`:
- settings
- logging/tracing
- http/db helpers
- lifecycle manager and adapters
- app factory registry
- control-plane runtime bootstrap

But it still does **not** contain the business/platform runtime core:
- `src.platform.container`
- most of `src.platform.*`
- most of `src.services.*`

So the package currently behaves more like:
- runtime bootstrap foundation
- shared runtime support layer

than a complete runtime core.

This is a naming semantics issue, not a functional issue.

---

### Problem E — versioning is still manually synchronized

Current release flow required manual edits to:
- all sub-package `pyproject.toml`
- `__version__`
- internal `runtime-core>=x.y.z` constraints
- git tag

This is manageable now but scales poorly.

Risk:
- missed version bump in one package
- mismatched internal dependency lower bounds
- tag/package drift

---

## Prioritized Roadmap

---

## P1 — Product semantics cleanup (high ROI, low risk)

### P1.1 Explicitly label thin role packages as thin launchers in docs

Update package README / project descriptions for:
- `task-api`
- `ops-api`
- `agent`
- `control-plane`

Add a short, explicit section:

- what this package provides
- whether it is self-contained or requires monorepo runtime implementation
- expected deployment mode
- whether it is a thin launcher over a framework install

Goal:
- eliminate user expectation mismatch
- avoid “pip installed but why is this not standalone?” confusion

Success criteria:
- each role package README contains an explicit runtime-model section
- package purpose is unambiguous to a first-time installer

---

### P1.2 Introduce role-oriented extras for runnable installs

Keep current source-minimal dependencies, but add product-facing extras.

Recommended additions:
- `runtime-core[task-api]`
- `runtime-core[ops-api]`
- `runtime-core[control-plane]`
- `runtime-core[agent]`
- optional: package-local `[full]` extras for role packages

Example mapping:
- `task-api` runtime profile = serving + redis + mysql + metrics (+ tracing optional)
- `ops-api` runtime profile = serving + redis + mysql + metrics + cron (+ tracing optional)
- `control-plane` runtime profile = redis + mysql + cron + metrics (+ tracing optional)
- `agent` runtime profile = role-specific runtime profile once clarified

Goal:
- preserve source-level dependency hygiene
- provide product-level installation ergonomics

Success criteria:
- users can install a role via a documented profile instead of guessing transitive runtime requirements

---

### P1.3 Add an architectural terminology note

Document the distinction between:
- thin launcher package
- shared bootstrap/runtime layer
- monorepo business implementation layer
- standalone runnable role package

Goal:
- stabilize team language
- reduce future design ambiguity around “independent package” claims

Success criteria:
- one concise architecture note under `docs/` referenced by package READMEs

---

## P2 — Structural symmetry improvements (medium risk, high long-term value)

### P2.1 Lift task-api runtime wiring into `runtime-core`

Current state:
- task-api uses app factory registration + late import of `src.main_tasks`

Target:
- introduce task-api-specific runtime/app wiring module in `runtime-core`
- analogous to how `control-plane` already has a shared implementation path

Not required immediately:
- moving all routes/business services

Required:
- move runtime/bootstrap composition layer
- reduce package-local `_internal.py` to simple configuration wiring

Success criteria:
- `task-api` no longer feels architecturally "behind" `control-plane`

---

### P2.2 Lift ops-api runtime wiring into `runtime-core`

Same principle as task-api.

Goal:
- runtime-core owns shared app/bootstrap/wiring abstractions for ops-api role startup
- package-local `_internal.py` becomes a thin configuration/wiring file only

Success criteria:
- `ops-api` runtime path structurally matches `control-plane` maturity level

---

### P2.3 Lift agent runtime wiring into a shared runtime layer

Current state:
- `agent` still late-imports `src.agent.server`

Target:
- isolate agent runtime bootstrap/wiring from business logic
- move reusable startup/runtime composition into shared layer
- keep agent server implementation in business/platform layer if needed

This may result in either:
- `runtime-core.agent_runtime`
- or a dedicated `agent-runtime-core` if divergence becomes large

Success criteria:
- agent startup path no longer depends on a package-local bridge as its main abstraction

---

## P3 — Deeper packaging and release maturity (higher effort)

### P3.1 Decide whether role packages should become truly standalone products

This is the major fork-in-the-road decision.

Two valid models exist:

#### Model 1 — Thin role package model
- role packages remain light wrappers
- monorepo/framework install remains the true implementation source
- docs explicitly say so

#### Model 2 — Standalone role product model
- each role package can be installed and run independently
- all required runtime/business wiring is published through stable package dependencies
- no hidden monorepo implementation dependency remains

This decision should be explicit.

Success criteria:
- architecture doc clearly chooses one model per role
- package descriptions and dependency profiles align with that choice

---

### P3.2 Reassess `runtime-core` naming

Options:
- keep `runtime-core` and document scope carefully
- rename in next major version to something like:
  - `runtime-bootstrap-core`
  - `runtime-foundation`
  - `runtime-support-core`

Recommendation:
- do not rename immediately unless there is strong confusion
- first stabilize the scope in docs
- revisit naming only if the mismatch causes repeated misunderstanding

---

### P3.3 Centralize version management

Recommended options (pick one):

- root `VERSION` file + release script
- setuptools_scm
- hatch version management
- custom release tool that updates all package versions and internal constraints together

Minimum acceptable target:
- no future release should rely on ad hoc manual sweeping

Success criteria:
- one command performs:
  - version bump
  - internal dependency constraint bump
  - tag creation
  - optional changelog entry

---

## Suggested Execution Order

### Recommended next sequence

1. **P1.1** README/runtime-model clarification
2. **P1.2** role-oriented extras (`[task-api]`, `[ops-api]`, `[control-plane]`, `[agent]`)
3. **P1.3** architecture terminology note
4. **P2.1** task-api runtime wiring uplift
5. **P2.2** ops-api runtime wiring uplift
6. **P2.3** agent runtime wiring uplift
7. **P3.1** explicit product model decision
8. **P3.3** central version management
9. **P3.2** package renaming reconsideration (only if still needed)

---

## Recommendation

### If optimizing for immediate practical value
Do **P1 first**.

Reason:
- low cost
- low risk
- immediately improves user/developer understanding
- avoids false expectations without forcing deeper refactors now

### If optimizing for long-term architecture cleanliness
Do **P1 + P2** as the next milestone.

Reason:
- it will remove the current asymmetry where control-plane is ahead of the other role packages
- it will make the package architecture feel intentional rather than partially migrated

---

## Bottom Line

v1.2.0 solved the hardest packaging hygiene problems.

The next phase is no longer about “make it work”.
It is about making the packaging model:

- semantically honest
- symmetric across roles
- product-friendly
- maintainable for future releases
