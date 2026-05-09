# Standalone Services Migration Design

Date: 2026-05-09  
Status: Proposed  
Scope: Evolve async-scheduler from thin role packages to truly standalone service products

---

## 1. Goal

Move from the current model:

- monorepo as the real implementation source
- role packages as thin launchers / packaging facades

To the target model:

- every service is a **self-contained runnable product package**
- monorepo becomes an **integration / assembly workspace**, not the hidden runtime source
- shared logic is published through **narrow, explicit shared packages**

Target standalone services:

- `task-api`
- `ops-api`
- `control-plane`
- `agent`

---

## 2. Non-goals

This migration does **not** require:

- immediately splitting into multiple git repositories
- fully independent business evolution on day 1
- changing external API contracts
- changing storage model (Redis / MySQL / Ray) unless needed for service boundaries

This migration is about **runtime/package independence**, not repository politics.

---

## 3. Current Problems

### 3.1 Package semantics are still misleading

Current role packages look like independent services, but still depend on monorepo implementation layers.

Examples of hidden dependence patterns:

- service bootstrap depends on `src.*`
- route ownership still belongs to monorepo modules
- service container composition is centralized in shared monorepo code
- packaged service runtime is not the canonical implementation owner

### 3.2 `runtime-core` is becoming a bridge concentrator

The current direction improves packaging hygiene, but if continued indefinitely it risks turning `runtime-core` into:

- half shared runtime
- half compatibility bridge
- half service composition layer

That usually becomes a maintenance trap.

### 3.3 Release semantics remain weaker than product semantics

A package should mean:

> installable + runnable + testable + releaseable as a product unit

Today, role packages are closer to:

> installable launcher + monorepo-backed implementation

---

## 4. Target Architecture

Use a **3-layer architecture**.

### 4.1 Layer A — Shared Foundation Packages

These packages contain only broadly reusable capabilities.

Examples:

- `async-scheduler-foundation`
  - settings primitives
  - config loading helpers
  - lifecycle abstractions
  - shared error primitives
  - HTTP client helpers
  - DB helpers

- `async-scheduler-observability`
  - logging setup
  - tracing setup
  - metrics helpers

- `async-scheduler-contracts`
  - shared models / DTOs / schemas / enums
  - public interfaces used across services

- `async-scheduler-sdk`
  - Python client / service-to-service access helpers

Rules:

- no service-specific route registration
- no service-specific FastAPI app factories
- no hidden imports from monorepo service code

---

### 4.2 Layer B — Standalone Service Packages

Each service owns its runnable implementation.

For each service package, ownership should include:

- app factory
- startup / shutdown / lifespan
- route registration
- service-specific container composition
- service-specific config validation
- entrypoints / CLI
- runtime dependency declaration
- service smoke tests

Target packages:

#### `async-scheduler-task-api`
Owns:
- task-facing FastAPI app
- task route registration
- task-api service container composition
- task-api auth/runtime validation

#### `async-scheduler-ops-api`
Owns:
- ops/admin FastAPI app
- ops route registration
- ops-api service container composition
- ops runtime wiring

#### `async-scheduler-control-plane`
Owns:
- reconcile / compensation / callback dispatch loops
- control-plane resource lifecycle
- control-plane runtime composition

#### `async-scheduler-agent`
Owns:
- node agent server
- agent config/runtime bootstrap
- package deploy/undeploy ownership
- heartbeat / ownership protocol runtime

Rule:

> A standalone service package must not require importing monorepo `src.*` to run.

---

### 4.3 Layer C — Monorepo Assembly / Integration Layer

The monorepo remains useful, but only for:

- local development
- all-in-one demos
- cross-service integration tests
- docs/examples
- unified CI orchestration

Rules:

- monorepo may compose service packages
- service packages must not require monorepo runtime modules as hidden dependencies

In other words:

- monorepo depends on services
- services do not depend on monorepo

---

## 5. Target Dependency Direction

Correct dependency direction:

```text
shared foundation  ->  standalone services  ->  monorepo assembly
```

Forbidden direction:

```text
standalone service  ->  monorepo src.*
```

Forbidden anti-patterns:

- service package imports `src.main*`
- service package imports `src.agent.server`
- service package runtime depends on a monolithic global container defined only in monorepo
- package “full” install still requires unpublished local source layout assumptions

---

## 6. Service Boundary Rules

To make services truly independent, adopt these rules.

### Rule 1 — Each service owns its app/runtime composition

Each service package must own:

- `create_app()` or equivalent runtime entry
- service lifespan/startup/shutdown
- route mounting
- middleware mounting
- exception handler wiring

Shared packages may provide helpers, but the service package owns the final composition.

### Rule 2 — Shared packages expose primitives, not hidden concrete runtime graphs

Allowed shared code:

- logging/tracing helpers
- DB session helpers
- Redis client helpers
- lifecycle manager abstractions
- shared schemas / contracts

Discouraged shared code:

- giant central `ServiceContainer` as the only implementation owner
- service-specific route registration living outside the service package
- generic bridge registries that exist only to hide monorepo coupling

### Rule 3 — Each service declares runnable dependencies honestly

If a service needs:

- FastAPI
- uvicorn
- redis
- aiomysql
- prometheus

then those must be declared by the service package (directly or via explicit shared extras with transparent meaning).

### Rule 4 — Every service must pass an isolated smoke test

For every service package, CI must validate:

- package installs in a clean env
- app/CLI imports without monorepo path tricks
- health endpoint or startup path works
- service-specific runtime validation is correct

### Rule 5 — Monorepo is not the canonical owner of service runtime

If a service package exists, then the service package should be the canonical owner of:

- its startup path
- its runtime graph
- its dependency truth

---

## 7. Recommended Package Layout

One practical target shape:

```text
packages/
  foundation/
    src/scheduler_foundation/
  observability/
    src/scheduler_observability/
  contracts/
    src/scheduler_contracts/
  sdk/
    src/scheduler_sdk/

  task-api/
    src/scheduler_task_api/
      app.py
      lifespan.py
      routes/
      container.py
      config.py
      server.py

  ops-api/
    src/scheduler_ops_api/
      app.py
      lifespan.py
      routes/
      container.py
      config.py
      server.py

  control-plane/
    src/scheduler_control_plane/
      runtime.py
      resources.py
      container.py
      server.py

  agent/
    src/scheduler_agent/
      app.py
      runtime.py
      config.py
      node.py
      server.py
      cli.py
```

Notes:

- route modules live with the service that owns them
- service container composition lives with the service
- only low-level reusable primitives remain in shared packages

---

## 8. Migration Strategy

Recommended strategy: **strangler-fig migration by service**.

Do not rewrite everything at once.

### Phase 0 — Architecture commitment

Decision:
- adopt **Standalone Role Product Model**

Required doc changes:
- package runtime model doc updated to say the target model is standalone services
- current state marked transitional

Success criteria:
- team agrees that packaged services become canonical runtime owners

---

### Phase 1 — Extract shared foundation deliberately

Before moving service code, define what is truly shared.

Keep shared:
- logging/tracing/metrics helpers
- DB/HTTP/lifecycle helpers
- contracts/models/interfaces used by multiple services

Do **not** keep shared:
- task-api-specific startup
- ops-api-specific route ownership
- agent-specific runtime behavior

Success criteria:
- shared packages contain primitives, not service implementations in disguise

---

### Phase 2 — Make `task-api` the first true standalone service

Why first:
- cleanest HTTP boundary
- easiest to define smoke tests
- high user-facing value

Migration steps:
1. move task-api app composition into package-owned modules
2. move task-api route ownership into package namespace
3. move task-api container composition into package-owned code
4. eliminate runtime imports of monorepo `src.main_tasks`
5. add isolated install/start/smoke CI

Success criteria:
- `async-scheduler-task-api` runs from package code only
- no runtime import of monorepo `src.*`

---

### Phase 3 — Make `ops-api` standalone

Migration steps mirror task-api:
1. move ops route ownership into package
2. move ops container/wiring into package
3. remove dependence on monorepo app assembly
4. add isolated smoke CI

Success criteria:
- `async-scheduler-ops-api` becomes canonical owner of ops runtime

---

### Phase 4 — Make `control-plane` standalone

Current state is already partially closer.

Migration targets:
- control-plane runtime composition fully package-owned
- callback/reconcile/compensation wiring owned by package
- any remaining monorepo-only lifecycle assumptions removed

Success criteria:
- control-plane package can run independently with only published dependencies

---

### Phase 5 — Make `agent` standalone

Why later:
- deployment/runtime concerns are more environment-coupled
- node lifecycle + Ray interactions are more operationally sensitive

Migration steps:
1. move agent runtime/bootstrap fully into package
2. make `NodeAgentServer` package-owned canonical runtime
3. remove monorepo fallback imports
4. add standalone startup + behavior smoke tests

Success criteria:
- agent package is fully self-contained

---

### Phase 6 — Shrink monorepo assembly layer

Once services are independent:
- monorepo app entrypoints become wrappers or integration examples only
- duplicated service logic in monorepo is deleted
- docs point to service packages as canonical runtime owners

Success criteria:
- no ambiguity about source of truth

---

## 9. Release Model

Once services are standalone, releases should support:

- per-service wheel builds
- per-service smoke tests
- per-service Docker images
- optional coordinated umbrella release

Recommended version policy options:

### Option A — lockstep versioning
All services share one version.

Pros:
- simple
- easy for one-product story

Cons:
- noisy bumps for unrelated services

### Option B — service-level versioning
Each service versions independently.

Pros:
- honest release cadence
- scales better long term

Cons:
- release tooling is more complex

Recommendation:
- start with **lockstep** while migration is active
- consider per-service versioning after boundaries stabilize

---

## 10. CI Model

Required CI additions for each standalone service:

### Build checks
- build wheel/sdist for service package

### Import checks
- create clean virtualenv
- install service package only (+ runtime extras)
- verify import/CLI works without repo-path hacks

### Smoke checks
- start service app or create app object
- validate `/health` or equivalent
- validate config failure behavior where required

### Integration checks
- retain monorepo end-to-end suite
- but treat it as integration validation, not proof of package independence

---

## 11. Risks

### Risk 1 — Over-extracting shared code

If too much remains in shared packages, they become hidden service owners again.

Mitigation:
- shared code must be primitive-oriented
- service-specific runtime graphs stay in service packages

### Risk 2 — Duplicate code during migration

Temporary duplication is acceptable.

Mitigation:
- prefer short-lived duplication over long-lived hidden coupling
- delete transitional bridges once service ownership is clear

### Risk 3 — Container refactor churn

The existing central `ServiceContainer` may be deeply convenient.

Mitigation:
- split container composition incrementally by service
- preserve lower-level builders where reusable
- move orchestration, not necessarily every primitive

### Risk 4 — Release complexity increases initially

Mitigation:
- automate builds and smoke tests early
- keep lockstep versioning during migration

---

## 12. Decision Checklist

Before implementation, confirm these decisions:

1. Do we explicitly choose **Standalone Role Product Model**?  
2. Is monorepo allowed to remain assembly-only after migration?  
3. Do service packages become canonical owners of app/runtime composition?  
4. Do we permit temporary duplication during boundary extraction?  
5. Do we keep lockstep versions during the migration window?  

Recommended answers:
- yes
- yes
- yes
- yes
- yes

---

## 13. Recommended Immediate Next Steps

### Next milestone
Build the first real standalone service using `task-api`.

### Concrete worklist
1. create package-owned `task_api/container.py`
2. create package-owned `task_api/lifespan.py`
3. migrate task routes into package namespace
4. replace shared/monorepo composition with package-local canonical composition
5. add standalone install/import/smoke CI
6. document `task-api` as the reference standalone service pattern

### After that
Repeat the same pattern for:
- `ops-api`
- `control-plane`
- `agent`

---

## 13.1 Progress Update — 2026-05-09

Completed first standalone-service implementation milestone for `task-api`:

- package-owned route namespace created under `scheduler_task_api.routes`
- package-owned runtime helpers created under `scheduler_task_api.runtime`
- package-owned lifespan created under `scheduler_task_api.lifespan`
- package-owned container shim created under `scheduler_task_api.container`
- package app factory is now canonical under `scheduler_task_api.app`
- shared `runtime-core.task_api` service-composition module removed
- standalone smoke test added for package import/app creation

Current status:
- `task-api` is now the **reference standalone-service pattern** in this repository
- it still uses selected monorepo business/platform implementations during transition
- next phases should apply the same pattern to `ops-api`, `control-plane`, and `agent`

## 14. Bottom Line

If the goal is truly:

> “all services should be independent”

then the most appropriate architecture is **not** to keep extending bridge-based runtime-core indirection.

The correct long-term direction is:

- shared primitives in narrow foundation packages
- each service owns its runnable implementation
- monorepo becomes integration/assembly only

That yields package semantics that are:

- honest
- testable
- releaseable
- maintainable
- operationally clearer

