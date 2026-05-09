# Package Runtime Model

This document defines the packaging/runtime terminology used by async-scheduler after the v1.2.0 package split.

## 1. Thin launcher package

A **thin launcher package** is a deployable role package that provides:
- CLI entrypoints
- package identity
- runtime role naming
- minimal package-local bootstrap/wiring

But it does **not** necessarily contain the full business implementation required to run independently.

Examples (current state):
- `async-scheduler-task-api`
- `async-scheduler-ops-api`
- `async-scheduler-agent`
- `async-scheduler-control-plane` (partially more advanced than the others)

Thin launcher packages may still rely on:
- shared runtime support from `async-scheduler-runtime-core`
- monorepo business/runtime implementations

---

## 2. Shared bootstrap/runtime layer

A **shared bootstrap/runtime layer** contains runtime support that can be reused across multiple roles.

Current package:
- `async-scheduler-runtime-core`

Current responsibilities:
- settings loading
- logging/tracing setup
- http/db helpers
- lifecycle helpers
- control-plane runtime bootstrap
- app factory registry

This layer is reusable but is not the full business runtime of the platform.

---

## 3. Monorepo business implementation layer

The **monorepo business implementation layer** is the internal implementation that currently remains under:
- `src.platform.*`
- `src.services.*`
- `src.main*`
- `src.agent.*`

This layer still contains the majority of scheduler business logic and service composition.

---

## 4. Source-level minimum dependency

**Source-level minimum dependency** means:
> only the third-party dependencies directly imported by the package source code itself.

This is the dependency standard now used by package `pyproject.toml` required dependencies.

Its purpose is:
- packaging hygiene
- avoiding redundant dependency duplication
- clarifying direct package-level coupling

---

## 5. Product runnable dependency

**Product runnable dependency** means:
> everything required to run a role as a real deployable product in a production-like environment.

This may include:
- database drivers
- Redis client
- serving stack
- metrics/tracing stack
- role-specific runtime profiles

This is broader than source-level minimum dependency.

---

## 6. Role-oriented runtime profile

A **role-oriented runtime profile** is an install profile aligned to how users deploy the product, for example:
- `runtime-core[task-api]`
- `runtime-core[ops-api]`
- `runtime-core[control-plane]`
- `runtime-core[agent]`

These profiles are intended to bridge the gap between:
- clean source-level dependency declaration
- convenient product-level installation

---

## 7. Current truth after v1.2.0

At v1.2.0:
- package dependency hygiene is good
- role packages are deployable and useful
- not all role packages are fully standalone products yet
- current role packages should be understood primarily as thin launcher packages over a shared runtime/bootstrap layer plus monorepo business implementation


## Product Model Decision (P3.1)

Current explicit choice by role:

- `task-api`: **thin launcher package over shared runtime + monorepo business implementation**
- `ops-api`: **thin launcher package over shared runtime + monorepo business implementation**
- `agent`: **thin launcher package over shared runtime bootstrap + monorepo agent implementation**
- `control-plane`: **shared-runtime-first role package**, but still not a fully standalone product package

This means the project currently adopts a **thin role package model** for all published role packages.

Implications:
- package installation is supported and documented
- shared runtime bootstrap is published through `runtime-core`
- business/platform implementation still lives in the framework/monorepo layer
- future work may move selected roles toward standalone-product status, but that is **not** the current contract

## Version Management (P3.3)

Versioning is centrally managed through:

- root `VERSION`
- `scripts/release_version.py`

Recommended release flow:

```bash
python scripts/release_version.py show
python scripts/release_version.py set 1.2.1
# optional
python scripts/release_version.py set 1.2.1 --tag
```

The script updates:
- root `VERSION`
- all package `version = ...` fields
- internal `async-scheduler-runtime-core>=...` lower bounds

This replaces ad hoc manual version sweeping for future releases.
