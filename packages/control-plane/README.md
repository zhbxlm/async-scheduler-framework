# async-scheduler-control-plane

Standalone control-plane worker for [async-scheduler](https://github.com/zhbxlm/async-scheduler-framework).

Runs background loops outside task-api / ops-api, including:
- cron scheduling
- task reconciliation / repair
- compensation processing
- callback dispatch

## Runtime model

This package is currently a **thin launcher package with a more advanced shared runtime path**.

It already uses shared runtime/bootstrap logic from `async-scheduler-runtime-core`, but still relies on monorepo business/runtime implementation for scheduler-specific service composition.

### Dependency semantics

- package `dependencies` = **source-level minimum dependencies**
- runnable deployment dependencies = use role-oriented runtime profiles (for example `async-scheduler-runtime-core[control-plane]`)

## Install

### Minimal package install

```bash
pip install async-scheduler-control-plane
```

### Runnable role profile (recommended)

```bash
pip install 'async-scheduler-runtime-core[control-plane]' async-scheduler-control-plane
```

## Start

```bash
scheduler-control-plane
```

## Runtime role

This package is intended to be deployed alongside:
- `async-scheduler-task-api`
- `async-scheduler-ops-api`

It is not a public API service. It is a background worker/process role.
