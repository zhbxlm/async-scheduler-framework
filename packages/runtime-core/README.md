# async-scheduler-runtime-core

Shared runtime/bootstrap implementation layer for independently packaged async-scheduler roles.

This package is not intended as an end-user-facing role by itself.

It provides shared bootstrapping and reusable runtime implementation that can be used by:
- `async-scheduler-control-plane`
- `async-scheduler-task-api`
- `async-scheduler-ops-api`
- `async-scheduler-agent`

## What it is

`async-scheduler-runtime-core` is a **shared bootstrap/runtime layer**.

Current scope:
- settings loading
- logging/tracing setup
- http/db helpers
- lifecycle helpers
- control-plane runtime bootstrap
- app factory registry

## What it is not

It is **not** the full scheduler business runtime.
Business/service implementation still primarily lives in the monorepo implementation layer.

## Install profiles

Technical extras:
- `runtime-core[redis]`
- `runtime-core[mysql]`
- `runtime-core[serving]`
- `runtime-core[cron]`
- `runtime-core[metrics]`
- `runtime-core[tracing]`
- `runtime-core[all]`

Role-oriented extras:
- `runtime-core[task-api]`
- `runtime-core[ops-api]`
- `runtime-core[control-plane]`
- `runtime-core[agent]`

## Example

```bash
pip install 'async-scheduler-runtime-core[task-api]'
```
