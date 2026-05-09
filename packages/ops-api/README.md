# async-scheduler-ops-api

Ops/Admin API service for [async-scheduler](https://github.com/zhbxlm/async-scheduler-framework).

Deploy separately from the Task API — internal use only (not exposed to end users).

## Runtime model

This package is currently a **thin launcher package**.

It provides:
- the ops-api role identity
- CLI/server entrypoint
- package-local startup/wiring

It does **not** yet contain the full scheduler business implementation by itself.
In the current architecture it runs together with:
- shared runtime/bootstrap support from `async-scheduler-runtime-core`
- monorepo business/runtime implementation provided by the framework install

### Dependency semantics

- package `dependencies` = **source-level minimum dependencies**
- runnable deployment dependencies = use role-oriented runtime profiles (for example `async-scheduler-runtime-core[ops-api]`)

## Install

### Minimal package install

```bash
pip install async-scheduler-ops-api
```

### Runnable role profile (recommended)

```bash
pip install 'async-scheduler-runtime-core[ops-api]' async-scheduler-ops-api
```

## Start

```bash
scheduler-ops-api --host 0.0.0.0 --port 8000
```

## Endpoints

| Prefix | Description |
|--------|-------------|
| `/ops/v1/capabilities` | Capability registration and management |
| `/ops/v1/clusters` | Cluster management |
| `/ops/v1/nodes` | Node management |
| `/ops/v1/dags` | DAG definition management |
| `/ops/v1/schedules` | Cron schedule management |
| `/ops/v1/tenants` | Tenant and API key management |
| `/alerts` | Alertmanager webhook receiver |
| `/health` | Liveness + readiness |
| `/metrics` | Prometheus metrics |
