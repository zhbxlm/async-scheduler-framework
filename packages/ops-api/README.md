# async-scheduler-ops-api

Ops/Admin API service for [async-scheduler](https://github.com/zhbxlm/async-scheduler-framework).

Deploy separately from the Task API — internal use only (not exposed to end users).

## Install

```bash
pip install async-scheduler-ops-api
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
