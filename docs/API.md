# Ray Async API Documentation

> Auto-generated on 2026-05-05 00:30 — do not edit manually.

**Version:** `1.0.0`  

## Contents

- [capabilities](#capabilities)
- [clusters](#clusters)
- [dags](#dags)
- [health](#health)
- [misc](#misc)
- [nodes](#nodes)
- [ops](#ops)
- [schedules](#schedules)
- [tasks](#tasks)
- [tenants](#tenants)

---

## capabilities

### `GET /ops/v1/capabilities/`

**List all capabilities**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `tenant_id` | query | string | — |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `POST /ops/v1/capabilities/`

**Register capability**

**Request Body:** `application/json` (application/json)

**Responses:**

| Status | Description |
|--------|-------------|
| `201` | Successful Response |
| `422` | Validation Error |

---

### `GET /ops/v1/capabilities/{capability_id}`

**Get capability detail**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `capability_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `PUT /ops/v1/capabilities/{capability_id}`

**Update capability**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `capability_id` | path | string | ✅ |  |

**Request Body:** `application/json` (application/json)

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `DELETE /ops/v1/capabilities/{capability_id}`

**Unregister capability**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `capability_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `204` | Successful Response |
| `422` | Validation Error |

---

### `GET /ops/v1/capabilities/{capability_id}/health`

**Get capability health**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `capability_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

## clusters

### `GET /ops/v1/clusters/`

**List all clusters**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `tenant_id` | query | string | — |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `POST /ops/v1/clusters/`

**Register cluster**

**Request Body:** `application/json` (application/json)

**Responses:**

| Status | Description |
|--------|-------------|
| `201` | Successful Response |
| `422` | Validation Error |

---

### `GET /ops/v1/clusters/{cluster_id}`

**Get cluster detail**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `cluster_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `PUT /ops/v1/clusters/{cluster_id}`

**Update cluster**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `cluster_id` | path | string | ✅ |  |

**Request Body:** `application/json` (application/json)

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `DELETE /ops/v1/clusters/{cluster_id}`

**Unregister cluster**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `cluster_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `204` | Successful Response |
| `422` | Validation Error |

---

### `GET /ops/v1/clusters/{cluster_id}/resources`

**Get cluster observed resources**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `cluster_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

## dags

### `GET /ops/v1/dags/`

**List registered DAGs**

List DAG IDs via Redis scan (dag_def:{tenant_id}:* keys).

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `tenant_id` | query | string | — |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `POST /ops/v1/dags/`

**Register DAG definition**

**Request Body:** `application/json` (application/json)

**Responses:**

| Status | Description |
|--------|-------------|
| `201` | Successful Response |
| `422` | Validation Error |

---

### `GET /ops/v1/dags/{dag_id}`

**Get DAG definition**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `dag_id` | path | string | ✅ |  |
| `tenant_id` | query | string | — |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `PUT /ops/v1/dags/{dag_id}`

**Update DAG definition**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `dag_id` | path | string | ✅ |  |

**Request Body:** `application/json` (application/json)

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `DELETE /ops/v1/dags/{dag_id}`

**Delete DAG definition**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `dag_id` | path | string | ✅ |  |
| `tenant_id` | query | string | — |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `204` | Successful Response |
| `422` | Validation Error |

---

## health

### `GET /health/`

**Health Overview**

Basic health check.

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |

---

### `GET /health/detailed`

**Health Detailed**

Detailed health check with system metrics.

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |

---

### `GET /health/metrics`

**Metrics Prometheus**

Prometheus metrics endpoint (text format).

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |

---

### `GET /health/mysql`

**Health Mysql**

MySQL health check.

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |

---

### `GET /health/ready`

**Readiness Probe**

Readiness probe for Kubernetes/load balancers.

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |

---

### `GET /health/redis`

**Health Redis**

Redis health check.

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |

---

## misc

### `GET /async-proxy/stats`

**Async Proxy Stats**

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |

---

### `GET /callbacks/stats`

**Callbacks Stats**

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |

---

### `GET /health`

**Health**

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |

---

### `GET /quota/stats`

**Quota Stats**

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |

---

### `GET /resources/stats`

**Resources Stats**

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |

---

## nodes

### `GET /ops/v1/nodes/`

**List all nodes**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `cluster_id` | query | string | — |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `GET /ops/v1/nodes/{node_id}`

**Get node detail**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `node_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `DELETE /ops/v1/nodes/{node_id}`

**Unregister node**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `node_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `204` | Successful Response |
| `422` | Validation Error |

---

### `POST /ops/v1/nodes/{node_id}/drain`

**Start draining node**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `node_id` | path | string | ✅ |  |

**Request Body:** `application/json` (application/json)

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `POST /ops/v1/nodes/{node_id}/invite`

**Invite node to join cluster**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `node_id` | path | string | ✅ |  |

**Request Body:** `application/json` (application/json)

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `POST /ops/v1/nodes/{node_id}/release`

**Release node from cluster**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `node_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

## ops

### `POST /ops/v1/callbacks/process`

**Process due callback retries**

Drain due entries from the callback retry ZSET.

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `batch_size` | query | integer | — |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `GET /ops/v1/health`

**Service health check**

Return health of Redis, DB and registered components.

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |

---

### `POST /ops/v1/queue/{capability}/cleanup-stale`

**Cleanup stale running entries**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `capability` | path | string | ✅ |  |
| `max_age_seconds` | query | number | — |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `GET /ops/v1/queue/{capability}/snapshot`

**Queue snapshot for one capability**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `capability` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `POST /ops/v1/reconcile`

**Manually trigger task reconciliation**

Trigger one reconcile cycle (all three phases).

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |

---

### `GET /ops/v1/stats`

**Queue stats across all capabilities**

Return queue depths for all registered capabilities.

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |

---

## schedules

### `GET /ops/v1/schedules/`

**List all cron schedules**

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |

---

### `POST /ops/v1/schedules/`

**Create cron schedule**

**Request Body:** `application/json` (application/json)

**Responses:**

| Status | Description |
|--------|-------------|
| `201` | Successful Response |
| `422` | Validation Error |

---

### `GET /ops/v1/schedules/{schedule_id}`

**Get schedule detail**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `schedule_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `PUT /ops/v1/schedules/{schedule_id}`

**Update schedule**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `schedule_id` | path | string | ✅ |  |

**Request Body:** `application/json` (application/json)

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `DELETE /ops/v1/schedules/{schedule_id}`

**Delete schedule**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `schedule_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `204` | Successful Response |
| `422` | Validation Error |

---

### `POST /ops/v1/schedules/{schedule_id}/toggle`

**Enable or disable schedule**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `schedule_id` | path | string | ✅ |  |

**Request Body:** `application/json` (application/json)

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

## tasks

### `GET /tasks/`

**List tasks**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `status` | query | any | — |  |
| `task_type` | query | any | — |  |
| `limit` | query | integer | — |  |
| `cursor` | query | any | — | Opaque pagination cursor (task_id) |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `POST /tasks/`

**Submit task**

**Request Body:** `TaskCreate` (application/json)

**Responses:**

| Status | Description |
|--------|-------------|
| `201` | Successful Response |
| `422` | Validation Error |

---

### `GET /tasks/{task_id}`

**Get task detail**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `task_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `DELETE /tasks/{task_id}`

**Cancel task**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `task_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `GET /tasks/{task_id}/result`

**Get task result**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `task_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

## tenants

### `GET /ops/v1/tenants/`

**List tenants (super-admin only)**

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |

---

### `POST /ops/v1/tenants/`

**Register tenant (super-admin only)**

**Request Body:** `application/json` (application/json)

**Responses:**

| Status | Description |
|--------|-------------|
| `201` | Successful Response |
| `422` | Validation Error |

---

### `GET /ops/v1/tenants/{tenant_id}`

**Get tenant detail**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `tenant_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `PUT /ops/v1/tenants/{tenant_id}`

**Update tenant**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `tenant_id` | path | string | ✅ |  |

**Request Body:** `application/json` (application/json)

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---

### `DELETE /ops/v1/tenants/{tenant_id}`

**Unregister tenant (super-admin only)**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `tenant_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `204` | Successful Response |
| `422` | Validation Error |

---

### `POST /ops/v1/tenants/{tenant_id}/api-keys`

**Generate API key for tenant**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `tenant_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `201` | Successful Response |
| `422` | Validation Error |

---

### `GET /ops/v1/tenants/{tenant_id}/usage`

**Get tenant resource usage**

**Parameters:**

| Name | In | Type | Required | Description |
|------|----|------|----------|-------------|
| `tenant_id` | path | string | ✅ |  |

**Responses:**

| Status | Description |
|--------|-------------|
| `200` | Successful Response |
| `422` | Validation Error |

---
