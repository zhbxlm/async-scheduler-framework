# API 参考

本文档基于 `async_scheduler/api/app.py` 实际实现，描述所有 HTTP 端点、请求/响应格式和错误码。

## 通用约定

**Base URL**：`http://127.0.0.1:8000`（默认）

**Content-Type**：`application/json`

### 通用错误格式

所有 4xx/5xx 错误统一返回结构化 detail：

```json
{
  "detail": {
    "error_code": "TASK_NOT_FOUND",
    "message": "Task not found"
  }
}
```

常用错误码：

| error_code | HTTP 状态 | 含义 |
|------------|----------|------|
| `SERVICE_UNAVAILABLE` | 503 | 服务容器未初始化 |
| `TASK_NOT_FOUND` | 404 | 任务不存在 |
| `ATTEMPT_NOT_FOUND` | 404 | 执行记录不存在 |
| `SCHEDULE_NOT_FOUND` | 404 | 调度不存在 |
| `DAG_NOT_FOUND` | 404 | DAG 不存在 |
| `CAPABILITY_NOT_FOUND` | 404 | Capability 不存在 |
| `TENANT_NOT_FOUND` | 404 | 租户不存在 |
| `QUOTA_EXCEEDED` | 429 | 超出配额限制 |
| `WORKER_REGISTRY_UNAVAILABLE` | 503 | Worker Registry 未配置 |

---

## 健康与监控

### `GET /health`

简洁健康信号，供负载均衡器 / 监控系统使用。

```json
{
  "status": "healthy",       // "healthy" | "degraded" | "starting"
  "issues": [],              // 降级时列出具体子系统
  "version": "1.0.0",
  "uptime_seconds": 42.1
}
```

`issues` 可能的值：`consumer_not_running`、`scheduler_not_running`、`reconciler_not_running`、`service_container_not_initialized`

### `GET /health/detail`

完整内部指标，供 operator 和 dashboard 使用。

```json
{
  "status": "healthy",
  "ready": true,
  "version": "1.0.0",
  "uptime_seconds": 42.1,
  "queue_size": 3,
  "scheduled_count": 1,
  "consumer_running": true,
  "scheduler_running": true,
  "reconciler_running": true,
  "worker_count": 2,
  "repair_history_count": 0
}
```

### `GET /readiness`

Kubernetes readiness probe，服务未初始化时返回 503。

### `GET /liveness`

Kubernetes liveness probe，始终返回 200。

---

## 任务（Tasks）

### `POST /tasks` — 创建任务

```json
// 请求体（TaskCreate）
{
  "name": "my-task",              // 必填
  "payload": {"capability": "echo", "message": "hello"},
  "tenant_id": "tenant-a",        // 可选
  "idempotency_key": "key-001",   // 可选，幂等去重
  "priority": 0,                  // 可选，数值越小优先级越高
  "scheduled_at": null            // 可选，ISO8601 延时入队
}

// 响应 201
{
  "id": "task-uuid",
  "name": "my-task",
  "status": "queued",
  "payload": {...},
  "created_at": "2026-05-03T06:00:00Z",
  ...
}
```

### `POST /tasks/batch` — 批量创建任务

单次最多 100 个，部分失败不回滚。

```json
// 请求体
{ "tasks": [ {...}, {...} ] }

// 响应 201
{
  "created": [...],
  "failed": [{"index": 1, "error": "...", "error_code": "CREATE_FAILED"}],
  "total": 2,
  "succeeded": 1
}
```

### `GET /tasks` — 任务列表

查询参数：`status`（过滤状态）、`limit`（1-1000，默认 100）、`offset`（默认 0）

### `GET /tasks/{task_id}` — 任务详情

### `GET /tasks/{task_id}/history` — 任务执行历史

返回任务元数据 + 所有执行记录 + 汇总统计（succeed / failed 计数）。

### `GET /tasks/{task_id}/attempts` — 执行记录列表

### `GET /tasks/{task_id}/attempts/latest` — 最新执行记录

### `POST /tasks/{task_id}/cancel` — 取消任务

取消队列中的任务并将状态更新为 `CANCELLED`。

### `DELETE /tasks/{task_id}` — 删除任务

---

## 调度（Schedules）

### `POST /schedules` — 创建调度

```json
{
  "name": "heartbeat",
  "cron_expression": "*/5 * * * *",
  "dedup_window_seconds": 60,
  "task_template": {"capability": "echo", "message": "tick"}
}
```

### `GET /schedules` — 调度列表

### `GET /schedules/{schedule_id}` — 调度详情

### `POST /schedules/{schedule_id}/trigger` — 立即触发

### `POST /schedules/{schedule_id}/pause` — 暂停调度

### `POST /schedules/{schedule_id}/resume` — 恢复调度

---

## DAG

### `POST /dags` — 创建 DAG

```json
{
  "name": "my-pipeline",
  "nodes": [
    {"id": "step1", "task_type": "preprocess", "depends_on": []},
    {"id": "step2", "task_type": "compute", "depends_on": ["step1"]}
  ],
  "max_parallelism": 4
}
```

### `GET /dags` — DAG 列表

### `GET /dags/{dag_id}` — DAG 详情

### `POST /dags/execute` — 执行 DAG

```json
// 请求体
{ "dag_id": "dag-uuid" }

// 响应（异步启动）
{ "dag": {...}, "status": "executing" }
```

### `POST /dags/{dag_id}/cancel` — 取消 DAG

### `GET /dags/{dag_id}/stream` — SSE 进度流

实时推送 DAG 执行节点事件，`text/event-stream` 格式。

事件类型：`node_start` | `node_done` | `node_error` | `dag_done` | `dag_failed` | `dag_cancelled` | `heartbeat` | `timeout`

```
data: {"event": "node_start", "node": "step1", "task_type": "preprocess"}
data: {"event": "node_done", "node": "step1", "result": {...}}
data: {"event": "dag_done", "dag_id": "...", "status": "success", "node_statuses": {...}}
```

### `POST /dags/stream` — 创建并流式执行 DAG

一次请求完成 DAG 创建 + 执行 + SSE 进度推送。

---

## 队列与 Capability

### `GET /queue/stats` — 队列统计

```json
{
  "queue_sizes": {...},
  "total_queued": 3,
  "scheduled_count": 1,
  "running_tasks": 1,
  "worker_count": 2,
  "reconciler_running": true
}
```

### `GET /queues/capabilities` — Capability 队列列表

### `GET /queues/{capability}/stats` — 单 Capability 队列统计

### `GET /capabilities` — Capability 列表（含使用指标）

### `GET /capabilities/{capability_name}` — Capability 详情

---

## Worker & Lease 诊断

### `GET /workers` — Worker 列表

查询参数：`include_stale=false`（默认只返回存活 worker）

### `GET /workers/{worker_id}` — Worker 详情

### `GET /workers/{worker_id}/leases` — Worker 持有的 Lease

### `GET /debug/summary` — 系统快照

返回 health / queue / workers / leases / reconciler 的完整快照，含 anomaly_summary。

**推荐排障入口**，先看这里再深入。

### `GET /debug/leases` — Lease 列表

过滤参数：`worker_id`、`task_status`、`attempt_status`、`locked_only`

### `GET /debug/leases/anomalies` — 异常 Lease 列表

返回所有检测到异常的 lease，每条含 `anomaly_types` 标注原因：

| anomaly_type | 含义 |
|--------------|------|
| `running_without_lock` | 任务状态 RUNNING 但无 lease |
| `locked_but_terminal` | 有 lease 但任务已终态 |
| `abandoned_but_running` | attempt ABANDONED 但任务还是 RUNNING |
| `stale_lease` | lease TTL < 5s，即将过期 |

### `GET /debug/leases/anomalies/summary` — 异常汇总统计

### `GET /debug/leases/{task_id}` — 单任务 Lease 详情

---

## Reconciler

### `POST /reconciler/run` — 手动触发一次修复

### `GET /reconciler/stats` — Reconciler 统计指标

### `GET /reconciler/history` — 修复历史

过滤参数：`action`、`task_id`

---

## 多租户（Tenants）

### `POST /tenants` — 创建租户

```json
{
  "name": "team-a",
  "config": {"max_queued": 20, "max_running": 5}
}
```

### `GET /tenants` — 租户列表

### `GET /tenants/{tenant_id}` — 租户详情

### `PATCH /tenants/{tenant_id}` — 更新租户配额

### `GET /quota/stats` — 配额统计

---

## 平台扩展组件

以下端点在对应组件未配置时返回 404，属于可选功能：

| 端点 | 说明 |
|------|------|
| `GET /actors/capabilities` | Actor Pool 能力列表 |
| `GET /actors/{capability}/stats` | Actor Pool 统计 |
| `GET /resources/stats` | Resource Manager 伸缩统计 |
| `GET /resources/scale-history` | 伸缩历史事件 |
| `GET /async-proxy/stats` | Async Proxy Sidecar 统计 |
| `GET /clusters` | 集群注册表列表 |
| `GET /clusters/{cluster_id}` | 集群详情 |
| `GET /clusters/by-capability/{capability}` | 按 capability 查集群 |

---

## cURL 快速示例

```bash
# 健康检查
curl http://127.0.0.1:8000/health

# 创建任务
curl -X POST http://127.0.0.1:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo","payload":{"capability":"echo","message":"hi"}}'

# 查看任务列表
curl 'http://127.0.0.1:8000/tasks?limit=10'

# 创建定时调度
curl -X POST http://127.0.0.1:8000/schedules \
  -H 'Content-Type: application/json' \
  -d '{"name":"tick","cron_expression":"*/1 * * * *","task_template":{"capability":"echo","message":"tick"}}'

# 查看系统快照（排障入口）
curl http://127.0.0.1:8000/debug/summary

# 查看异常 lease
curl http://127.0.0.1:8000/debug/leases/anomalies

# 手动触发 reconciler
curl -X POST http://127.0.0.1:8000/reconciler/run
```
