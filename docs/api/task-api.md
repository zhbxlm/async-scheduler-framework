<!-- 中文文档 -->
# Task API 参考

## 概述

- **Task API**（端口 8001）：任务提交与查询
- **Ops API**（端口 8000）：运维管理端点
- **认证**：请求头 `X-API-Key: <key>` 或 `Authorization: Bearer <token>`
- **Content-Type**：`application/json`

---

## POST /api/v1/tasks/

创建新任务。

**请求体：**

```json
{
  "dag_id": "my_dag",
  "input_data": {"key": "value"},
  "priority": "normal",
  "tenant_id": "default",
  "idempotency_key": "optional-key",
  "callback_url": "https://example.com/callback"
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `dag_id` | string | 否 | 要执行的 DAG ID |
| `task_type` | string | 否 | 任务类型（与 dag_id 二选一） |
| `input_data` | object | 否 | 任务输入数据 |
| `priority` | string | 否 | `critical` / `high` / `normal`（默认）/ `low` |
| `tenant_id` | string | 否 | 租户 ID |
| `idempotency_key` | string | 否 | 幂等键，相同键不重复创建 |
| `callback_url` | string | 否 | 任务完成后的回调地址 |

**响应 201：**

```json
{
  "task_id": "task_abc123",
  "status": "queued",
  "accepted": true,
  "queue_position": 0,
  "idempotent_reused": false
}
```

---

## GET /api/v1/tasks/

查询任务列表。

**查询参数：**`status`、`capability`、`dag_id`、`tenant_id`、`limit`、`offset`

**响应 200：**

```json
{
  "tasks": [
    {"task_id": "...", "status": "running", "created_at": "..."}
  ],
  "total": 100
}
```

---

## GET /api/v1/tasks/{task_id}

获取任务详情，返回 `TaskInfo`，包含状态、dag_id、时间戳、输出数据等字段。

---

## GET /api/v1/tasks/{task_id}/result

获取任务结果。

- **202**：任务尚未完成
- **200**：任务已完成，响应包含 `output_data`

---

## DELETE /api/v1/tasks/{task_id}

取消处于 `pending` 或 `running` 状态的任务。

---

## 健康检查

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 基础存活检查 |
| GET | `/health/ready` | 就绪检查（含 Redis + MySQL 连通性） |
| GET | `/health/live` | 存活检查 |
| GET | `/api/v1/health/metrics` | Prometheus 格式指标 |
