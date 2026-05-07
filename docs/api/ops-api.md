<!-- 中文文档 -->
# Ops API 参考

## 概述

Ops API（端口 8000）提供运维管理端点，仅依赖 Redis，不依赖 MySQL。

---

## 健康与概览

| 方法 | 路径 | 认证 | 说明 |
|------|------|------|------|
| GET | `/ops/v1/health` | 无 | 公开健康检查 |
| GET | `/ops/v1/overview` | 需要 | 系统概览：各 capability 统计、熔断状态、利用率 |
| GET | `/ops/v1/stats` | 需要 | 全部 capability 的队列统计 |

---

## Capabilities — `/ops/v1/capabilities`

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 列出所有 capability |
| POST | `/` | 注册新 capability（201） |
| GET | `/{id}` | 获取详情 |
| PUT | `/{id}` | 更新 |
| DELETE | `/{id}` | 注销（204） |
| GET | `/{id}/health` | capability 健康状态 |

---

## Clusters — `/ops/v1/clusters`

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 列出所有集群 |
| POST | `/` | 注册集群（201） |
| GET | `/{id}` | 获取详情 |
| PUT | `/{id}` | 更新 |
| DELETE | `/{id}` | 注销（204） |
| GET | `/{id}/resources` | 集群资源统计 |

---

## Nodes — `/ops/v1/nodes`

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 列出所有节点 |
| GET | `/{id}` | 获取节点详情 |
| POST | `/{id}/invite` | 加入集群 |
| POST | `/{id}/drain` | 开始排空（Drain） |
| POST | `/{id}/release` | 释放节点 |
| DELETE | `/{id}` | 注销节点（204） |

---

## DAGs — `/ops/v1/dags`

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 列出已注册 DAG |
| POST | `/` | 注册 DAG（201） |
| GET | `/{dag_id}` | 获取 DAG 定义 |
| PUT | `/{dag_id}` | 更新 DAG |
| DELETE | `/{dag_id}` | 删除 DAG（204） |

---

## Schedules — `/ops/v1/schedules`

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 列出所有调度计划 |
| POST | `/` | 创建调度计划（201） |
| GET | `/{id}` | 获取详情 |
| PUT | `/{id}` | 更新 |
| POST | `/{id}/toggle` | 启用 / 禁用 |
| DELETE | `/{id}` | 删除（204） |

---

## Tenants — `/ops/v1/tenants`

| 方法 | 路径 | 权限 | 说明 |
|------|------|------|------|
| GET | `/` | 超级管理员 | 列出所有租户 |
| POST | `/` | 超级管理员 | 注册租户（201） |
| GET | `/{id}` | 租户自身 | 获取租户详情 |
| PUT | `/{id}` | 租户自身 | 更新租户信息 |
| DELETE | `/{id}` | 超级管理员 | 注销租户（204） |
| POST | `/{id}/api-keys` | 超级管理员 | 生成 API Key |
| GET | `/{id}/usage` | 租户自身 | 查询使用统计 |

---

## 队列运维

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/ops/v1/queue/{cap}/snapshot` | 队列快照 |
| POST | `/ops/v1/queue/{cap}/cleanup-stale` | 清理卡死任务 |
| GET | `/ops/v1/tasks/{id}/debug` | 任务调试端点 |

---

## 错误码

| 状态码 | 含义 |
|--------|------|
| 200 | 成功 |
| 201 | 创建成功 |
| 202 | 已接受（结果尚未就绪） |
| 204 | 无内容（删除成功） |
| 400 | 请求参数错误 |
| 401 | API Key 无效 |
| 403 | 权限不足 |
| 404 | 资源不存在 |
| 409 | 冲突（如重复注册） |
| 422 | 数据校验失败 |
| 503 | 服务不可用 |
