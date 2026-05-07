<!-- 中文文档 -->
# 多租户

每个租户拥有独立的资源配额与 capability 白名单，互相隔离。

## 创建租户

```bash
scheduler tenant create --name team-a --quota '{"max_concurrent_tasks": 100}'
```

## 租户 API Key

每个租户会被分配一个 API Key，提交任务时通过 `X-API-Key` 请求头传入：

```bash
curl -X POST http://localhost:8001/api/v1/tasks/ \
  -H "X-API-Key: <tenant-api-key>" \
  -H "Content-Type: application/json" \
  -d '{"task_type": "image_generate", "input_data": {...}}'
```

## 租户配额配置

```yaml
tenants:
  team-a:
    max_concurrent_tasks: 100   # 最大并发任务数
    priority_boost: 2           # 优先级加成
    allowed_capabilities:
      - image_generate
      - data_process

  team-b:
    max_concurrent_tasks: 50
    allowed_capabilities:
      - report_generate
```

## 查看租户用量

```bash
# 查看指定租户的使用统计
scheduler tenant usage <tenant-id>
```

通过 Ops API 也可以查询：

```bash
curl http://localhost:8000/ops/v1/tenants/<id>/usage \
  -H "Authorization: Bearer <admin-key>"
```

## 超级管理员

租户的创建、删除、API Key 颁发需要超级管理员权限（`TENANT_SUPER_ADMIN_API_KEY`）。

普通租户只能查看和更新自身配置。

## 关闭多租户模式

对于单租户部署，可以关闭多租户模式（默认关闭）：

```bash
TENANT_MULTI_TENANT_ENABLED=false
```

此时所有请求共享同一默认租户，无需传 API Key。
