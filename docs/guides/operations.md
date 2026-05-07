<!-- 中文文档 -->
# 运维手册

## 系统架构

```
客户端 → task-api:8001（任务 CRUD）
       → ops-api:8000（运维管理）
              |
        Redis（队列 + 注册表 + 锁）
              |
        MySQL（TaskRecord 持久化）
              |
        Node Agent + Worker
```

## 环境变量

| 变量 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `REDIS_URL` | 是 | `redis://localhost:6379` | Redis 连接地址 |
| `MYSQL_URL` | task-api 必填 | — | MySQL 连接地址 |
| `ENVIRONMENT` | 否 | `development` | dev / staging / production |
| `LOG_LEVEL` | 否 | `INFO` | 日志级别 |
| `LOG_JSON` | 否 | `true` | 是否输出 JSON 结构化日志 |
| `BACKGROUND__RECONCILE__ENABLED` | 否 | `true` | 启用任务 reconciler |
| `BACKGROUND__CRON__ENABLED` | 否 | `false` | 启用 cron 调度（task-api） |
| `TENANT_SUPER_ADMIN_API_KEY` | 否 | — | 超级管理员 API Key |
| `TENANT_MULTI_TENANT_ENABLED` | 否 | `false` | 启用多租户模式 |

## 部署

### Docker Compose

```yaml
services:
  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
  mysql:
    image: mysql:8
    environment:
      MYSQL_ROOT_PASSWORD: testpass
      MYSQL_DATABASE: scheduler_test
    ports: ["3306:3306"]
  ops-api:
    build: .
    command: python -m uvicorn src.main:app --host 0.0.0.0 --port 8000
    ports: ["8000:8000"]
    environment:
      REDIS_URL: redis://redis:6379
      BACKGROUND__CRON__ENABLED: "true"
  task-api:
    build: .
    command: python -m uvicorn src.main_tasks:app --host 0.0.0.0 --port 8001
    ports: ["8001:8001"]
    environment:
      REDIS_URL: redis://redis:6379
      MYSQL_URL: mysql://root:testpass@mysql/scheduler_test
      BACKGROUND__RECONCILE__ENABLED: "true"
```

### 手动部署（systemd）

```ini
[Unit]
Description=Async Scheduler Ops API
After=network.target

[Service]
User=app
WorkingDirectory=/opt/async-scheduler
ExecStart=/opt/async-scheduler/.venv/bin/uvicorn src.main:app --host 0.0.0.0 --port 8000
Restart=always

[Install]
WantedBy=multi-user.target
```

## 健康检查

```bash
curl http://localhost:8000/health/ready
curl http://localhost:8001/health/ready
curl -H "Authorization: Bearer KEY" http://localhost:8000/ops/v1/overview
```

## 常见问题排查

### Redis 不可用
**现象**：所有端点返回 503

**处理**：
```bash
redis-cli ping
# 检查连接配置
```

### MySQL 不可用
**现象**：task-api 返回 503

**处理**：
```bash
mysqladmin ping
# 检查磁盘空间和网络连通性
```

### 熔断器打开
**现象**：对应 capability 的任务提交被拒绝

**处理**：确认下游服务健康后，手动重置：
```bash
redis-cli DEL queue:cb:CAPABILITY:cb:state
redis-cli DEL queue:cb:CAPABILITY:cb:failures
```

### 队列积压
**现象**：`POST /tasks` 返回 `accepted=false`

**处理**：增大 `max_queue_depth` 或添加更多 Worker 节点

### 任务卡死（stuck in running）
**现象**：任务长时间停留在 `running` 状态

**处理**：触发 reconciler 清理：
```bash
curl -X POST http://localhost:8000/ops/v1/queue/CAPABILITY/cleanup-stale
```

## 备份

```bash
# Redis 快照
redis-cli BGSAVE

# MySQL 全量备份
mysqldump -u root -p scheduler_test > backup.sql
```

## 扩缩容

- **ops-api**：无状态，可水平扩容
- **task-api**：无状态（状态存于 Redis + MySQL），可水平扩容
- **Redis**：单实例负责全局协调
- **MySQL**：主库负责写入，只读副本负责查询

多实例部署使用 Redis Lua 脚本保证原子性全局并发控制，`CronScheduler` 和 `TaskReconciler` 均依赖 Redis 协调，避免重复执行。

## 日志

结构化 JSON 格式，输出到 stdout：

```json
{
  "timestamp": "...",
  "level": "INFO",
  "logger": "src.platform",
  "message": "...",
  "module": "..."
}
```

使用 `docker logs` 或 `journalctl` 查看日志。
