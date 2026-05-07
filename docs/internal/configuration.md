# Configuration

本文档描述当前项目的实际配置结构，而不是历史 `ray_async.*` 体系。

## 配置入口

当前主入口：
- `config/settings_pydantic.py`：基于 pydantic-settings 的配置定义
- `config/settings_compat.py`：兼容层，向旧代码暴露统一 `settings` 对象

运行时大多数代码通过：

```python
from config.settings_compat import settings
```

读取配置。

## 主要配置分组

### Redis
- `settings.redis.url`
- `settings.redis.host`
- `settings.redis.port`
- `settings.redis.password`
- `settings.redis.db`

### MySQL
- `settings.mysql.url`
- `settings.mysql.host`
- `settings.mysql.port`
- `settings.mysql.username`
- `settings.mysql.password`
- `settings.mysql.database`

### Server
- `settings.server.host`
- `settings.server.port`
- `settings.server.workers`
- `settings.server.reload`
- `settings.server.access_log`

### Task
- `settings.task.default_priority`
- `settings.task.max_retries`
- `settings.task.timeout_seconds`
- `settings.task.result_ttl_seconds`
- `settings.task.max_concurrent`

### DAG
- `settings.dag.config_dir`
- `settings.dag.auto_reload`
- `settings.dag.reload_interval`

### Background
- `settings.background.reconcile.enabled`
- `settings.background.reconcile.interval_seconds`
- `settings.background.reconcile.stuck_max_per_tick`
- `settings.background.reconcile.stuck_task_max_age_seconds`
- `settings.background.reconcile.batch_size`
- `settings.background.cron.enabled`
- `settings.background.cron.poll_interval`

### Agent
- `settings.agent.node_id`
- `settings.agent.host`
- `settings.agent.port`
- `settings.agent.heartbeat_interval`
- `settings.agent.owner_ttl_seconds`

## 核心环境变量

### 基础设施
- `REDIS_URL`
- `MYSQL_URL`

### 运行环境
- `ENVIRONMENT`
- `DEBUG`

### 后台任务开关
- `BACKGROUND__RECONCILE__ENABLED`
- `BACKGROUND__CRON__ENABLED`

### 可观测性
- `OTEL_EXPORTER_OTLP_ENDPOINT`
- `OTEL_SERVICE_NAME`

## 推荐配置方式

### ops-api

```bash
BACKGROUND__RECONCILE__ENABLED=false
BACKGROUND__CRON__ENABLED=true
```

说明：
- ops-api 负责 cron 调度
- ops-api 主要依赖 Redis

### task-api

```bash
BACKGROUND__RECONCILE__ENABLED=true
BACKGROUND__CRON__ENABLED=false
```

说明：
- task-api 负责任务一致性修复
- task-api 依赖 Redis + MySQL

## 本地开发示例

```bash
export REDIS_URL=redis://localhost:6379/0
export MYSQL_URL=mysql+aiomysql://user:pass@localhost:3306/scheduler

uvicorn src.main:app --reload --port 8000
uvicorn src.main_tasks:app --reload --port 8001
```

## Docker Compose 场景

compose 已默认拆分为两类 API：
- `ops-api`
- `task-api`

参见：`docker-compose.yml`

## 说明

旧文档里出现的这些历史命名已经不再适用：
- `ray_async.settings`
- `ray_async.config`
- `DATABASE_URL` / `QUEUE_TYPE` / `LOCK_TYPE` / `REGISTRY_TYPE` 这套旧配置中心叙述

当前仓库以 `config/settings_pydantic.py` + `config/settings_compat.py` 为准。
