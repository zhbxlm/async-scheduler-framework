# 架构概述

> 更新：2026-05-05，基于 API 拆分和包重构

## 系统上下文

```ascii
┌─────────────────────────────────────────────────────────┐
│                    外部用户 / CLI                        │
└─────────────────────────────────────────────────────────┘
           ↓                              ↓
┌─────────────────────┐      ┌─────────────────────────┐
│      Task API       │      │        Ops API          │
│   (端口 8001)       │◄────►│      (端口 8000)        │
│ - 任务创建/状态     │      │ - 运维监控/管理         │
│ - DAG 执行          │      │ - 队列/集群管理         │
│ - Cron 调度         │      └─────────────────────────┘
└─────────────────────┘                 ↓
           ↓                   ┌─────────────────┐
┌─────────────────┐            │    Redis (缓存)  │
│ MySQL (任务状态) │            └─────────────────┘
└─────────────────┘                 ↓       ↑
           ↑                   ┌─────────────────┐
┌─────────────────┐            │   Node Agent    │
│    Worker SDK   │◄──────────►│  (async-agent)  │
│  (async-worker) │            └─────────────────┘
└─────────────────┘
```

## 组件图

```ascii
┌─────────────────────────────────────────────────────────────────┐
│                          ServiceContainer                       │
├─────────────────────────────────────────────────────────────────┤
│ build_task_api()           │ build_ops_api()                   │
├────────────────────────────┼────────────────────────────────────┤
│ • TaskCreator              │ • CapabilityRegistry              │
│ • CronScheduler            │ • ClusterRegistry                 │
│ • TaskReconciler           │ • NodeRegistry                    │
│ • QueueManager             │ • QueueManager                    │
│ • DAG 引擎                  │ • 运维路由 (/ops/v1)               │
│ • 任务路由 (/api/v1)         │ • 无 MySQL 依赖                   │
└────────────────────────────┴────────────────────────────────────┘
```

## 数据流：任务生命周期

```ascii
1. 任务创建
   CLI / HTTP → Task API → TaskCreator → 幂等检查 → QueueManager.enqueue()

2. 队列处理
   QueueManager (Redis Sorted Sets)
   ├── pending: ZADD (score = execute_after_ms)
   ├── running: ZADD (score = start_time_ms)
   └── stats: HINCRBY (计数器)

3. Worker 执行
   Node Agent 轮询 → TaskConsumer.dequeue() → ProxyWorker → 外部服务

4. 结果回调
   TaskCompletionNode → MySQL 持久化 → Redis 回调队列 → HTTP 回调
```

## API 边界

### Task API (`src/main_tasks.py`, 端口 8001)
- **职责**: 任务生命周期管理
- **路由前缀**: `/api/v1`
- **依赖**: MySQL + Redis
- **包含**: TaskCreator, CronScheduler, TaskReconciler, DAG 引擎

**路由**:
- `POST /api/v1/tasks` - 创建任务
- `GET /api/v1/tasks/{id}` - 获取任务状态
- `GET /api/v1/dags` - DAG 管理
- `GET /api/v1/health` - 健康检查 (含 DB 连接性)

### Ops API (`src/main.py`, 端口 8000)
- **职责**: 运维监控和系统管理
- **路由前缀**: `/ops/v1`
- **依赖**: 仅 Redis
- **包含**: 所有 Registry 类

**路由**:
- `GET /ops/v1/overview` - 系统概览 (含 capability 统计)
- `GET /ops/v1/capabilities` - 能力管理
- `GET /ops/v1/nodes` - 节点管理
- `GET /ops/v1/tasks/{id}/debug` - 任务调试端点
- `GET /ops/v1/health` - Redis 健康检查

## 关键组件

### ServiceContainer
- **模式**: 依赖注入容器
- **方法**: `build_task_api()` / `build_ops_api()` (已删除遗留的 `build()`)
- **设计**: 边界明确，避免不必要的依赖

### QueueManager (`src/platform/queue_manager.py`)
- **存储**: Redis Sorted Sets
- **键模式**: `queue:{capability}:{pending|running|stats}`
- **特性**: Lua 脚本原子操作，TTL 自动续期

### TaskCreator (`src/platform/task_creator.py`)
- **幂等性**: 基于 `idempotency_key` 的确定性 task_id (SHA256)
- **验证**: 提前检查，避免重复工作
- **集成**: 与 QueueManager 解耦

### CronScheduler (`src/platform/cron_scheduler.py`)
- **位置**: 仅在 Task API (依赖 TaskCreator → MySQL)
- **触发**: 定时调用 `task_creator.create_task()`
- **配置**: 通过 `settings.background.cron`

## 包结构

### 核心框架 (`src/`)
- `src/common/` - 通用工具 (容器, 生命周期, Redis/DB 客户端)
- `src/platform/` - 平台核心 (QueueManager, TaskCreator, DAG 引擎)
- `src/api/` - HTTP 路由层
- `src/models/` - SQLAlchemy 模型
- `src/cli/` - CLI 客户端 (基于 CrudCommandGroup)

### 独立包 (`packages/`)
- `async-agent` - Node Agent，管理 worker 生命周期
- `async-proxy` - 代理服务，转发任务到外部
- `async-worker` - Worker SDK，包含 `AsyncProxyWorker` 基类

## 配置系统

### 单一真实源: `config/settings_pydantic.py`
- **基于**: Pydantic Settings + 环境变量
- **层级**: 嵌套配置类 (RedisConfig, MySQLConfig, TenantConfig 等)
- **已淘汰**: `config/settings_compat.py` (兼容层已删除)

**环境变量示例**:
```bash
REDIS_URL=redis://localhost:6379/0
MYSQL_URL=mysql://root:@localhost/scheduler
SERVER_PORT=8001
TENANT_MULTI_TENANT_ENABLED=false
```

## 测试策略

- **单元测试**: 组件隔离测试
- **集成测试**: API 路由 + Redis/DB 集成
- **当前状态**: 138/138 测试通过

## 部署拓扑

```yaml
version: '3.8'
services:
  task-api:
    image: async-scheduler:latest
    command: ["uvicorn", "src.main_tasks:app", "--host", "0.0.0.0", "--port", "8001"]
    environment:
      - REDIS_URL=redis://redis:6379/0
      - MYSQL_URL=mysql://mysql:3306/scheduler
    ports:
      - "8001:8001"

  ops-api:
    image: async-scheduler:latest  
    command: ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
    environment:
      - REDIS_URL=redis://redis:6379/0
    ports:
      - "8000:8000"

  node-agent:
    image: async-agent:latest
    environment:
      - AGENT_NODE_ID=worker-1
      - SCHEDULER_URL=http://task-api:8001
```

## 演进历史

1. **单体架构** → **API 拆分** (Task API + Ops API)
2. **手动配置** → **Pydantic Settings**
3. **代码重复** → **DRY CLI (CrudCommandGroup)**
4. **伪幂等** → **真幂等 (确定性 task_id)**
5. **缺少调试** → **统一调试端点**

## 设计原则

1. **边界明确**: Task API (业务) 与 Ops API (运维) 分离
2. **依赖最小化**: Ops API 不依赖 MySQL
3. **幂等优先**: 任务创建必须是幂等的
4. **可观测性**: 健康检查、调试端点、结构化日志
5. **向后兼容**: 保留废弃接口但标记 deprecated