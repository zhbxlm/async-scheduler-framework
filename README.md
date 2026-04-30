# Async Scheduler Framework

一个本地可运行的异步调度框架 MVP，参考 ray-amu deepwiki 的能力边界，提供：

- FastAPI 任务 API
- SQLite 持久化
- 优先级队列与延时任务
- 任务消费循环
- 带超时 / 重试 / 取消的执行器
- DAG 编排引擎（依赖、并行、条件、skip/fallback 基础能力）
- Cron 定时调度
- Worker 抽象和示例 worker
- CLI 启动入口
- Capability Registry
- Tenant / Quota 基础治理
- Reconciler 后台修复

## 项目结构

```text
async_scheduler/
  api/           # FastAPI 应用
  cli/           # CLI 入口
  backends/      # Backend 抽象层（支持内存/Redis 等可插拔存储）🆕
  core/          # 核心模型与 consumer
  dag/           # DAG 引擎 / loader / step executors
  executor/      # 任务执行器
  persistence/   # SQLAlchemy + repository
  platform/      # router / quota / completion / reconciler / handlers
  queue/         # 优先级队列（Backend 抽象）
  registry/      # capability registry
  scheduler/     # cron scheduler + schedule registry（Backend 抽象）
  worker/        # worker 抽象与示例
tests/           # pytest 测试
examples/        # demo 文档
```

## 快速开始

### 1. 安装依赖

```bash
cd /home/gem/.openclaw/workspace/projects/async-scheduler-framework
python3 -m pip install -e .[dev] --no-build-isolation
```

### 2. 初始化数据库

```bash
async-scheduler init-db-cmd --force
```

### 3. 启动 API

```bash
async-scheduler api --init-db
```

打开：
- http://127.0.0.1:8000/docs
- http://127.0.0.1:8000/health

### 4. 启动开发模式

```bash
async-scheduler dev --init-db
```

### 5. 运行测试

```bash
pytest -q
```

## 常用 CLI

### 创建任务

```bash
async-scheduler task demo --payload '{"capability":"echo","message":"hello"}'
```

### 创建调度

```bash
async-scheduler schedule heartbeat '*/5 * * * *' --payload '{"capability":"echo","message":"tick"}'
```

### 查看状态

```bash
async-scheduler status
```

### 手动跑一次 reconciler

```bash
async-scheduler reconcile
```

## 常用 API

### 健康检查

```bash
curl http://127.0.0.1:8000/health
```

### 创建租户

```bash
curl -X POST http://127.0.0.1:8000/tenants \
  -H 'Content-Type: application/json' \
  -d '{"name":"team-a","config":{"max_queued":20,"max_running":5}}'
```

### 创建任务

```bash
curl -X POST http://127.0.0.1:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"compute-task",
    "tenant_id":"tenant-a",
    "idempotency_key":"demo-task-1",
    "payload":{"capability":"compute","operation":"add","a":1,"b":2}
  }'
```

### 查看能力列表

```bash
curl http://127.0.0.1:8000/capabilities
```

### 创建 Schedule

```bash
curl -X POST http://127.0.0.1:8000/schedules \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"heartbeat",
    "cron_expression":"*/5 * * * *",
    "dedup_window_seconds":60,
    "task_template":{"capability":"echo","message":"tick"}
  }'
```

### Pause / Resume Schedule

```bash
curl -X POST http://127.0.0.1:8000/schedules/<schedule_id>/pause
curl -X POST http://127.0.0.1:8000/schedules/<schedule_id>/resume
```

### 手动运行 Reconciler

```bash
curl -X POST http://127.0.0.1:8000/reconciler/run
```

## 端到端 Demo

见：

- `examples/end_to_end_demo.md`

## 与 deepwiki 的对齐边界

这个仓库当前应被视为：

- **ray-amu / deepwiki 设计启发下的单机版 MVP**
- **本地可运行、可测试、可演进的结构原型**
- **不是 deepwiki 分布式平台的等价实现**

### 已对齐的方向

- TaskRouter / Queue / Consumer / Executor / DAGEngine 主链
- 多租户 / quota 的基础治理语义
- task idempotency 与 schedule dedup 的平台语义
- capability registry 与 DAG loader 的基本分层
- completion node / schedule registry / reconciler / step executors 的结构存在
- CLI + API + persistence 的工程骨架

### 尚未完全对齐的部分

- 还没有 Ray / SchedulerActor / ActorPoolManager
- 还没有 Redis 队列、Lua 原子操作与分布式锁
- 还没有 ResourceManager / NodeAgent / Cluster 管理
- 还没有 Async Proxy sidecar 的真实实现
- DAG 模型仍是简化版，尚未完整覆盖 step_kind / map / streaming / flask_wrapped 全语义
- quota 仍是本地内存实现，不是 deepwiki 的 Redis 原子配额执行器
- reconciler 仍是本地轻量版，不是完整对账补偿链路

## 当前状态

这是一个 **已经基本可用的本地异步调度框架基线仓库**，适合继续做二次开发与逐步向 deepwiki 核心架构收敛。

## Backend 抽象层（Batch 1 - 已实现）

框架现在引入了 **Backend 抽象层**，为分布式 deepwiki 架构的对齐做准备。当前实现支持：

### 抽象接口

- **QueueBackend** - 任务队列后端抽象
  - `enqueue()` / `dequeue()` / `peek()` / `cancel()` / `update_priority()`
  - 支持优先级队列和延时任务
  - 当前实现：`InMemoryQueueBackend`（使用 `asyncio.PriorityQueue`）

- **LockBackend** - 分布式锁后端抽象
  - `acquire()` / `release()` / `extend()` / `is_locked()`
  - 当前实现：`InMemoryLockBackend`（使用 `asyncio.Lock`）

- **RegistryBackend** - 调度注册表后端抽象
  - `create()` / `get()` / `list_active()` / `list_ready()`
  - `advance_next_fire()` / `pause()` / `resume()`
  - 当前实现：`InMemoryRegistryBackend`（使用 SQLite 持久化）

### 使用方式

```python
from async_scheduler.backends import BackendConfig, BackendFactory

# 使用默认内存后端（当前行为）
from async_scheduler.platform import build_service_container
services = await build_service_container()

# 配置自定义后端（未来支持 Redis 等）
config = BackendConfig(
    queue_type="redis",  # 未来支持
    lock_type="redis",   # 未来支持
    registry_type="postgres",  # 未来支持
    queue_config={"url": "redis://localhost:6379"},
)
services = await build_service_container(backend_config=config)
```

### DeepWiki 分布式对齐路线图

| 阶段 | 状态 | 内容 |
|------|------|------|
| **Batch 1** | ✅ 已完成 | Backend 抽象层 + 内存实现，保持现有 API/CLI 行为不变 |
| **Batch 2** | 🔜 待实现 | Redis 后端实现（Queue, Lock） |
| **Batch 3** | 🔜 待实现 | PostgreSQL 后端实现（Registry） |
| **Batch 4** | 🔜 待实现 | 分布式协调器（Raft/Paxos 共识） |
| **Batch 5** | 🔜 待实现 | ResourceManager / NodeAgent 集成 |