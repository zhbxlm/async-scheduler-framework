# Ray AMU — Async Management Unit

> 基于 Ray 的企业级异步任务调度框架，提供 DAG 编排、多租户隔离、潮汐资源管理和长耗时服务代理能力。

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://python.org)
[![License MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

---

## 项目介绍

Ray AMU（Asynchronous Management Unit）构建在 Ray 分布式计算框架之上，结合 FastAPI、Redis 和 MySQL，提供完整的异步任务调度解决方案：

- **DAG 编排引擎** — 拓扑排序、并行扇出、条件分支、MAP scatter-gather、STREAMING 流式处理
- **任务执行引擎** — 分布式执行锁、后台锁续约、取消信号检测
- **调度 Actor** — Ray Detached Named Actor，多 capability ActorPool，支持灰度发布
- **队列管理器** — Redis Sorted Set 优先级队列、三态熔断器（CLOSED/OPEN/HALF_OPEN）、Lua 原子操作
- **资源管理器** — 4 阶段节点分配（SELECT → RESERVE → INVITE → CONFIRM）、自动扩缩容
- **Cron 调度器** — Redis Leader 选举、幂等触发、分布式防重
- **配额执行器** — 多租户配额管理、Redis Lua atomic check-increment
- **异步代理** — 长耗时 Flask 服务 Sidecar，Redis Pub/Sub 异步通知
- **节点代理** — 资源探测、心跳所有权协议、Ray 集群加入/退出
- **CLI 工具** — kubectl 风格命令行，管理集群/能力/任务/节点/调度/租户

---

## 项目结构

```
ray-amu/
├── config/                  # 分层配置
│   ├── settings.py          # 全局单例 settings
│   ├── _infra.py            # Redis + MySQL + 服务器配置
│   ├── _task.py             # 任务执行配置
│   ├── _dag.py              # DAG 编排配置
│   ├── _scaling.py          # 扩缩容 + 熔断器配置
│   ├── _background.py       # 后台守护任务配置
│   └── _tenant.py           # 多租户配置
├── src/
│   ├── main.py              # API 服务入口
│   ├── main_task_api.py     # Task API 独立部署入口
│   ├── api/                 # RESTful API (FastAPI)
│   │   ├── auth.py          # API Key 认证
│   │   ├── dependencies.py  # FastAPI 依赖注入
│   │   └── routes/          # tasks / dags / clusters / capabilities / nodes /
│   │                        # schedules / tenants / ops
│   ├── cli/                 # CLI 工具 (Click)
│   │   ├── main.py          # ray-amu 主命令
│   │   ├── client.py        # HTTP 客户端
│   │   └── commands/        # cluster / capability / node / task / schedule /
│   │                        # queue / deploy / tenant / worker / dag
│   ├── agent/               # 节点代理
│   │   ├── server.py        # FastAPI HTTP 服务
│   │   ├── config.py        # 代理配置
│   │   ├── heartbeat.py     # 所有权协议 + 心跳 (Lua CAS)
│   │   ├── resource_detector.py  # CPU/GPU/内存探测 (60s 缓存)
│   │   ├── ray_manager.py   # ray start/stop 幂等管理
│   │   └── deploy_manager.py     # 部署包下载/校验/解压
│   ├── common/
│   │   ├── db.py            # SQLAlchemy Base + get_db()
│   │   └── redis_client.py  # Redis 客户端工厂
│   ├── models/              # Pydantic/ORM 数据模型
│   │   ├── task.py          # TaskRecord / TaskStatus / TaskPriority / TaskDispatchMode
│   │   ├── dag.py           # DagStep / DagDefinition / DagContext / StepKind
│   │   ├── capability.py    # CapabilityInfo / ActorConfig
│   │   ├── cluster.py       # ClusterInfo / ClusterResources
│   │   ├── node.py          # NodeInfo / NodeState / NodeResources / NodeLease
│   │   ├── schedule.py      # ScheduleRecord / ScheduleInfo
│   │   ├── tenant.py        # TenantInfo / TenantQuota
│   │   ├── deploy.py        # DeployedPackageInfo / DeployRequest
│   │   └── tenant_context.py     # TenantContext (per-request)
│   ├── platform/            # 平台核心
│   │   ├── dag_engine.py    # DAG 执行引擎（含 STREAMING）
│   │   ├── dag_loader.py    # YAML + Redis DAG 加载
│   │   ├── step_executors.py     # Sync/Async/Map 步骤执行器
│   │   ├── task_executor.py      # 任务执行服务层
│   │   ├── task_consumer.py      # 任务消费循环
│   │   ├── task_router.py        # 任务路由
│   │   ├── task_completion_node.py  # 任务完成节点（持久化+回调）
│   │   ├── task_reconciler.py    # 三阶段对账修复
│   │   ├── cron_scheduler.py     # Cron 调度器（Leader 选举）
│   │   ├── schedule_registry.py  # 调度表 CRUD
│   │   ├── queue_manager.py      # 优先级队列 + 熔断器
│   │   ├── queue_keys.py         # Redis 键命名工具
│   │   ├── circuit_breaker.py    # 三态熔断器
│   │   ├── resource_manager.py   # 节点分配 + 自动扩缩容
│   │   ├── node_registry.py      # 节点注册表
│   │   ├── capability_registry.py  # 能力注册表
│   │   ├── cluster_registry.py   # 集群注册表
│   │   ├── quota_enforcer.py     # 配额执行器
│   │   ├── tenant_registry.py    # 租户注册表
│   │   ├── base_registry.py      # Redis 注册表基类
│   │   ├── raydata_client.py     # RayData HTTP 客户端
│   │   └── remote_code_fetcher.py  # 远程代码获取
│   ├── proxy/
│   │   ├── async_service_proxy.py  # 长耗时服务 Sidecar
│   │   └── async_command_proxy.py  # 命令代理
│   └── workload/
│       ├── scheduler_actor.py    # Ray Detached Actor 入口
│       ├── actor_pool_manager.py # ActorPool 管理
│       ├── base_worker_actor.py  # Worker 基类
│       ├── async_proxy_worker.py # 异步代理 Worker
│       ├── worker_dev_kit.py     # 开发调试工具
│       ├── node_registry.py      # 工作负载节点注册
│       └── resource_manager.py   # 工作负载资源管理
├── tests/                   # pytest 测试套件 (474+ 测试)
├── examples/                # 使用示例
├── scripts/                 # 验证脚本
│   ├── dag_deploy_verify.py # 4 类 DAG 部署验证
│   └── dag_streaming_verify.py  # STREAMING DAG 验证
├── Dockerfile               # 多阶段镜像 (api / task-api / agent)
├── docker-compose.yml       # 完整部署编排
├── pyproject.toml           # 包配置 (src/ 布局)
└── setup.py                 # 兼容 setuptools
```

---

## 快速开始

### 本地开发

```bash
# 1. 安装依赖
pip install -e ".[dev]"

# 2. 启动 Redis + MySQL (Docker)
docker-compose up -d redis mysql

# 3. 启动 API 服务
MYSQL_PORT=3307 MYSQL_DATABASE=async_scheduler_test uvicorn src.main:app --reload

# 4. 运行测试
pytest -q
```

### Docker 一键部署

```bash
# 构建并启动全部服务
docker-compose up -d

# 服务端口
# API:       http://localhost:8000
# Task API:  http://localhost:8001
# Agent:     http://localhost:9100
# MySQL:     localhost:3307
# Redis:     localhost:6379
```

### CLI 使用

```bash
# 查看集群状态
ray-amu cluster list

# 注册能力
ray-amu capability register --name cap_preprocess --endpoint http://worker:8080

# 提交任务
ray-amu task submit --capability cap_preprocess --payload '{"data": "..."}'

# 查看队列
ray-amu queue stats --capability cap_preprocess

# 创建 Cron 调度
ray-amu schedule create --name daily-job --cron "0 9 * * *" --capability cap_preprocess
```

---

## DAG 示例

### 线性 Pipeline

```python
from src.models.dag import DagDefinition, DagStep, RetryPolicy, ExecutionMode, StepKind
from src.platform.dag_engine import DagEngine

dag = DagDefinition(
    dag_id="pipeline_001",
    tenant_id="tenant_a",
    steps=[
        DagStep(step_name="A", capability="cap_preprocess", step_kind=StepKind.TASK,
                execution_mode=ExecutionMode.SYNC, depends_on=[], ...),
        DagStep(step_name="B", capability="cap_transform", step_kind=StepKind.TASK,
                execution_mode=ExecutionMode.SYNC, depends_on=["A"], ...),
    ],
)

engine = DagEngine()
ctx = await engine.execute(dag, my_dispatcher, initial_context={"task_id": "t1"})
```

### STREAMING 流式处理

```python
DagStep(
    step_name="stream_producer",
    step_kind=StepKind.STREAMING,
    streaming_trigger=StreamingTrigger(
        buffer_key="streaming:{task_id}:stream_producer",
        trigger_condition="chunk_ready",
        downstream_steps=["chunk_handler"],
        flush_on_complete=True,
    ),
    ...
)
```

Worker 通过 `rpush(buffer_key, json)` 推送 chunk，引擎消费并并发触发下游步骤，最后 `{"__done__": true, "summary": {...}}` 结束流。

---

## 配置

所有配置通过环境变量控制，优先级：**环境变量 > YAML 文件 > 代码默认值**

```bash
# Redis
REDIS_HOST=localhost  REDIS_PORT=6379  REDIS_DB=0

# MySQL
MYSQL_HOST=localhost  MYSQL_PORT=3306  MYSQL_USER=root  MYSQL_PASSWORD=  MYSQL_DATABASE=ray_amu

# API
API_HOST=0.0.0.0  API_PORT=8000  RAY_AMU_API_KEY=your-key

# DAG
DAG_MAX_PARALLELISM=8  DAG_HTTP_TIMEOUT_SECONDS=300

# Scaling
SCALE_UP_THRESHOLD=0.8  SCALE_DOWN_THRESHOLD=0.2  SCALE_COOLDOWN_SECONDS=300

# Agent
AGENT_NODE_ID=node-01  AGENT_PORT=9100
```

---

## 测试

```bash
# 全量测试 (474 个)
pytest -q

# 单模块
pytest tests/test_dag_engine.py -v

# 部署验证 (需要 MariaDB 3307)
python scripts/dag_deploy_verify.py      # 4 类 DAG
python scripts/dag_streaming_verify.py   # STREAMING DAG
```

---

## 架构参考

详细架构文档见 [`docs/deepwiki-reference/`](docs/deepwiki-reference/)：

| 文档 | 说明 |
|---|---|
| [项目概述](docs/deepwiki-reference/项目概述.md) | 系统架构总览 |
| [DAG 编排](docs/deepwiki-reference/DAG%20编排.md) | DAG 引擎设计 |
| [任务执行](docs/deepwiki-reference/任务执行.md) | 执行引擎与对账 |
| [队列管理](docs/deepwiki-reference/队列管理.md) | Redis 队列 + 熔断器 |
| [调度与资源管理](docs/deepwiki-reference/调度与资源管理.md) | 节点分配 + 扩缩容 |
| [节点代理](docs/deepwiki-reference/节点代理.md) | 所有权协议 + 资源探测 |
| [Cron 调度](docs/deepwiki-reference/Cron%20调度.md) | 分布式 Cron |
| [配额与多租户](docs/deepwiki-reference/配额与多租户.md) | 多租户隔离 |
| [配置说明](docs/deepwiki-reference/配置说明.md) | 完整配置参数 |
| [API 参考](docs/deepwiki-reference/API%20参考.md) | RESTful API |
| [命令行工具](docs/deepwiki-reference/命令行工具.md) | CLI 命令参考 |
| [Worker 开发](docs/deepwiki-reference/Worker%20开发.md) | 自定义 Worker |
| [异步代理](docs/deepwiki-reference/异步代理.md) | Sidecar 代理 |

---

## License

MIT
