# Worker 开发

## Worker 抽象基类

所有 Worker 继承自 `async_scheduler/worker/base.py` 中的 `Worker`：

```python
from async_scheduler.worker.base import Worker
from async_scheduler.core.models import Task

class MyWorker(Worker):
    async def execute(self, task: Task) -> dict:
        """
        执行任务，返回结果 dict。
        抛出异常将触发重试（未超过 max_retries）或 FAILED。
        """
        payload = task.payload
        # 实现你的逻辑
        return {"result": "ok", "data": payload.get("message")}
```

## Capability 注册

Worker 通过 `CapabilityRegistry` 与 capability 名称关联：

```python
from async_scheduler.registry.capability import CapabilityRegistry

registry = CapabilityRegistry()

# 注册单个 capability
async def echo_handler(task_type: str, payload: dict) -> dict:
    return {"echo": payload.get("message")}

registry.register(
    "echo",
    echo_handler,
    description="Echo back the message",
    version="1.0.0",
    tags=["demo"],
)
```

## 注册到 ServiceContainer

```python
from async_scheduler.platform import build_service_container

services = await build_service_container()

# 向全局 registry 注册 capability
services.registry.register(
    "my_capability",
    my_handler_func,
    description="My custom processor",
)
```

## TaskWorker — 从队列消费的完整 Worker

```python
from async_scheduler.worker.base import TaskWorker

class VideoWorker(TaskWorker):
    async def handle(self, task_type: str, payload: dict) -> dict:
        if task_type == "video_encode":
            return await self.encode(payload)
        raise ValueError(f"Unknown task_type: {task_type}")

    async def encode(self, payload: dict) -> dict:
        # 实际业务逻辑
        return {"encoded_url": "..."}
```

## 示例 Worker

`async_scheduler/worker/examples.py` 提供了几个开箱即用的示例：

```python
from async_scheduler.worker.examples import (
    EchoWorker,        # 回显 payload
    ComputeWorker,     # 简单数学计算
    SlowWorker,        # 模拟长时任务（测试超时用）
    FailingWorker,     # 模拟失败（测试重试用）
)
```

## 重试语义

- `max_retries=0`（默认）：失败后直接进入 `FAILED`
- `max_retries=3`：最多重试 3 次，每次创建新的 ExecutionAttempt
- 任何未捕获异常均触发重试计数

## Worker 生命周期（分布式模式）

```
Worker.start()
  └── WorkerRegistry.register(worker_info)       # 注册心跳
       └── background: extend_heartbeat()         # 定期续期

Worker 处理任务时：
  └── LockBackend.acquire(task_id, lease_token)  # 获取分布式 lease
       └── background: extend_lease()             # 定期续期 lease
            └── on done: LockBackend.release()   # 释放 lease
```

## Worker 注册表（WorkerRegistry）

Redis 模式下，worker 存活状态通过心跳维护：

```python
from async_scheduler.distributed.worker_registry import WorkerRegistry, WorkerInfo

registry = WorkerRegistry(redis_url="redis://localhost:6379/0")
info = WorkerInfo(worker_id="worker-1", name="main-worker")

await registry.register(info)

# 列出存活 worker（排除超时的）
workers = await registry.list_workers(include_stale=False)

# 检查单个 worker 是否存活
is_alive = await registry.is_alive("worker-1")
```

## 调试 Worker 状态

```bash
# 列出所有 worker（含失联的）
curl 'http://127.0.0.1:8000/workers?include_stale=true'

# 查看某 worker 持有的 lease
curl http://127.0.0.1:8000/workers/worker-1/leases
```
