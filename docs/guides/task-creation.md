<!-- 中文文档 -->
# 任务创建

完整的可运行示例请参见 [快速启动](../getting-started/quick-start.md)。

## 通过 SDK 提交任务

```python
from scheduler_sdk import SchedulerClient

async with SchedulerClient("http://localhost:8001", api_key="secret") as client:
    task = await client.submit_task(
        dag_id="train_v1",
        capability="gpu_training",
        input_data={"batch_size": 32},
        callback_url="http://my-service/callback",
    )
    print(task["task_id"])
```

## 通过 CLI 提交任务

```bash
scheduler task submit \
  --dag-id train_v1 \
  --capability gpu_training \
  --input '{"batch_size": 32}'
```

## 轮询等待结果

```python
import asyncio

result = await client.wait_for_task(task["task_id"], poll_interval=2.0, timeout=300)
print(result["output"])
```

## 幂等性说明

所有任务创建请求基于 `task_id`（或 `idempotency_key`）保证幂等。
重复提交相同 `task_id` 的请求不会创建新任务，而是返回已有任务的状态。

## 优先级

提交时可指定 `priority` 字段，支持：

| 优先级 | 说明 |
|--------|------|
| `critical` | 最高优先级，优先调度 |
| `high` | 高优先级 |
| `normal` | 默认优先级 |
| `low` | 低优先级，资源充裕时执行 |
