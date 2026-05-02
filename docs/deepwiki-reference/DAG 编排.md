# DAG 编排

## 概念

DAG（有向无环图）编排允许将多个任务步骤组织成有依赖关系的工作流。`DAGEngine` 负责拓扑排序、并发扇出、条件跳过和错误处理。

## 定义 DAG

### 通过 API

```bash
curl -X POST http://127.0.0.1:8000/dags \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "video-pipeline",
    "max_parallelism": 4,
    "nodes": [
      {
        "id": "preprocess",
        "task_type": "video_preprocess",
        "depends_on": [],
        "payload": {"resolution": "1080p"}
      },
      {
        "id": "encode",
        "task_type": "video_encode",
        "depends_on": ["preprocess"],
        "payload": {"codec": "h264"}
      },
      {
        "id": "upload",
        "task_type": "file_upload",
        "depends_on": ["encode"],
        "payload": {"bucket": "output"}
      }
    ]
  }'
```

### 通过代码

```python
from async_scheduler.core.models import DAG, DAGNode

dag = DAG(
    name="my-pipeline",
    nodes=[
        DAGNode(id="step1", task_type="preprocess", depends_on=[]),
        DAGNode(id="step2", task_type="compute", depends_on=["step1"]),
        DAGNode(id="step3a", task_type="export_csv", depends_on=["step2"]),
        DAGNode(id="step3b", task_type="export_json", depends_on=["step2"]),
        # step3a 和 step3b 并行执行
    ],
    max_parallelism=4,
)
```

## 执行 DAG

```bash
# 异步执行（立即返回，后台运行）
curl -X POST http://127.0.0.1:8000/dags/execute \
  -H 'Content-Type: application/json' \
  -d '{"dag_id": "dag-uuid"}'

# 创建并流式执行（SSE 实时进度）
curl -X POST http://127.0.0.1:8000/dags/stream \
  -H 'Content-Type: application/json' \
  -d '{"name":"pipeline","nodes":[...]}'
```

## 流式进度（SSE）

连接 SSE 端点获取实时执行进度：

```bash
# 先启动执行，再连接 SSE
curl http://127.0.0.1:8000/dags/{dag_id}/stream
```

事件示例：

```
data: {"event": "node_start", "node": "preprocess", "task_type": "video_preprocess"}
data: {"event": "node_done", "node": "preprocess", "result": {"status": "ok"}}
data: {"event": "node_start", "node": "encode", "task_type": "video_encode"}
data: {"event": "node_done", "node": "encode", "result": {"output": "video.mp4"}}
data: {"event": "dag_done", "status": "success", "node_statuses": {...}}
```

## 并行执行

同一 `depends_on` 层级的节点自动并行执行，受 `max_parallelism` 限制：

```
step1 ──┬── step2a ──┐
        └── step2b ──┴── step3
```

## 条件执行

通过 `condition` 字段控制节点是否执行：

```python
DAGNode(
    id="premium_step",
    task_type="premium_process",
    depends_on=["classify"],
    condition="context.get('is_premium', False)",  # Python 表达式
    payload={},
)
```

条件为 `False` 时节点状态为 `skipped`，不影响后续节点的执行。

## 错误处理

- 节点失败默认中止整个 DAG（`status=failed`）
- 通过 `on_skip` 配置跳过失败节点继续执行
- DAG 取消：`POST /dags/{dag_id}/cancel`

## 上下文传递

节点执行结果自动写入 `dag.context`，后续节点可通过 `context` 字典访问：

```python
# step1 返回
{"output_path": "/tmp/processed.mp4"}

# step2 的 condition 或 payload 中可访问
condition="context.get('output_path') is not None"
```

## 示例 DAG

`examples/dag_samples.py` 包含 5 个完整示例：

| 示例 | 说明 |
|------|------|
| `linear_pipeline` | 线性三步流水线 |
| `fan_out_fan_in` | 扇出并行 + 汇聚 |
| `conditional_dag` | 条件分支 |
| `error_recovery_dag` | 失败跳过 + fallback |
| `mixed_dag` | 混合依赖 + 条件 |

`examples/dag_streaming_samples.py` 包含 3 个流式 DAG 示例，演示 SSE 进度推送。

## Python API 直接调用

```python
from async_scheduler.platform import build_service_container

services = await build_service_container()

result_dag = await services.dag_engine.execute(dag, services.task_handler)
print(result_dag.status)           # "success" | "failed"
print(result_dag.node_executions)  # 每个节点的执行结果
```
