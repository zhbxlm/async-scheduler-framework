<!-- 中文文档 -->
# DAG 编排

DAG 定义了具有明确依赖关系的多步骤工作流。

关于 DAG 的执行模型，请参见 [架构概述](../architecture/overview.md)。

## 注册 DAG

```bash
scheduler dag register --file my-dag.yaml
```

## DAG 定义示例

```yaml
id: train_pipeline
steps:
  - id: preprocess
    capability: data_pipeline

  - id: train
    capability: gpu_training
    depends_on: [preprocess]

  - id: evaluate
    capability: model_eval
    depends_on: [train]
```

## 执行策略

| 字段 | 可选值 | 说明 |
|------|--------|------|
| `on_failure` | `stop`（默认）/ `continue` | 任一步骤失败时的行为 |
| `timeout_seconds` | 整数 | 整个 DAG 的超时时间 |

## 查看 DAG 状态

```bash
# 列出所有已注册 DAG
scheduler dag list

# 查看某个 DAG 的定义
scheduler dag get train_pipeline
```

## 触发 DAG 执行

提交任务时指定 `dag_id` 即可触发 DAG 执行：

```bash
scheduler task submit --dag-id train_pipeline --input '{"dataset": "v2"}'
```

DAG 引擎会自动按依赖顺序调度各步骤，根节点任务立即入队，下游任务在其依赖全部完成后自动就绪。
