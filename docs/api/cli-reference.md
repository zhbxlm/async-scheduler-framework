<!-- 中文文档 -->
# CLI 参考

`scheduler` 命令提供类似 kubectl 的交互界面，用于管理 async-scheduler 集群。

## 安装

```bash
pip install async-scheduler-cli
```

## 全局选项

```
scheduler [--url URL] [--api-key KEY] [--output json|table] <命令>
```

| 选项 | 环境变量 | 默认值 |
|------|---------|--------|
| `--url` | `SCHEDULER_URL` | `http://localhost:8000` |
| `--api-key` | `SCHEDULER_API_KEY` | （空） |
| `--output` | `SCHEDULER_OUTPUT` | `table` |

---

## 任务命令

```bash
# 提交任务
scheduler task submit --dag-id train_v1 --capability gpu_training --input '{"batch":32}'

# 查询任务详情
scheduler task get <task-id>

# 取消任务
scheduler task cancel <task-id>

# 列出任务（支持过滤）
scheduler task list [--status running] [--capability gpu_training] [--limit 20]
```

---

## 集群与节点命令

```bash
# 集群管理
scheduler cluster list
scheduler cluster get <cluster-id>

# 节点管理
scheduler node list [--cluster <id>]
scheduler node get <node-id>
```

---

## Capability 命令

```bash
scheduler capability list
scheduler capability get <name>
scheduler capability register --file capability.yaml
```

---

## DAG 命令

```bash
scheduler dag list
scheduler dag get <dag-id>
scheduler dag register --file dag.yaml
```

---

## 调度计划命令

```bash
scheduler schedule list
scheduler schedule get <id>
scheduler schedule create --dag-id <id> --cron "0 2 * * *"
scheduler schedule pause <id>
scheduler schedule resume <id>
```

---

## 租户命令

```bash
scheduler tenant list
scheduler tenant get <id>
scheduler tenant create --name my-team --quota '{"max_concurrent":100}'
```

---

## 部署命令

```bash
# 部署 Worker 节点，声明支持的 capabilities
scheduler deploy node --capabilities gpu_training,data_pipeline
```
