# CLI Reference

The `scheduler` command provides a kubectl-style interface for managing the async-scheduler cluster.

## Installation

```bash
pip install async-scheduler-cli
```

## Global Options

```
scheduler [--url URL] [--api-key KEY] [--output json|table] <command>
```

| Option | Env Var | Default |
|--------|---------|---------|
| `--url` | `SCHEDULER_URL` | `http://localhost:8000` |
| `--api-key` | `SCHEDULER_API_KEY` | _(empty)_ |
| `--output` | `SCHEDULER_OUTPUT` | `table` |

## Commands

### Task Commands

```bash
scheduler task submit --dag-id train_v1 --capability gpu_training --input '{"batch":32}'
scheduler task get <task-id>
scheduler task cancel <task-id>
scheduler task list [--status running] [--capability gpu_training] [--limit 20]
```

### Cluster & Node Commands

```bash
scheduler cluster list
scheduler cluster get <cluster-id>
scheduler node list [--cluster <id>]
scheduler node get <node-id>
```

### Capability Commands

```bash
scheduler capability list
scheduler capability get <name>
scheduler capability register --file capability.yaml
```

### DAG Commands

```bash
scheduler dag list
scheduler dag get <dag-id>
scheduler dag register --file dag.yaml
```

### Schedule Commands

```bash
scheduler schedule list
scheduler schedule get <id>
scheduler schedule create --dag-id <id> --cron "0 2 * * *"
scheduler schedule pause <id>
scheduler schedule resume <id>
```

### Tenant Commands

```bash
scheduler tenant list
scheduler tenant get <id>
scheduler tenant create --name my-team --quota '{"max_concurrent":100}'
```

### Deployment Commands

```bash
scheduler deploy node --capabilities gpu_training,data_pipeline
```
