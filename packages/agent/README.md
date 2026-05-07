# async-scheduler-agent

Node Agent for [async-scheduler](https://github.com/zhbxlm/async-scheduler-framework).

Deploy a worker node with a single command — no need to understand framework internals.

## Install

```bash
pip install async-scheduler-agent
```

## Start via CLI

```bash
scheduler-agent start \
  --scheduler-url http://scheduler:8000 \
  --api-key secret \
  --capabilities image_resize,data_pipeline \
  --max-concurrent 16
```

## Or via Python

```python
from scheduler_agent import NodeAgent

agent = NodeAgent(
    scheduler_url="http://scheduler:8000",
    api_key="secret",
    capabilities=["image_resize", "data_pipeline"],
    max_concurrent_tasks=16,
)
await agent.start()
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `SCHEDULER_URL` | `http://localhost:8000` | Scheduler API URL |
| `SCHEDULER_API_KEY` | `` | API key |
| `AGENT_CAPABILITIES` | `` | Comma-separated capability names |
| `AGENT_MAX_CONCURRENT` | `8` | Max parallel tasks |
| `AGENT_HEARTBEAT_INTERVAL` | `30` | Heartbeat interval (seconds) |
