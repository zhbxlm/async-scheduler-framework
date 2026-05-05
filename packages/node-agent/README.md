# async-agent

Ray Async Node Agent — lightweight HTTP service for cluster node management.

## Overview

The Node Agent runs on each cluster machine and handles:
- Ownership protocol with Redis
- Heartbeat and health reporting
- Resource detection (CPU, memory, GPU)
- Ray cluster join/leave
- Package deployment and undeployment

## Installation

### Installation
```bash
pip install async-agent
```

`ray` is a required dependency because the agent manages local Ray lifecycle via the `ray` CLI.

### Optional Redis support
```bash
pip install async-agent[redis]
```

## Usage

### Running the agent

```bash
# Basic usage
async-agent

# With environment variables
export AGENT_NODE_ID="node-001"
export AGENT_HOST="192.168.1.10"
export AGENT_PORT="9100"
export REDIS_URL="redis://redis-server:6379/0"
async-agent
```

### Programmatic usage

```python
from async_agent import create_agent_app, AgentConfig
import redis.asyncio as redis

redis_client = redis.from_url(AgentConfig.redis_url)
app = create_agent_app(
    node_id="node-001",
    host="192.168.1.10",
    agent_port=9100,
    redis_client=redis_client,
)

import uvicorn
uvicorn.run(app, host="0.0.0.0", port=9100)
```

## API Endpoints

- `GET /health` - Health check
- `POST /invite` - Accept cluster invitation
- `POST /release` - Release from cluster
- `POST /drain` - Start draining
- `GET /resources` - Get node resources
- `GET /node` - Get full node info
- `POST /deploy` - Deploy a package
- `POST /undeploy` - Undeploy a package
- `GET /packages` - List deployed packages

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENT_NODE_ID` | `""` | Unique node identifier |
| `AGENT_HOST` | `"127.0.0.1"` | Node hostname/IP |
| `AGENT_PORT` | `"9100"` | Agent listening port |
| `REDIS_URL` | `"redis://localhost:6379/0"` | Redis connection URL |
| `AGENT_HEARTBEAT_INTERVAL` | `"10"` | Heartbeat interval in seconds |
| `AGENT_OWNER_TTL` | `"30"` | Ownership TTL in seconds |
| `LOG_LEVEL` | `"INFO"` | Logging level |

## License

Apache-2.0
