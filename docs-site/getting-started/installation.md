# Installation

## Requirements

- Python 3.10+
- Redis 7+
- MySQL 8+ / MariaDB 10.6+
- Docker & Docker Compose (recommended)

## Install from PyPI

Each component is published as a separate package — install only what you need:

```bash
# Python HTTP client (for callers)
pip install async-scheduler-sdk

# Build your own Worker (zero required deps)
pip install async-scheduler-worker

# Embed fire-and-forget dispatch in an existing service
pip install async-scheduler-proxy

# Command-line tool (kubectl-style)
pip install async-scheduler-cli

# Deploy the Task submission service (port 8001)
pip install async-scheduler-task-api

# Deploy the Ops/Admin service (port 8000)
pip install async-scheduler-ops-api

# Deploy a Worker node agent
pip install async-scheduler-agent
```

## Install from Source

```bash
git clone https://github.com/zhbxlm/async-scheduler-framework.git
cd async-scheduler-framework
pip install -e ".[metrics,dev]"
```

## Verify

```bash
scheduler version
scheduler-task-api --help
scheduler-ops-api  --help
```
