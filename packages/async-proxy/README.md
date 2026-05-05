# async-proxy

Ray AMU Async Proxy — sidecar for long-running service async wrapping.

## Overview

The Async Proxy provides two components:
- **AsyncServiceProxy**: Sidecar HTTP proxy for long-running Flask/HTTP algorithm services
- **AsyncCommandProxy**: Executes local shell/Python commands via subprocess

Both components store results in Redis so workers can retrieve them via BLPOP.

## Installation

```bash
pip install async-proxy
```

## Usage

### AsyncServiceProxy

Running the service proxy:

```bash
# Basic usage
async-proxy

# With environment variables
export BACKEND_URL="http://my-service:8080"
export REDIS_URL="redis://redis-server:6379/0"
export PROXY_PORT="5000"
async-proxy
```

Programmatic usage:

```python
from async_proxy import create_proxy_app
from flask import Flask

app = create_proxy_app(
    backend_url="http://my-service:8080",
    redis_url="redis://redis-server:6379/0",
)
app.run(host="0.0.0.0", port=5000)
```

### AsyncCommandProxy

Running the command proxy:

```bash
python -m async_proxy.async_command_proxy
```

Programmatic usage:

```python
from async_proxy import AsyncCommandProxy

proxy = AsyncCommandProxy()

# Submit a command
result = proxy.submit(
    command="python",
    params={"-m", "pip", "list"},
    timeout=30
)

# Wait for result
job_id = result["job_id"]
result_key = result["result_key"]
final_result = proxy.blpop_result(result_key, timeout=30)
```

## API Endpoints

### AsyncServiceProxy

- `POST /submit` - Submit a job
- `GET /result/<job_id>` - Get job result
- `GET /health` - Health check

### AsyncCommandProxy

- `POST /submit` - Submit command
- `POST /cancel/<job_id>` - Cancel running job
- `GET /result/<job_id>` - Get job result
- `GET /health` - Health check

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BACKEND_URL` | `"http://localhost:8080"` | Backend service URL |
| `BACKEND_TIMEOUT` | `"600"` | Backend request timeout (seconds) |
| `REDIS_URL` | `"redis://localhost:6379/0"` | Redis connection URL |
| `RESULT_TTL` | `"3600"` | Result TTL in Redis (seconds) |
| `MAX_WORKERS` | `"32"` | Maximum concurrent workers |
| `PROXY_PORT` | `"5000"` | Service proxy port |
| `ASYNC_COMMAND_PROXY_PORT` | `"5002"` | Command proxy port |
| `DEFAULT_TIMEOUT` | `"300"` | Default command timeout (seconds) |

## License

Apache-2.0
