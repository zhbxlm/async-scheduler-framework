# async-worker

Ray Async Worker SDK — base classes for building Ray workers.

## Overview

The Worker SDK provides:
- **BaseWorkerActor**: Template-method base class for Ray actors
- **AsyncProxyWorker**: Worker that delegates to long-running HTTP services via AsyncServiceProxy
- **WorkerDevKit**: Local development and testing utilities

## Installation

### Basic installation (no dependencies)
```bash
pip install async-worker
```

### With Ray support
```bash
pip install async-worker[ray]
```

### With AsyncProxyWorker support
```bash
pip install async-worker[proxy]
```

### All features
```bash
pip install async-worker[all]
```

## Usage

### BaseWorkerActor

Create a simple worker with the template-method pattern:

```python
from async_worker import BaseWorkerActor

class MyWorker(BaseWorkerActor):
    def __init__(self, capability: str, config: dict | None = None):
        super().__init__(capability, config)
        # Initialize your model/resources here
        self.model = load_model(config)

    def pre_process(self, input_data: dict) -> dict:
        """Optional: transform input before processing."""
        return input_data

    def call(self, data: dict) -> dict:
        """Required: core business logic."""
        result = self.model.infer(data)
        return {"result": result}

    def post_process(self, result: dict) -> dict:
        """Optional: transform output."""
        return result

    def health_check(self) -> dict:
        """Optional: custom health checks."""
        return {"status": "healthy", "model_loaded": True}
```

### AsyncProxyWorker

Create a worker that delegates to an async service proxy:

```python
from async_worker import AsyncProxyWorker

class LLMWorker(AsyncProxyWorker):
    backend_path = "/generate"
    timeout = 300
    http_method = "POST"

    def transform_input(self, input_data: dict) -> dict:
        """Transform DAG input to backend format."""
        return {
            "prompt": input_data.get("prompt", ""),
            "max_tokens": input_data.get("max_tokens", 100),
        }

    def transform_output(self, data: dict) -> dict:
        """Transform backend response to DAG format."""
        return {
            "generated_text": data.get("text", ""),
            "tokens_used": data.get("tokens", 0),
        }
```

Configuration:

```python
config = {
    "proxy_url": "http://proxy-server:5000",
    "redis_url": "redis://redis-server:6379/0",
    "timeout": 300,
}

worker = LLMWorker(capability="llm", config=config)
result = worker.run({"prompt": "Hello, world!"})
```

### WorkerDevKit

Test workers locally without Ray:

```python
from async_worker import run_local_test, WorkerDevKit

# Simple test
result = run_local_test(MyWorker, {"input": "test"}, config={"model": "gpt-4"})

# Using DevKit
kit = WorkerDevKit()
kit.mock_capability("test_cap", lambda x: {"result": x["input"] * 2})
result = kit.dispatch("test_cap", {"input": 5})
kit.pretty_print(result)
```

## Classes

### BaseWorkerActor

Template-method base class with hooks:
- `__init__(capability, config)` - Initialize worker
- `pre_process(input_data)` - Transform input (default: identity)
- `call(data)` - Core logic (must override)
- `post_process(result)` - Transform output (default: identity)
- `run(input_data)` - Main entry point
- `health_check()` - Health status

### AsyncProxyWorker

Worker for long-running services:
- `backend_path` - Backend API path (required)
- `timeout` - BLPOP wait seconds (default: 300)
- `http_method` - HTTP method (default: "POST")
- `transform_input(data)` - Transform input for backend
- `transform_output(data)` - Transform output from backend
- `health_check()` - Check proxy and Redis (async)

### WorkerDevKit

Local testing utilities:
- `mock_capability(name, handler)` - Register mock handler
- `dispatch(capability, input_data)` - Dispatch to mock
- `local_runner(worker_class, ...)` - Run worker locally
- `assert_output(result, expected_keys)` - Assert output structure
- `pretty_print(result)` - Pretty-print JSON

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PROXY_URL` | `"http://localhost:5000"` | AsyncServiceProxy URL |
| `REDIS_URL` | `"redis://localhost:6379/0"` | Redis connection URL |

## License

Apache-2.0
