# Async Scheduler

> Enterprise async task scheduling framework with DAG orchestration, multi-tenancy and tidal resource management

## Overview

Async Scheduler is a high-performance, scalable task scheduling system designed for modern distributed applications. It provides:

- **DAG-based workflow orchestration** - Define complex task dependencies
- **Multi-tenancy support** - Isolate workloads for different teams/customers
- **Tidal resource management** - Handle predictable resource spikes
- **Real-time monitoring** - Built-in Prometheus metrics and Grafana dashboards
- **Production ready** - Kubernetes native with health checks and auto-scaling

## Quick Links

<div class="grid cards" markdown>

- :fontawesome-solid-rocket: **Quick Start**

  Get up and running in minutes with Docker Compose.
  
  [Quick Start →](getting-started/quick-start.md)

- :material-architecture: **Architecture**

  Learn about the system design and data flow.
  
  [Architecture →](architecture/overview.md)

- :material-api: **API Reference**

  Explore the Task API and Ops API endpoints.
  
  [API Reference →](api/task-api.md)

- :material-monitor-dashboard: **Monitoring**

  Set up Prometheus, Grafana, and alerting.
  
  [Monitoring →](guides/monitoring.md)

</div>

## Features

### 🔄 DAG Orchestration
Define complex workflows with task dependencies using simple Python decorators:

```python
from async_worker import AsyncProxyWorker

@dag.task("download_data")
async def download_data():
    return {"url": "http://example.com/data.csv"}

@dag.task("process_data", depends_on=["download_data"])
async def process_data(data):
    # Process the downloaded data
    return {"processed": True}

@dag.task("generate_report", depends_on=["process_data"])  
async def generate_report(processed_data):
    # Generate final report
    return {"report_url": "http://example.com/report.pdf"}
```

### 🏢 Multi-tenancy
Isolate workloads with tenant-specific quotas and policies:

```yaml
tenants:
  team-a:
    max_concurrent_tasks: 100
    priority_boost: 2
    allowed_capabilities: ["image_generate", "data_process"]
  
  team-b:
    max_concurrent_tasks: 50
    priority_boost: 1
    allowed_capabilities: ["report_generate"]
```

### 📊 Real-time Monitoring
Built-in Prometheus metrics and Grafana dashboards:

- Queue depth per capability
- Task execution duration distribution
- Success/failure rates
- System resource utilization
- Custom business metrics

### 🚀 Production Ready
- Health checks and readiness probes
- Graceful shutdown
- Circuit breaker pattern
- Automatic retry with backoff
- Dead letter queue for failed callbacks

## Get Started

### Prerequisites
- Python 3.10+
- Redis 7+
- MySQL 8+
- Docker (optional)

### Installation

```bash
# Clone the repository
git clone https://github.com/zhbxlm/async-scheduler-framework.git
cd async-scheduler-framework

# Install dependencies
pip install -e .[metrics]

# Start with Docker Compose
docker-compose -f docker-compose.monitoring.yml up -d
```

### Basic Usage

```python
import httpx

# Create a task
async with httpx.AsyncClient() as client:
    response = await client.post(
        "http://localhost:8001/api/v1/tasks",
        json={
            "task_id": "my_task_123",
            "capability": "image_generate",
            "payload": {"prompt": "A beautiful sunset"},
            "priority": "normal",
            "callback_url": "http://my-service/callback"
        }
    )
    
    print(f"Task created: {response.json()}")
```

## Community

- **GitHub**: [zhbxlm/async-scheduler-framework](https://github.com/zhbxlm/async-scheduler-framework)
- **Issues**: [Report bugs or request features](https://github.com/zhbxlm/async-scheduler-framework/issues)
- **Discussions**: [Ask questions and share ideas](https://github.com/zhbxlm/async-scheduler-framework/discussions)

## License

MIT License - see [LICENSE](https://github.com/zhbxlm/async-scheduler-framework/blob/main/LICENSE) file for details.