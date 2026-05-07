<!-- 中文文档 -->
# Async Scheduler

> 面向企业的高性能异步任务调度框架，支持 DAG 编排、多租户隔离与潮汐资源管理

## 简介

Async Scheduler 是一个高性能、可横向扩展的分布式任务调度系统，专为现代分布式应用设计。核心能力：

- **DAG 工作流编排** — 声明式定义复杂任务依赖关系
- **多租户隔离** — 为不同团队/业务方提供资源配额隔离
- **潮汐资源管理** — 应对可预期的流量高峰
- **实时可观测性** — 内置 Prometheus 指标与 Grafana 仪表板
- **生产就绪** — Kubernetes 原生，支持健康检查与自动扩缩容

## 快速导航

<div class="grid cards" markdown>

- :fontawesome-solid-rocket: **快速启动**

  使用 Docker Compose，几分钟内完成部署。

  [快速启动 →](getting-started/quick-start.md)

- :material-architecture: **架构设计**

  了解系统架构与数据流转。

  [架构设计 →](architecture/overview.md)

- :material-api: **API 参考**

  查阅 Task API 与 Ops API 接口文档。

  [API 参考 →](api/task-api.md)

- :material-monitor-dashboard: **监控运维**

  配置 Prometheus、Grafana 与告警规则。

  [监控运维 →](guides/monitoring.md)

</div>

## 功能亮点

### 🔄 DAG 工作流编排

使用 Python 装饰器定义多步骤任务流，清晰表达依赖关系：

```python
from async_worker import AsyncProxyWorker

@dag.task("download_data")
async def download_data():
    return {"url": "http://example.com/data.csv"}

@dag.task("process_data", depends_on=["download_data"])
async def process_data(data):
    # 处理下载的数据
    return {"processed": True}

@dag.task("generate_report", depends_on=["process_data"])
async def generate_report(processed_data):
    # 生成最终报告
    return {"report_url": "http://example.com/report.pdf"}
```

### 🏢 多租户支持

为每个租户配置独立的资源配额与能力白名单：

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

### 📊 实时监控

内置 Prometheus 指标与 Grafana 仪表板，覆盖：

- 各 capability 的队列深度
- 任务执行时长分布
- 成功率与失败率
- 系统资源利用率
- 自定义业务指标

### 🚀 生产就绪

- 健康检查与就绪探针
- 优雅关机
- 熔断器模式
- 带退避策略的自动重试
- 失败任务的死信队列

## 快速上手

### 环境要求
- Python 3.10+
- Redis 7+
- MySQL 8+
- Docker（可选）

### 安装

```bash
# 克隆仓库
git clone https://github.com/zhbxlm/async-scheduler-framework.git
cd async-scheduler-framework

# 安装依赖
pip install -e .[metrics]

# 使用 Docker Compose 启动
docker-compose -f docker-compose.monitoring.yml up -d
```

### 基础用法

```python
import httpx

# 提交一个任务
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

    print(f"任务已创建: {response.json()}")
```

## 社区

- **GitHub**: [zhbxlm/async-scheduler-framework](https://github.com/zhbxlm/async-scheduler-framework)
- **Issues**: [提交 Bug 或功能请求](https://github.com/zhbxlm/async-scheduler-framework/issues)
- **Discussions**: [提问与讨论](https://github.com/zhbxlm/async-scheduler-framework/discussions)

## 许可证

MIT License — 详见 [LICENSE](https://github.com/zhbxlm/async-scheduler-framework/blob/main/LICENSE)。
