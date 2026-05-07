<!-- 中文文档 -->
# 安装

## 环境要求

- Python 3.10+
- Redis 7+
- MySQL 8+ / MariaDB 10.6+
- Docker & Docker Compose（推荐）

## 从 PyPI 安装

每个组件作为独立包发布，按需安装：

```bash
# Python HTTP 客户端（供调用方使用）
pip install async-scheduler-sdk

# 自定义 Worker 基类（零强制依赖，Ray 可选）
pip install async-scheduler-worker

# 嵌入已有服务的 fire-and-forget 分发代理
pip install async-scheduler-proxy

# kubectl 风格命令行工具
pip install async-scheduler-cli

# 任务提交服务（端口 8001）
pip install async-scheduler-task-api

# 运维管理服务（端口 8000）
pip install async-scheduler-ops-api

# Worker 节点 Agent
pip install async-scheduler-agent
```

## 从源码安装

```bash
git clone https://github.com/zhbxlm/async-scheduler-framework.git
cd async-scheduler-framework
pip install -e ".[metrics,dev]"
```

## 验证安装

```bash
scheduler version
scheduler-task-api --help
scheduler-ops-api  --help
```
