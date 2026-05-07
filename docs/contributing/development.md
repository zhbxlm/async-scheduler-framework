<!-- 中文文档 -->
# 开发环境搭建

## 克隆与安装

```bash
git clone https://github.com/zhbxlm/async-scheduler-framework.git
cd async-scheduler-framework
pip install -e ".[metrics,dev]"
pre-commit install
```

## 启动本地服务

```bash
# 启动 Redis + MySQL
docker compose up -d redis mysql

# 启动 ops-api（端口 8000）
export REDIS_URL=redis://localhost:6379/0
python -m uvicorn src.main:app --reload --port 8000

# 启动 task-api（端口 8001，另开终端）
export REDIS_URL=redis://localhost:6379/0
export MYSQL_URL=mysql+aiomysql://root:secret@localhost:3306/async_scheduler
python -m uvicorn src.main_tasks:app --reload --port 8001
```

## 运行测试

```bash
# 运行全部测试
pytest tests/ -q

# 详细模式
pytest tests/ -xvs
```

详细的测试说明请参见 [测试](testing.md)。

## 代码结构

```
src/
  common/       # 通用工具（容器、生命周期、Redis/DB 客户端）
  platform/     # 平台核心（QueueManager、TaskCreator、DAG 引擎）
  api/          # HTTP 路由层
  models/       # SQLAlchemy 模型
  cli/          # CLI 客户端（基于 CrudCommandGroup）
packages/
  async-agent/  # Node Agent
  async-proxy/  # 代理服务
  async-worker/ # Worker SDK
```

## 提交规范

提交信息使用 Conventional Commits 格式：

```
feat: 新增 capability 健康检查端点
fix: 修复 DAG 引擎步骤依赖解析 bug
docs: 更新 Kubernetes 部署文档
```
