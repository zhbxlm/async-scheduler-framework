# Configuration

项目当前采用两层配置体系：

## 1. 基础运行时设置 `ray_async.settings`

负责：
- 数据库配置
- 日志配置
- backend / Redis / lease 配置

主要入口：
- `load_settings(env=None)`
- `get_settings()`

## 2. 增强配置层 `ray_async.config`

负责：
- 环境识别（production / staging / test / development / local）
- dotenv 加载
- 临时覆盖（`ConfigContext`）
- 统一对外暴露 config 对象

### 示例

```python
from ray_async.config import config

print(config.environment)
print(config.database.url)
print(config.backends.redis_url)
```

### 查看当前配置

```bash
ray-async config --json
ray-async config --env
```

## 关键环境变量

### 数据库
- `DATABASE_URL`
- `SQL_ECHO`
- `DB_POOL_SIZE`
- `DB_MAX_OVERFLOW`
- `DB_POOL_TIMEOUT`
- `DB_POOL_RECYCLE`

### 日志
- `LOG_LEVEL`
- `LOG_FORMAT`
- `SERVICE_NAME`
- `SERVICE_VERSION`
- `NODE_ID`

### 分布式后端
- `REDIS_URL`
- `QUEUE_TYPE`
- `LOCK_TYPE`
- `REGISTRY_TYPE`
- `LEASE_TTL_SECONDS`
- `HEARTBEAT_INTERVAL_SECONDS`

### 环境
- `ENVIRONMENT=production|staging|test|development|local`

## 推荐实践

### 本地开发
```bash
ENVIRONMENT=local
LOG_LEVEL=DEBUG
QUEUE_TYPE=memory
LOCK_TYPE=memory
```

### 分服务部署
```bash
ENVIRONMENT=production
DATABASE_URL=mysql+asyncmy://user:pass@mysql:3306/ray_async
REDIS_URL=redis://redis:6379/0
QUEUE_TYPE=redis
LOCK_TYPE=redis
```
