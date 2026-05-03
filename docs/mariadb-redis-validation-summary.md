# MariaDB + Redis 本机验证阶段总结

## 本轮目标

在 **无法安装 Docker、无法直接安装系统 MySQL/Redis** 的限制下，完成：

1. 项目从 SQLite 默认思路迁移到 **MySQL 家族部署思路**
2. 在本机通过**用户态 Redis + 用户态 MariaDB** 跑通核心链路
3. 验证分布式调度 / lease / reconciler / observability 等关键测试

## 本轮完成的关键改造

### 1. 配置体系
- 新增统一 `async_scheduler/settings.py`
- 新增增强配置层 `async_scheduler/config/`
- 支持 `TEST_DATABASE_URL` 优先级
- 持久化层改为 **懒加载 / 可重绑 engine**

### 2. 部署交付物
- `Dockerfile`
- `.dockerignore`
- `docker-compose.yml`
- `docker/entrypoint.sh`
- `.env.example`
- `scripts/build_images.sh`
- `scripts/run_local_split.sh`
- `docs/deployment/docker.md`
- `docs/configuration.md`
- `docs/testing.md`

### 3. MySQL/MariaDB 兼容改造
- repository 层移除 `RETURNING` 依赖
- `drop_db()` 改为 MySQL 友好的逐表 `DROP TABLE IF EXISTS`
- `ExecutionAttempt` 最新记录查询改为稳定排序：
  - `created_at desc`
  - `retry_index desc`
  - `id desc`
- 时间精度断言适配 MariaDB 秒级行为

### 4. Redis 分布式锁兼容修复
- 根因定位：Redis 锁 TTL 被按秒取整，导致 `0.05s` 实际变成 `1s`
- 修复：
  - `SET ex=` → `SET px=`
  - `expire()` → `pexpire()`
  - compare-expire Lua 脚本改为毫秒精度
- 结果：lease loss / stale lease / dual reconciler 相关测试恢复正确

### 5. 测试工程
- 新增 `tests/conftest.py`
- 支持 `mysql_required` / `redis_required` marker
- 支持 MySQL / Redis 可达性门禁
- 测试文件级数据库隔离
- Redis `flushdb()` 隔离

## 本机用户态中间件

### Redis
- 源码编译启动成功
- 本地地址：`127.0.0.1:6379`

### MariaDB
- 用户态二进制启动成功
- 本地地址：`127.0.0.1:3307`
- 测试库用户：`async_scheduler / async_scheduler`

## 核心回归结果

本轮在本机 **Redis + MariaDB** 环境下跑通：

- `tests/test_task_lifecycle.py`
- `tests/test_execution_attempts.py`
- `tests/test_observability_api.py`
- `tests/test_consumer_attempt_consistency.py`
- `tests/test_completion_idempotency.py`
- `tests/test_distributed_claim_flow.py`
- `tests/test_distributed_reconciler.py`

### 结果

```text
44 passed
```

## 当前结论

项目已经不再只是 SQLite/in-memory 假设，已经能够在：

- **MariaDB（MySQL 家族兼容）**
- **Redis**

的真实本机环境下跑通任务生命周期、attempt、observability、distributed claim、reconciler 等核心链路。

## 后续建议

1. 提交本轮改动
2. 如后续拿到官方 MySQL 8 环境，再补一轮最终验证
3. 可继续扩展更多 integration tests 到当前 MariaDB + Redis 环境
4. 后续如需要，再验证 Docker / compose / 多进程部署链路
