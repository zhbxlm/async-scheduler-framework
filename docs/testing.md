# Testing

## 目标

项目现在支持 **MySQL 部署默认**，因此测试分为两类：

1. **纯单元/轻集成测试**：不依赖外部 MySQL
2. **MySQL 集成测试**：需要真实可连接 MySQL

## MySQL 集成测试

为需要真实数据库的测试打了标记：

```python
pytestmark = pytest.mark.mysql_required
```

当本机没有可用 MySQL 时，这些测试会自动跳过。

## 指定测试数据库

优先使用：

```bash
export TEST_DATABASE_URL=mysql+asyncmy://user:pass@host:3306/async_scheduler_test
```

如果未设置，则回退到：

```bash
DATABASE_URL
```

## 运行方式

### 仅跑基础测试

```bash
pytest -q tests/test_settings.py tests/test_config_module.py tests/test_service_container_settings.py
```

### 跑全部测试（有 MySQL 时）

```bash
export TEST_DATABASE_URL=mysql+asyncmy://user:pass@127.0.0.1:3306/async_scheduler_test
pytest -q
```

### 仅跑 MySQL 集成测试

```bash
pytest -q -m mysql_required
```

### 排除 MySQL 集成测试

```bash
pytest -q -m "not mysql_required"
```

## 当前限制

如果当前机器：
- 不能安装 Docker
- 没有系统级 MySQL / mysqld
- 没有外部可用 MySQL

则无法在本机真正执行 MySQL 集成测试，但不会阻塞基础测试和交付物演进。
