<!-- 中文文档 -->
# 代码风格

## 工具链

| 工具 | 用途 |
|------|------|
| **black** | 代码格式化（行长 100） |
| **flake8** | 代码检查 |
| **mypy** | 类型检查（可选，非阻断） |
| **bandit** | 安全扫描 |

## 运行检查

```bash
# 格式化
black src/ tests/

# Lint 检查
flake8 src/ tests/

# 类型检查（可选）
mypy src/ --ignore-missing-imports

# 安全扫描
bandit -r src/ -ll
```

## 命名规范

- **变量 / 函数**：`snake_case`
- **类名**：`PascalCase`
- **常量**：`UPPER_SNAKE_CASE`
- **模块**：`snake_case`

## Docstring 风格

使用 Google 风格的 Docstring：

```python
def submit_task(task_id: str, capability: str) -> dict:
    """提交任务到调度队列。

    Args:
        task_id: 任务唯一标识符。
        capability: 处理此任务所需的 capability 名称。

    Returns:
        包含 task_id 和 status 的字典。

    Raises:
        ValueError: 若 capability 不存在。
    """
```

## Pre-commit Hooks

项目配置了 pre-commit hooks，提交前会自动运行 black 和 flake8：

```bash
pre-commit install
```
