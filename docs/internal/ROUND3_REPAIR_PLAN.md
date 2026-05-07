# Round 3 Review - Repair Plan

## 概览

第三轮代码审查发现 **10 个架构与运维问题**，按优先级和复杂度分组：

- **P0 立即修复**：数据库迁移、关键索引、认证日志泄露
- **P1 本周完成**：优雅关闭、租户隔离、DAG引擎清理
- **P2 架构调整**：模型分离、限流、CLI重试、包结构统一

## 问题列表与优先级

| 编号 | 严重性 | 优先级 | 问题描述 | 影响 |
|------|--------|--------|----------|------|
| #1 | 🔴 | P0 | 无数据库迁移机制 | 生产环境无法安全变更schema |
| #6 | 🔴 | P0 | TaskRecord缺少关键索引 | 大表查询性能差 |
| #9 | 🔴 | P0 | 日志中泄露敏感数据 | 安全风险 |
| #2 | 🔴 | P1 | main.py缺少DagEngine.close()调用 | 资源泄漏 |
| #4 | 🟡 | P1 | 租户数据隔离不完整 | 多租户数据泄露 |
| #7 | 🟡 | P1 | graceful shutdown顺序问题 | 任务状态不一致 |
| #3 | 🟡 | P2 | 模型层DagDefinition包含执行逻辑泄露 | 架构不清晰 |
| #5 | 🟡 | P2 | 包结构不一致 | 代码维护性差 |
| #8 | 🔵 | P2 | 无请求限流 | 服务易被压垮 |
| #10 | 🔵 | P2 | CLI缺少连接重试 | 客户端容错性差 |

## 详细实施计划

### 🔴 P0: 立即修复（1-3天）

#### #1 数据库迁移机制（Alembic）

**现状**：只有conftest.py里`Base.metadata.create_all()`，无版本管理。

**修复步骤**：
```bash
1. pip install alembic
2. alembic init alembic
3. 修改alembic.ini配置MySQL连接
4. 生成初始迁移：alembic revision --autogenerate -m "init"
5. 集成到CI/CD流程
```

**文件修改**：
- `alembic/` (新目录)
- `alembic.ini`
- `pyproject.toml` (添加alembic依赖)
- `src/main_tasks.py` (启动时检查迁移)
- `.github/workflows/ci.yml` (添加迁移测试)

#### #6 TaskRecord索引

**现状**：`task_reconciler.py`查询`status`和`updated_at`无索引。

**修复步骤**：
1. 添加Alembic迁移添加索引
2. 复合索引：`(status, updated_at)`
3. 单列索引：`dag_id`

**迁移脚本**：
```sql
CREATE INDEX idx_task_status_updated ON task_records(status, updated_at);
CREATE INDEX idx_task_dag_id ON task_records(dag_id);
```

#### #9 API key日志掩码

**现状**：`auth.py`中`logger.debug("Auth: key=%s", api_key)`可能泄露。

**修复步骤**：
```python
# src/api/auth.py
def mask_api_key(api_key: str) -> str:
    if len(api_key) <= 8:
        return "****"
    return api_key[:4] + "***" + api_key[-4:]

# 在日志中使用：
api_key_masked = mask_api_key(api_key)
logger.debug("Auth: key=%s", api_key_masked)
```

### 🔴 P1: 本周完成（3-7天）

#### #2 DagEngine.close()调用

**现状**：`DagEngine`创建线程池但无统一清理。

**修复步骤**：
1. 在`DagEngine`类添加类方法清理所有executor
2. 在main.py/main_tasks.py生命周期调用

**代码修改**：
```python
# src/platform/dag_engine.py
class DagEngine:
    @classmethod
    async def close_all(cls):
        for executor in cls._executors:
            executor.shutdown(wait=True)
    
# src/main.py (在shutdown部分)
from src.platform.dag_engine import DagEngine
await DagEngine.close_all()
```

#### #4 租户数据隔离

**现状**：`ops_overview`返回所有租户的能力统计。

**修复步骤**：
```python
# src/api/routes/ops.py
async def ops_overview(request: Request, _auth: dict = Depends(authenticate)) -> dict:
    tenant_id = _auth.get("tenant_id", "default")
    
    # 仅返回该租户有权限的capabilities
    capabilities = await queue_manager.discover_queue_capabilities()
    if tenant_id != "super_admin":
        capabilities = [c for c in capabilities 
                       if c in tenant_allowed_capabilities]
    
    return {"capabilities": capabilities, ...}
```

#### #7 graceful shutdown顺序

**现状**：关闭顺序随机，可能导致reconciler误判任务状态。

**修复步骤**：
```python
# src/common/lifecycle.py
async def stop_all(self):
    # 1. 停止接收新任务
    if self.task_consumer:
        await self.task_consumer.stop()
    
    # 2. 等待运行中任务完成
    await asyncio.sleep(5)  # 等待drain
    
    # 3. 停止后台任务
    if self.task_reconciler:
        await self.task_reconciler.stop()
    if self.cron_scheduler:
        await self.cron_scheduler.stop()
```

### 🟡 P2: 架构调整（2-4周）

#### #3 模型层执行逻辑分离

**问题**：`DagDefinition`包含运行时参数（`map_over`, `streaming_trigger`）。

**解决方案**：
1. 创建`DagExecutionConfig`类存放运行时参数
2. 重构`DagEngine`使用分离后的配置

#### #5 包结构统一

**问题**：4种import风格混用。

**解决方案**：
1. 制定项目导入规范
2. 批量重构所有文件
3. 添加pre-commit检查

#### #8 请求限流

**解决方案**：
1. 安装slowapi或实现Redis令牌桶
2. 按租户+接口配置限流规则
3. 中间件集成

#### #10 CLI连接重试

**解决方案**：
1. 重构`SchedulerClient`增加指数退避
2. 网络异常时自动重试
3. 连接池管理

## 时间线

| 阶段 | 时间 | 任务 | 产出 |
|------|------|------|------|
| P0修复 | 第1-3天 | #1, #6, #9 | Alembic迁移、索引、日志掩码 |
| P1修复 | 第4-7天 | #2, #4, #7 | 优雅关闭、租户隔离、shutdown顺序 |
| P2规划 | 第2周 | #3, #5, #8, #10方案设计 | 架构设计文档 |
| P2实施 | 第3-4周 | 架构调整实现 | 重构后的代码 |

## 风险评估

1. **#1 数据库迁移**：高。需在开发环境充分测试，准备好回滚方案。
2. **#6 索引创建**：中。大表添加索引可能锁表，需在低峰期操作。
3. **#3 模型分离**：高。涉及DAG执行核心逻辑，需充分测试。
4. **#8 限流**：中。可能影响现有客户端，需渐进式部署。

## 验收标准

1. ✅ Alembic迁移正常工作，可回滚
2. ✅ 关键查询使用索引（EXPLAIN验证）
3. ✅ 日志中无明文API key
4. ✅ 服务关闭无资源泄漏
5. ✅ 租户只能看到自己的数据
6. ✅ 关闭顺序正确，无状态不一致
7. ✅ 模型定义清晰，无运行时泄露
8. ✅ 统一import风格
9. ✅ API有限流保护
10. ✅ CLI有重试机制

## 负责团队

- **平台团队**：#1, #6, #2, #7
- **安全团队**：#9, #4
- **架构团队**：#3, #5, #8, #10
- **质量保证**：全流程测试验证

---
**最后更新**：2026-05-06  
**状态**：计划制定完成，等待执行
