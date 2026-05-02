# RayData 集成

## 当前状态

RayData 集成是 deepwiki 路线图中与 Ray 框架对接的高级功能，**本仓库当前不包含 Ray 依赖，也未实现 Ray 相关集成**。

## 背景

deepwiki 的生产实现使用 Ray 作为分布式计算引擎，提供：
- `SchedulerActor`：Ray 远程 Actor 实现的调度器
- `ActorPoolManager`：管理 capability 对应的 Ray Actor 池
- `ResourceManager`：基于 Ray 集群资源信息的弹性伸缩

## 本仓库的对应实现

| deepwiki / Ray 组件 | 本仓库等价实现 | 状态 |
|--------------------|--------------|------|
| SchedulerActor | TaskRouter + TaskConsumer | ✅ 已实现（asyncio）|
| ActorPoolManager | `platform/actor_pool.py` 骨架 | 🔧 仅接口 |
| ResourceManager | `platform/resource_manager.py` 骨架 | 🔧 仅接口 |
| Ray remote task | CapabilityRegistry handler | ✅ 已实现（本地函数）|
| Ray Serve | FastAPI app | ✅ 已实现 |

## 从本仓库迁移到 Ray

若需要对接真实 Ray 集群，需要：

1. 将 `TaskExecutor` 中的 `asyncio` 调用替换为 Ray remote 调用
2. 用 `ray.init()` 替换 `build_service_container()` 中的进程内初始化
3. 将 `RedisQueueBackend` 对接到 Ray GCS（Global Control Store）
4. 实现真实的 `ActorPoolManager`（当前为骨架）

详见 `docs/reference/deepwiki-distributed-architecture-reference.md`。
