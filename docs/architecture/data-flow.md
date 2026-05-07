<!-- 中文文档 -->
# 数据流

## 任务提交流程

```
调用方 → Task API (8001) → Redis 队列 → Worker 节点 → 回调 URL
                                ↓
                           MySQL（任务状态）
```

1. 调用方向 **Task API** 提交任务，携带 `capability`、`input_data`、可选的 `callback_url`
2. Task API 将任务记录写入 **MySQL**（状态 = `pending`）
3. Task API 将任务 ID 推入 **Redis** 对应 capability 队列
4. **Worker 节点**（通过 Agent）从队列中 BLPOP 取出任务
5. Worker 执行任务，更新 MySQL 中的状态（`running` → `success` / `failed`）
6. 若设置了 `callback_url`，Worker 在任务完成后将结果 POST 给调用方

## DAG 执行流程

对于多步骤 DAG，**DAG 引擎**负责步骤的顺序编排：

1. 根节点任务立即入队
2. 每个步骤完成后，DAG 引擎检查下游任务是否就绪
3. 就绪的任务被推入对应 capability 的队列
4. 所有叶子任务成功（或任一任务失败，取决于策略）时 DAG 完成

## 状态存储

| 数据 | 存储 | 有效期 |
|------|------|--------|
| 任务记录 | MySQL | 永久 |
| 待处理队列 | Redis List | — |
| 运行中任务锁 | Redis String | 2 × 超时时间 |
| 结果缓存 | Redis Hash | 可配置 |
| 指标数据 | Prometheus | 按抓取间隔 |
