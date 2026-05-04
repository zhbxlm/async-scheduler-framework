"""
STREAMING DAG 类型部署验证 (纯 asyncio MockRedis)
- SQL: 127.0.0.1:3307
- 覆盖: pre → stream_producer (chunks×N, downstream chunk_handler) → finalize
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from datetime import datetime
from typing import Any

sys.path.insert(0, ".")

from sqlalchemy import text
from src.common.db import init_engine, get_db
from src.models.dag import (
    DagDefinition, DagStep, RetryPolicy, ExecutionMode,
    OnFailureAction, StepKind, DagStatus, StreamingTrigger,
)
from src.platform.dag_engine import DagEngine

# ── DB 初始化 ──────────────────────────────────────────────
DB_URL = "mysql+pymysql://root@127.0.0.1:3307/async_scheduler_test?charset=utf8mb4"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dag_streaming_log (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    dag_id          VARCHAR(128) NOT NULL,
    status          VARCHAR(32)  NOT NULL,
    steps_done      INT          NOT NULL DEFAULT 0,
    chunks_received INT          NOT NULL DEFAULT 0,
    output_json     TEXT,
    started_at      DATETIME     NOT NULL,
    finished_at     DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def setup_db():
    init_engine(DB_URL, echo=False, pool_pre_ping=True)
    with get_db() as db:
        db.execute(text(SCHEMA_SQL))
        db.commit()
    print("✅ DB schema ready (dag_streaming_log @ 3307)")


def persist_streaming_result(dag_id: str, ctx, chunks: int, started: float):
    with get_db() as db:
        db.execute(text("""
            INSERT INTO dag_streaming_log
                (dag_id, status, steps_done, chunks_received, output_json, started_at, finished_at)
            VALUES
                (:dag_id, :status, :steps_done, :chunks_received, :output_json, :started_at, :finished_at)
        """), {
            "dag_id": dag_id,
            "status": ctx.status.value,
            "steps_done": len(ctx.completed_steps),
            "chunks_received": chunks,
            "output_json": json.dumps(ctx.context, ensure_ascii=False),
            "started_at": datetime.fromtimestamp(started),
            "finished_at": datetime.now(),
        })
        db.commit()


def verify_streaming_db(dag_id: str) -> dict:
    with get_db() as db:
        row = db.execute(
            text("SELECT status, steps_done, chunks_received, output_json "
                 "FROM dag_streaming_log WHERE dag_id=:d ORDER BY id DESC LIMIT 1"),
            {"d": dag_id}
        ).fetchone()
    return dict(row._mapping) if row else {}


# ── Async-native MockRedis ─────────────────────────────────
class AsyncMockRedis:
    """Pure asyncio queue — no threads needed."""

    def __init__(self):
        self._queues: dict[str, asyncio.Queue] = {}

    def _q(self, key: str) -> asyncio.Queue:
        if key not in self._queues:
            self._queues[key] = asyncio.Queue()
        return self._queues[key]

    def rpush(self, key: str, value: str) -> None:
        """Non-blocking push (safe to call from within event loop)."""
        self._q(key).put_nowait(value)

    async def async_blpop(self, key: str, timeout: int = 30):
        """Awaitable pop — integrates naturally with asyncio."""
        try:
            val = await asyncio.wait_for(self._q(key).get(), timeout=float(timeout))
            return (key, val)
        except asyncio.TimeoutError:
            return None

    async def delete(self, key: str) -> None:
        self._queues.pop(key, None)


# ── Build STREAMING DAG ────────────────────────────────────
def build_streaming_dag(task_id: str = "task_stream_001") -> DagDefinition:
    return DagDefinition(
        dag_id=f"dag_streaming_{task_id}",
        tenant_id="tenant_test",
        steps=[
            # 前置普通步骤
            DagStep(
                step_name="pre",
                capability="cap_pre",
                step_kind=StepKind.TASK,
                execution_mode=ExecutionMode.SYNC,
                depends_on=[],
                input_mapping={},
                output_mapping={"pre_out": "result"},
                retry_policy=RetryPolicy(max_retries=0),
                timeout_seconds=10,
            ),
            # STREAMING 生产者
            DagStep(
                step_name="stream_producer",
                capability="cap_stream",
                step_kind=StepKind.STREAMING,
                execution_mode=ExecutionMode.ASYNC,
                depends_on=["pre"],
                input_mapping={},
                output_mapping={
                    "total_chunks": "total_chunks",
                    "producer_result": "producer_result",
                },
                streaming_trigger=StreamingTrigger(
                    buffer_key=f"streaming:{{task_id}}:stream_producer",
                    trigger_condition="chunk_ready",
                    flush_on_complete=True,
                    downstream_steps=["chunk_handler"],
                ),
                retry_policy=RetryPolicy(max_retries=0),
                timeout_seconds=30,
            ),
            # 聚合步骤
            DagStep(
                step_name="finalize",
                capability="cap_finalize",
                step_kind=StepKind.COLLECTOR,
                execution_mode=ExecutionMode.SYNC,
                depends_on=["stream_producer"],
                input_mapping={
                    "_streaming_results_chunk_handler": "_streaming_results_chunk_handler",
                },
                output_mapping={
                    "aggregated_count": "aggregated_count",
                    "final": "final",
                },
                retry_policy=RetryPolicy(max_retries=0),
                timeout_seconds=10,
            ),
        ],
        max_concurrent=4,
    )


# ── Run & Verify ───────────────────────────────────────────
async def run_streaming_verify(num_chunks: int = 5):
    task_id = "task_stream_001"
    dag_def = build_streaming_dag(task_id)
    buffer_key = f"streaming:{task_id}:stream_producer"

    mock_redis = AsyncMockRedis()
    chunk_payloads = [f"video_frame_{i}" for i in range(num_chunks)]
    chunks_received: list[dict] = []

    async def dispatcher(capability: str, step_name: str, payload: dict) -> dict:
        if step_name == "stream_producer":
            # Launch async producer as background task
            async def produce():
                await asyncio.sleep(0.01)
                for i, chunk in enumerate(chunk_payloads):
                    mock_redis.rpush(buffer_key, json.dumps({
                        "chunk_index": i, "data": chunk,
                    }))
                    await asyncio.sleep(0.005)
                mock_redis.rpush(buffer_key, json.dumps({
                    "__done__": True,
                    "summary": {
                        "total_chunks": num_chunks,
                        "producer_result": "stream_done",
                    },
                }))
            asyncio.create_task(produce())
            return {"submitted": True}

        elif step_name == "chunk_handler":
            chunk_data = payload.get("chunk", {})
            chunks_received.append(chunk_data)
            await asyncio.sleep(0.002)
            return {
                "processed": chunk_data.get("data"),
                "idx": chunk_data.get("chunk_index"),
            }

        elif step_name == "finalize":
            sr = payload.get("_streaming_results_chunk_handler", [])
            return {"aggregated_count": len(sr), "final": "ok"}

        else:
            await asyncio.sleep(0.005)
            return {"result": f"output_of_{step_name}"}

    engine = DagEngine(
        max_parallelism=4,
        redis=mock_redis,
        streaming_executor_max_workers=8,
    )

    print(f"\n🔄 STREAMING DAG 执行 (chunks={num_chunks}, buffer_key={buffer_key})")
    t0 = time.time()
    ctx = await engine.execute(
        dag_def, dispatcher, initial_context={"task_id": task_id}
    )
    elapsed = time.time() - t0

    num_chunks_recv = len(chunks_received)
    persist_streaming_result(dag_def.dag_id, ctx, num_chunks_recv, t0)
    row = verify_streaming_db(dag_def.dag_id)

    # ── 验证项 ──
    checks = {
        "DAG 状态=completed":                    ctx.status == DagStatus.COMPLETED,
        "stream_producer 步骤完成":               "stream_producer" in ctx.completed_steps,
        "finalize 步骤完成":                      "finalize" in ctx.completed_steps,
        f"收到 {num_chunks} 个 chunks":            num_chunks_recv == num_chunks,
        "_streaming_results 存在":                "_streaming_results_chunk_handler" in ctx.context,
        "streaming_results 条数正确":              len(ctx.context.get("_streaming_results_chunk_handler", [])) == num_chunks,
        "producer_result 写入 context":           ctx.context.get("producer_result") == "stream_done",
        "total_chunks 写入 context":              ctx.context.get("total_chunks") == num_chunks,
        "finalize aggregated_count 正确":         ctx.context.get("aggregated_count") == num_chunks,
        "DB 持久化正确":                           row.get("status") == "completed",
        "DB chunks 计数正确":                      row.get("chunks_received") == num_chunks,
    }

    all_ok = all(checks.values())
    icon = "✅" if all_ok else "❌"

    print(f"\n{icon} [STREAMING DAG] dag_id={dag_def.dag_id}")
    print(f"   steps_done          : {len(ctx.completed_steps)} → {ctx.completed_steps}")
    print(f"   chunks_received     : {num_chunks_recv}/{num_chunks}")
    print(f"   _streaming_results  : {len(ctx.context.get('_streaming_results_chunk_handler', []))} items")
    print(f"   producer_result     : {ctx.context.get('producer_result')}")
    print(f"   total_chunks        : {ctx.context.get('total_chunks')}")
    print(f"   finalize.aggregated : {ctx.context.get('aggregated_count')}")
    print(f"   status              : {ctx.status.value}")
    print(f"   elapsed             : {elapsed*1000:.1f}ms")
    print(f"\n   DB verify:")
    print(f"     status          = {row.get('status')}")
    print(f"     steps_done      = {row.get('steps_done')}")
    print(f"     chunks_received = {row.get('chunks_received')}")
    print(f"\n   逐项检查:")
    for desc, ok in checks.items():
        print(f"     {'✅' if ok else '❌'} {desc}")
    return all_ok


async def main():
    print("=" * 60)
    print("  STREAMING DAG 部署验证 (MariaDB 127.0.0.1:3307)")
    print("=" * 60)
    setup_db()
    ok = await run_streaming_verify(num_chunks=5)
    print("\n" + "=" * 60)
    if ok:
        print("  🎉 STREAMING DAG 部署验证通过！")
    else:
        print("  ❌ STREAMING DAG 验证失败，请检查上方日志")
    print("=" * 60)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
