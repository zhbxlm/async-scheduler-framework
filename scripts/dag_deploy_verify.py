"""
DAG 类型构建 + 部署验证脚本
- SQL 接口: 127.0.0.1:3307
- 覆盖 4 类 DAG: 线性 / 并行扇出 / 条件分支 / Map-Reduce
- 验证: 建库建表 → 执行 DAG → 写入结果 → 读回校验
"""
from __future__ import annotations

import asyncio
import json
import time
import sys
from datetime import datetime
from typing import Any

import pymysql
from sqlalchemy import text

sys.path.insert(0, ".")

from src.common.db import init_engine, get_db
from src.models.dag import (
    DagDefinition, DagStep, RetryPolicy, ExecutionMode, OnFailureAction, StepKind, DagStatus,
)
from src.platform.dag_engine import DagEngine

# ─────────────────────────────────────────────
# DB 初始化 (3307)
# ─────────────────────────────────────────────
DB_URL = "mysql+pymysql://root@127.0.0.1:3307/ray_async_test?charset=utf8mb4"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dag_execution_log (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    dag_id      VARCHAR(128) NOT NULL,
    dag_type    VARCHAR(64)  NOT NULL,
    status      VARCHAR(32)  NOT NULL,
    steps_done  INT          NOT NULL DEFAULT 0,
    output_json TEXT,
    started_at  DATETIME     NOT NULL,
    finished_at DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

def setup_db():
    init_engine(DB_URL, echo=False, pool_pre_ping=True)
    with get_db() as db:
        db.execute(text(SCHEMA_SQL))
        db.commit()
    print("✅ DB schema ready (port 3307)")


def persist_result(dag_type: str, dag_id: str, ctx, started: float):
    with get_db() as db:
        db.execute(text("""
            INSERT INTO dag_execution_log
                (dag_id, dag_type, status, steps_done, output_json, started_at, finished_at)
            VALUES
                (:dag_id, :dag_type, :status, :steps_done, :output_json, :started_at, :finished_at)
        """), {
            "dag_id": dag_id,
            "dag_type": dag_type,
            "status": ctx.status.value,
            "steps_done": len(ctx.completed_steps),
            "output_json": json.dumps(ctx.context, ensure_ascii=False),
            "started_at": datetime.fromtimestamp(started),
            "finished_at": datetime.now(),
        })
        db.commit()


def verify_persisted(dag_id: str) -> dict:
    with get_db() as db:
        row = db.execute(
            text("SELECT dag_type, status, steps_done, output_json FROM dag_execution_log WHERE dag_id=:d ORDER BY id DESC LIMIT 1"),
            {"d": dag_id}
        ).fetchone()
    return dict(row._mapping) if row else {}


# ─────────────────────────────────────────────
# 通用 mock dispatcher
# ─────────────────────────────────────────────
CALL_LOG: list[dict] = []

async def mock_dispatch(capability: str, step_name: str, payload: dict) -> dict:
    await asyncio.sleep(0.02)   # 模拟网络延迟
    CALL_LOG.append({"cap": capability, "step": step_name})
    return {
        "result": f"output_of_{step_name}",
        "value": len(payload),
        "ts": time.time(),
    }


# ═══════════════════════════════════════════════
# DAG 类型 1: 线性 (LINEAR)
# A → B → C
# ═══════════════════════════════════════════════
def build_linear_dag() -> DagDefinition:
    return DagDefinition(
        dag_id="dag_linear_001",
        tenant_id="tenant_test",
        steps=[
            DagStep(
                step_name="A",
                capability="cap_preprocess",
                kind=StepKind.TASK,
                execution_mode=ExecutionMode.SYNC,
                depends_on=[],
                input_mapping={},
                output_mapping={"a_out": "result"},
                retry_policy=RetryPolicy(max_retries=1),
                timeout_seconds=10,
            ),
            DagStep(
                step_name="B",
                capability="cap_transform",
                kind=StepKind.TASK,
                execution_mode=ExecutionMode.SYNC,
                depends_on=["A"],
                input_mapping={"prev": "a_out"},
                output_mapping={"b_out": "result"},
                retry_policy=RetryPolicy(max_retries=0),
                timeout_seconds=10,
            ),
            DagStep(
                step_name="C",
                capability="cap_postprocess",
                kind=StepKind.TASK,
                execution_mode=ExecutionMode.SYNC,
                depends_on=["B"],
                input_mapping={"prev": "b_out"},
                output_mapping={"final": "result"},
                retry_policy=RetryPolicy(max_retries=0),
                timeout_seconds=10,
            ),
        ],
        max_concurrent=1,
    )


# ═══════════════════════════════════════════════
# DAG 类型 2: 并行扇出 (PARALLEL)
# A → [B1, B2, B3] → C
# ═══════════════════════════════════════════════
def build_parallel_dag() -> DagDefinition:
    return DagDefinition(
        dag_id="dag_parallel_001",
        tenant_id="tenant_test",
        steps=[
            DagStep(
                step_name="A",
                capability="cap_split",
                kind=StepKind.TASK,
                execution_mode=ExecutionMode.SYNC,
                depends_on=[],
                input_mapping={},
                output_mapping={"split_out": "result"},
                retry_policy=RetryPolicy(max_retries=0),
                timeout_seconds=10,
            ),
            DagStep(
                step_name="B1",
                capability="cap_worker",
                kind=StepKind.TASK,
                execution_mode=ExecutionMode.SYNC,
                depends_on=["A"],
                input_mapping={"data": "split_out"},
                output_mapping={"b1_out": "result"},
                retry_policy=RetryPolicy(max_retries=0),
                timeout_seconds=10,
            ),
            DagStep(
                step_name="B2",
                capability="cap_worker",
                kind=StepKind.TASK,
                execution_mode=ExecutionMode.SYNC,
                depends_on=["A"],
                input_mapping={"data": "split_out"},
                output_mapping={"b2_out": "result"},
                retry_policy=RetryPolicy(max_retries=0),
                timeout_seconds=10,
            ),
            DagStep(
                step_name="B3",
                capability="cap_worker",
                kind=StepKind.TASK,
                execution_mode=ExecutionMode.SYNC,
                depends_on=["A"],
                input_mapping={"data": "split_out"},
                output_mapping={"b3_out": "result"},
                retry_policy=RetryPolicy(max_retries=0),
                timeout_seconds=10,
            ),
            DagStep(
                step_name="C",
                capability="cap_merge",
                kind=StepKind.COLLECTOR,
                execution_mode=ExecutionMode.SYNC,
                depends_on=["B1", "B2", "B3"],
                input_mapping={"r1": "b1_out", "r2": "b2_out", "r3": "b3_out"},
                output_mapping={"merged": "result"},
                retry_policy=RetryPolicy(max_retries=0),
                timeout_seconds=10,
            ),
        ],
        max_concurrent=4,
    )


# ═══════════════════════════════════════════════
# DAG 类型 3: 条件分支 (CONDITIONAL)
# A → [B_yes | B_no] → C
# ═══════════════════════════════════════════════
def build_conditional_dag(flag: bool = True) -> DagDefinition:
    cond_yes = "flag == True"
    cond_no  = "flag == False"
    return DagDefinition(
        dag_id=f"dag_cond_{'yes' if flag else 'no'}_001",
        tenant_id="tenant_test",
        steps=[
            DagStep(
                step_name="A",
                capability="cap_router",
                kind=StepKind.DISPATCH,
                execution_mode=ExecutionMode.SYNC,
                depends_on=[],
                input_mapping={},
                output_mapping={"route_out": "result"},
                retry_policy=RetryPolicy(max_retries=0),
                timeout_seconds=10,
            ),
            DagStep(
                step_name="B_yes",
                capability="cap_branch_yes",
                kind=StepKind.TASK,
                execution_mode=ExecutionMode.SYNC,
                depends_on=["A"],
                condition=cond_yes,
                input_mapping={},
                output_mapping={"branch_out": "result"},
                retry_policy=RetryPolicy(max_retries=0),
                timeout_seconds=10,
            ),
            DagStep(
                step_name="B_no",
                capability="cap_branch_no",
                kind=StepKind.TASK,
                execution_mode=ExecutionMode.SYNC,
                depends_on=["A"],
                condition=cond_no,
                input_mapping={},
                output_mapping={"branch_out": "result"},
                retry_policy=RetryPolicy(max_retries=0),
                timeout_seconds=10,
            ),
            DagStep(
                step_name="C",
                capability="cap_finalize",
                kind=StepKind.TASK,
                execution_mode=ExecutionMode.SYNC,
                depends_on=["B_yes", "B_no"],
                input_mapping={"in": "branch_out"},
                output_mapping={"final": "result"},
                on_failure=OnFailureAction.SKIP,
                retry_policy=RetryPolicy(max_retries=0),
                timeout_seconds=10,
            ),
        ],
        max_concurrent=2,
    )


# ═══════════════════════════════════════════════
# DAG 类型 4: On-Failure Fallback (FAULT-TOLERANT)
# A → B(会失败) → C (B 失败则 fallback)
# ═══════════════════════════════════════════════
from src.models.dag import FallbackConfig

async def failing_dispatch(capability: str, step_name: str, payload: dict) -> dict:
    if step_name == "B_fail":
        raise RuntimeError("Simulated step failure!")
    return await mock_dispatch(capability, step_name, payload)

def build_fault_tolerant_dag() -> DagDefinition:
    return DagDefinition(
        dag_id="dag_fault_tolerant_001",
        tenant_id="tenant_test",
        steps=[
            DagStep(
                step_name="A",
                capability="cap_start",
                kind=StepKind.TASK,
                execution_mode=ExecutionMode.SYNC,
                depends_on=[],
                input_mapping={},
                output_mapping={"a_out": "result"},
                retry_policy=RetryPolicy(max_retries=0),
                timeout_seconds=10,
            ),
            DagStep(
                step_name="B_fail",
                capability="cap_unstable",
                kind=StepKind.TASK,
                execution_mode=ExecutionMode.SYNC,
                depends_on=["A"],
                input_mapping={},
                output_mapping={"b_out": "result"},
                on_failure=OnFailureAction.FALLBACK,
                fallback=FallbackConfig(output_mapping={"result": "fallback_value"}),
                retry_policy=RetryPolicy(max_retries=1, retry_delay_seconds=0.01),
                timeout_seconds=10,
            ),
            DagStep(
                step_name="C",
                capability="cap_end",
                kind=StepKind.TASK,
                execution_mode=ExecutionMode.SYNC,
                depends_on=["B_fail"],
                input_mapping={"prev": "b_out"},
                output_mapping={"final": "result"},
                retry_policy=RetryPolicy(max_retries=0),
                timeout_seconds=10,
            ),
        ],
        max_concurrent=2,
    )


# ─────────────────────────────────────────────
# 运行 + 验证
# ─────────────────────────────────────────────
async def run_and_verify(label: str, dag_def: DagDefinition, dispatcher=mock_dispatch, initial_ctx: dict | None = None):
    engine = DagEngine(max_parallelism=4)
    CALL_LOG.clear()
    t0 = time.time()

    ctx = await engine.execute(dag_def, dispatcher, initial_context=initial_ctx or {})
    elapsed = time.time() - t0

    # 持久化到 MySQL
    persist_result(label, dag_def.dag_id, ctx, t0)

    # 从 MySQL 读回校验
    row = verify_persisted(dag_def.dag_id)

    ok = (
        ctx.status == DagStatus.COMPLETED
        and row.get("status") == "completed"
        and row.get("steps_done", 0) > 0
    )

    icon = "✅" if ok else "❌"
    print(f"\n{icon} [{label}] dag_id={dag_def.dag_id}")
    print(f"   steps_done   : {len(ctx.completed_steps)} → {ctx.completed_steps}")
    print(f"   context_keys : {list(ctx.context.keys())}")
    print(f"   status       : {ctx.status.value}")
    print(f"   elapsed      : {elapsed*1000:.1f}ms")
    print(f"   DB verify    : steps_done={row.get('steps_done')} status={row.get('status')}")
    return ok


async def main():
    print("=" * 60)
    print("  DAG 类型部署验证 (MariaDB 127.0.0.1:3307)")
    print("=" * 60)

    setup_db()

    results = {}

    # 1. 线性 DAG
    results["linear"] = await run_and_verify(
        "LINEAR (A→B→C)",
        build_linear_dag(),
    )

    # 2. 并行扇出
    results["parallel"] = await run_and_verify(
        "PARALLEL (A→[B1,B2,B3]→C)",
        build_parallel_dag(),
    )

    # 3. 条件分支 (flag=True → B_yes 执行, B_no 跳过)
    results["cond_yes"] = await run_and_verify(
        "CONDITIONAL (flag=True)",
        build_conditional_dag(flag=True),
        initial_ctx={"flag": True},
    )

    # 4. 容错/Fallback
    results["fault_tolerant"] = await run_and_verify(
        "FAULT-TOLERANT (B fails→fallback)",
        build_fault_tolerant_dag(),
        dispatcher=failing_dispatch,
    )

    # ── 最终汇总 ──
    print("\n" + "=" * 60)
    print("  部署验证汇总")
    print("=" * 60)
    all_ok = True
    for name, ok in results.items():
        icon = "✅" if ok else "❌"
        print(f"  {icon} {name}")
        if not ok:
            all_ok = False

    # 读取 DB 全部记录
    with get_db() as db:
        rows = db.execute(text(
            "SELECT dag_type, dag_id, status, steps_done, started_at "
            "FROM dag_execution_log ORDER BY id DESC LIMIT 10"
        )).fetchall()
    print(f"\n  MariaDB dag_execution_log 最新 {len(rows)} 条记录:")
    for r in rows:
        d = dict(r._mapping)
        print(f"    [{d['status']}] {d['dag_type']} steps={d['steps_done']}")

    print()
    if all_ok:
        print("  🎉 全部 4 类 DAG 部署验证通过！")
    else:
        print("  ⚠️  部分验证失败，请检查上方日志")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
