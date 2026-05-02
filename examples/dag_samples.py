"""
Typical DAG samples for deployment testing.

Covers 5 real-world patterns:
1. Linear pipeline        (extract → transform → load)
2. Fan-out / Fan-in       (parallel processing with aggregation)
3. Conditional branching  (route based on runtime context value)
4. Retry with fallback    (node that fails N-1 times, then fallback)
5. ML training pipeline   (data prep → feature eng → train → evaluate → deploy)

Run with:
    python examples/dag_samples.py
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("dag_samples")


# ──────────────────────────────────────────────────────────────────────────────
# Shared handler registry
# ──────────────────────────────────────────────────────────────────────────────

_HANDLERS: dict[str, Any] = {}


def register(name: str):
    def decorator(fn):
        _HANDLERS[name] = fn
        return fn
    return decorator


async def dispatch(task_type: str, payload: dict[str, Any]) -> Any:
    """Route task_type to the registered async handler."""
    fn = _HANDLERS.get(task_type)
    if fn is None:
        raise ValueError(f"No handler registered for task_type={task_type!r}")
    return await fn(payload)


# ──────────────────────────────────────────────────────────────────────────────
# Sample 1: Linear ETL pipeline   extract → transform → load
# ──────────────────────────────────────────────────────────────────────────────

@register("extract")
async def handle_extract(payload: dict) -> dict:
    logger.info("[extract] reading source=%s", payload.get("source"))
    await asyncio.sleep(0.05)
    return {"rows": [{"id": i, "value": i * 2} for i in range(5)], "source": payload.get("source")}


@register("transform")
async def handle_transform(payload: dict) -> dict:
    rows = payload.get("rows", [])
    logger.info("[transform] processing %d rows", len(rows))
    await asyncio.sleep(0.03)
    transformed = [{"id": r["id"], "value": r["value"] ** 2} for r in rows]
    return {"rows": transformed, "count": len(transformed)}


@register("load")
async def handle_load(payload: dict) -> dict:
    rows = payload.get("rows", [])
    logger.info("[load] writing %d rows to destination", len(rows))
    await asyncio.sleep(0.02)
    return {"loaded": len(rows), "status": "ok"}


def make_etl_dag() -> "DAG":
    from async_scheduler.core.models import DAG, DAGNode
    extract = DAGNode(id="extract", name="Extract", task_type="extract",
                      payload={"source": "s3://raw-data/2026/"})
    transform = DAGNode(id="transform", name="Transform", task_type="transform",
                        payload={}, dependencies=["extract"])
    load = DAGNode(id="load", name="Load", task_type="load",
                   payload={}, dependencies=["transform"])
    return DAG(name="ETL Pipeline", nodes=[extract, transform, load])


# ──────────────────────────────────────────────────────────────────────────────
# Sample 2: Fan-out / Fan-in   ingest → [shard-0, shard-1, shard-2] → merge
# ──────────────────────────────────────────────────────────────────────────────

@register("ingest")
async def handle_ingest(payload: dict) -> dict:
    logger.info("[ingest] preparing %d shards", payload.get("shards", 3))
    await asyncio.sleep(0.02)
    return {"shard_count": payload.get("shards", 3), "data_id": "batch-42"}


@register("process_shard")
async def handle_shard(payload: dict) -> dict:
    shard = payload.get("shard_id", 0)
    logger.info("[shard-%d] processing", shard)
    await asyncio.sleep(0.05)
    return {f"shard_{shard}_result": shard * 100}


@register("merge")
async def handle_merge(payload: dict) -> dict:
    logger.info("[merge] aggregating shard results")
    await asyncio.sleep(0.02)
    return {"merged": True, "total": sum(v for k, v in payload.items() if "result" in k)}


def make_fanout_dag() -> "DAG":
    from async_scheduler.core.models import DAG, DAGNode
    ingest = DAGNode(id="ingest", name="Ingest", task_type="ingest",
                     payload={"shards": 3})
    shards = [
        DAGNode(id=f"shard-{i}", name=f"Shard {i}", task_type="process_shard",
                payload={"shard_id": i}, dependencies=["ingest"])
        for i in range(3)
    ]
    merge = DAGNode(id="merge", name="Merge", task_type="merge",
                    payload={}, dependencies=[f"shard-{i}" for i in range(3)])
    return DAG(name="Fan-out Fan-in", nodes=[ingest, *shards, merge], max_parallelism=3)


# ──────────────────────────────────────────────────────────────────────────────
# Sample 3: Conditional branching   validate → [fast_path | slow_path] → notify
# ──────────────────────────────────────────────────────────────────────────────

@register("validate")
async def handle_validate(payload: dict) -> dict:
    logger.info("[validate] checking input size=%s", payload.get("size"))
    await asyncio.sleep(0.01)
    return {"size": payload.get("size", 0), "is_small": payload.get("size", 0) < 100}


@register("fast_path")
async def handle_fast(payload: dict) -> dict:
    logger.info("[fast_path] executing")
    await asyncio.sleep(0.02)
    return {"path": "fast", "result": "ok"}


@register("slow_path")
async def handle_slow(payload: dict) -> dict:
    logger.info("[slow_path] executing (large input)")
    await asyncio.sleep(0.08)
    return {"path": "slow", "result": "ok"}


@register("notify")
async def handle_notify(payload: dict) -> dict:
    path = payload.get("path", "unknown")
    logger.info("[notify] sending completion via path=%s", path)
    await asyncio.sleep(0.01)
    return {"notified": True, "path": path}


def make_conditional_dag(small_input: bool = True) -> "DAG":
    from async_scheduler.core.models import DAG, DAGNode
    validate = DAGNode(id="validate", name="Validate", task_type="validate",
                       payload={"size": 50 if small_input else 200})
    fast = DAGNode(id="fast_path", name="Fast Path", task_type="fast_path",
                   payload={}, dependencies=["validate"],
                   condition="context.get('is_small', False)")
    slow = DAGNode(id="slow_path", name="Slow Path", task_type="slow_path",
                   payload={}, dependencies=["validate"],
                   condition="not context.get('is_small', False)")
    notify = DAGNode(id="notify", name="Notify", task_type="notify",
                     payload={}, dependencies=["fast_path", "slow_path"],
                     on_failure="skip")
    return DAG(name="Conditional Branch", nodes=[validate, fast, slow, notify],
               context={})


# ──────────────────────────────────────────────────────────────────────────────
# Sample 4: Retry with fallback
# ──────────────────────────────────────────────────────────────────────────────

_flaky_call_count: dict[str, int] = {}


@register("flaky_api")
async def handle_flaky(payload: dict) -> dict:
    key = payload.get("request_id", "default")
    _flaky_call_count[key] = _flaky_call_count.get(key, 0) + 1
    attempt = _flaky_call_count[key]
    logger.info("[flaky_api] attempt=%d request_id=%s", attempt, key)
    if attempt < 3:
        raise RuntimeError(f"Transient failure (attempt {attempt})")
    return {"api_result": "success", "attempts_taken": attempt}


@register("fallback_api")
async def handle_fallback(payload: dict) -> dict:
    logger.info("[fallback_api] using cached fallback result")
    await asyncio.sleep(0.01)
    return {"api_result": "fallback", "source": "cache"}


@register("process_result")
async def handle_process_result(payload: dict) -> dict:
    result = payload.get("api_result", "unknown")
    logger.info("[process_result] handling api_result=%s", result)
    return {"processed": True, "final_result": result}


def make_retry_fallback_dag() -> "DAG":
    from async_scheduler.core.models import DAG, DAGNode
    _flaky_call_count.clear()
    flaky = DAGNode(id="flaky", name="Flaky API", task_type="flaky_api",
                    payload={"request_id": "req-001"},
                    max_retries=3,
                    on_failure="fallback",
                    fallback_payload={"api_result": "fallback", "source": "fallback_payload"})
    process = DAGNode(id="process", name="Process", task_type="process_result",
                      payload={}, dependencies=["flaky"])
    return DAG(name="Retry with Fallback", nodes=[flaky, process])


# ──────────────────────────────────────────────────────────────────────────────
# Sample 5: ML Training Pipeline
# data_prep → feature_eng → [train_model, compute_baseline] → evaluate → deploy
# ──────────────────────────────────────────────────────────────────────────────

@register("data_prep")
async def handle_data_prep(payload: dict) -> dict:
    logger.info("[data_prep] preparing dataset=%s", payload.get("dataset"))
    await asyncio.sleep(0.04)
    return {"dataset_size": 10000, "features": ["f1", "f2", "f3"], "split": 0.8}


@register("feature_eng")
async def handle_feature_eng(payload: dict) -> dict:
    features = payload.get("features", [])
    logger.info("[feature_eng] engineering %d features", len(features))
    await asyncio.sleep(0.03)
    return {"feature_matrix_shape": [10000, len(features) * 2], "features_engineered": True}


@register("train_model")
async def handle_train(payload: dict) -> dict:
    logger.info("[train_model] training on shape=%s", payload.get("feature_matrix_shape"))
    await asyncio.sleep(0.1)
    return {"model_id": "model-v1", "train_accuracy": 0.94, "val_accuracy": 0.91}


@register("compute_baseline")
async def handle_baseline(payload: dict) -> dict:
    logger.info("[compute_baseline] computing baseline metrics")
    await asyncio.sleep(0.04)
    return {"baseline_accuracy": 0.72, "baseline_f1": 0.68}


@register("evaluate")
async def handle_evaluate(payload: dict) -> dict:
    model_acc = payload.get("val_accuracy", 0)
    baseline_acc = payload.get("baseline_accuracy", 0)
    improvement = model_acc - baseline_acc
    logger.info("[evaluate] val_accuracy=%.3f baseline=%.3f improvement=%.3f",
                model_acc, baseline_acc, improvement)
    await asyncio.sleep(0.02)
    passed = improvement > 0.1
    return {"passed": passed, "improvement": round(improvement, 3), "model_id": payload.get("model_id")}


@register("deploy")
async def handle_deploy(payload: dict) -> dict:
    passed = payload.get("passed", False)
    model_id = payload.get("model_id", "unknown")
    logger.info("[deploy] model_id=%s passed=%s", model_id, passed)
    await asyncio.sleep(0.02)
    if not passed:
        raise RuntimeError(f"Model {model_id} did not pass evaluation threshold")
    return {"deployed": True, "model_id": model_id, "endpoint": f"/models/{model_id}"}


def make_ml_pipeline_dag() -> "DAG":
    from async_scheduler.core.models import DAG, DAGNode
    data_prep = DAGNode(id="data_prep", name="Data Prep", task_type="data_prep",
                        payload={"dataset": "imagenet-subset-2026"})
    feature_eng = DAGNode(id="feature_eng", name="Feature Engineering", task_type="feature_eng",
                          payload={}, dependencies=["data_prep"])
    train = DAGNode(id="train", name="Train Model", task_type="train_model",
                    payload={}, dependencies=["feature_eng"])
    baseline = DAGNode(id="baseline", name="Compute Baseline", task_type="compute_baseline",
                       payload={}, dependencies=["feature_eng"])
    evaluate = DAGNode(id="evaluate", name="Evaluate", task_type="evaluate",
                       payload={}, dependencies=["train", "baseline"])
    deploy = DAGNode(id="deploy", name="Deploy", task_type="deploy",
                     payload={}, dependencies=["evaluate"],
                     condition="context.get('passed', False)")
    return DAG(name="ML Training Pipeline",
               nodes=[data_prep, feature_eng, train, baseline, evaluate, deploy],
               max_parallelism=2)


# ──────────────────────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────────────────────

async def run_dag(name: str, dag, verbose: bool = True) -> dict:
    """Execute a DAG and return a result summary."""
    from async_scheduler.dag.engine import DAGEngine

    engine = DAGEngine()
    t0 = time.perf_counter()
    result = await engine.execute(dag, dispatch)
    elapsed = time.perf_counter() - t0

    summary = {
        "name": name,
        "status": result.status.value,
        "elapsed_s": round(elapsed, 3),
        "nodes": {},
    }
    for node in dag.nodes:
        ex = result.node_executions.get(node.id)
        summary["nodes"][node.id] = {
            "status": ex.status.value if ex else "missing",
            "skipped": ex.skipped if ex else False,
            "error": ex.error_message if ex else None,
            "result": ex.result if ex else None,
        }

    status_icon = "✅" if result.status.value == "success" else (
        "⚠️" if result.status.value == "partial" else "❌"
    )
    logger.info("%s %s [%s] %.3fs", status_icon, name, result.status.value, elapsed)
    if verbose:
        for nid, info in summary["nodes"].items():
            icon = "✓" if info["status"] == "success" else ("↷" if info["skipped"] else "✗")
            logger.info("  %s %-20s %s%s", icon, nid, info["status"],
                        f" (skipped: {info['error']})" if info["skipped"] else
                        (f" ERROR: {info['error']}" if info["error"] else ""))
    return summary


async def main():
    print("\n" + "=" * 60)
    print("  DAG Sample Deployment Tests")
    print("=" * 60 + "\n")

    results = []

    # 1. ETL
    results.append(await run_dag("ETL Pipeline", make_etl_dag()))

    # 2. Fan-out
    results.append(await run_dag("Fan-out / Fan-in", make_fanout_dag()))

    # 3. Conditional (small)
    results.append(await run_dag("Conditional Branch (small)", make_conditional_dag(small_input=True)))

    # 4. Conditional (large)
    results.append(await run_dag("Conditional Branch (large)", make_conditional_dag(small_input=False)))

    # 5. Retry with fallback
    results.append(await run_dag("Retry with Fallback", make_retry_fallback_dag()))

    # 6. ML Pipeline
    results.append(await run_dag("ML Training Pipeline", make_ml_pipeline_dag()))

    print("\n" + "=" * 60)
    print("  Summary")
    print("=" * 60)
    for r in results:
        icon = "✅" if r["status"] == "success" else "❌"
        print(f"  {icon} {r['name']:40s} {r['status']:10s} {r['elapsed_s']:.3f}s")
    print()

    failures = [r for r in results if r["status"] != "success"]
    if failures:
        print(f"  ⚠️  {len(failures)} DAG(s) did not complete successfully:")
        for f in failures:
            print(f"    - {f['name']}: {f['status']}")
            for nid, info in f["nodes"].items():
                if info["status"] not in ("success",) and not info["skipped"]:
                    print(f"      node {nid}: {info['status']} {info['error'] or ''}")
    else:
        print("  All DAGs completed successfully!")
    print()
    return results


if __name__ == "__main__":
    asyncio.run(main())
