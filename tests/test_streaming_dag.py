"""Streaming DAG integration test: A -> B(streaming token-by-token) -> C."""
from __future__ import annotations

import asyncio
import json

import fakeredis.aioredis
import pytest


class TestStreamingDag:
    """End-to-end streaming DAG pipeline tests.

    Pattern:
        A: fetch/prepare input
        B: streaming LLM node – generates tokens, accumulates sentences,
           pushes to Redis buffer
        C: downstream consumer – polls Redis, processes each sentence
    """

    @pytest.mark.asyncio
    async def test_a_b_streaming_c(self):
        """A -> B(streaming) -> C: sentences flow through Redis buffer."""
        from src.platform.dag_engine import DagEngine, DagStep
        from src.models.dag import DagDefinition, StreamingTrigger, StepKind

        task_id = "stream_abc"
        sentences = ["hello world"]
        chunks = []
        tokens_generated = []
        buf = []

        r = fakeredis.aioredis.FakeRedis(decode_responses=False)
        async def mock_blpop(keys, timeout=0):
            while not buf:
                await asyncio.sleep(0.005)
            val = buf.pop(0)
            k = keys[0] if isinstance(keys, list) else keys
            return (k, val)
        r.async_blpop = mock_blpop

        async def dispatch(capability, step_name, input_data):
            if step_name == "A":
                return {"ok": True, "sentences": sentences}
            elif step_name == "B":
                s = sentences[0]
                acc = ""
                for ch in s:
                    acc += ch
                    tokens_generated.append(acc)
                buf.append(json.dumps({"text": acc, "len": len(acc)}).encode())
                buf.append(json.dumps({"__done__": True, "summary": {"count": 1}}).encode())
                return {"ok": True}
            elif step_name == "C":
                chunk = input_data.get("chunk", {})
                if chunk:
                    chunks.append(chunk.get("text", ""))
                return {"ok": True}
            return {"ok": True}

        dag = DagDefinition(dag_id="stream_abc", steps=[
            DagStep(step_name="A", capability="http"),
            DagStep(step_name="B", capability="http",
                    step_kind=StepKind.STREAMING, depends_on=["A"],
                    streaming_trigger=StreamingTrigger(
                        buffer_key="stream:{task_id}",
                        flush_on_complete=True,
                        downstream_steps=["C"])),
            DagStep(step_name="C", capability="http", depends_on=["B"]),
        ])

        engine = DagEngine(context_store=None, redis=r)
        ctx = await engine.execute(dag, dispatch, {"task_id": task_id})

        assert ctx.status.value == "completed", f"got {ctx.status.value}"
        assert len(chunks) == len(sentences)
        assert len(tokens_generated) == sum(len(s) for s in sentences)

    @pytest.mark.asyncio
    async def test_streaming_multiple_sentences(self):
        """B streams multiple sentences, C receives them all."""
        from src.platform.dag_engine import DagEngine, DagStep
        from src.models.dag import DagDefinition, StreamingTrigger, StepKind

        task_id = "stream_multi"
        sentences = ["hello world", "foo bar"]
        chunks = []
        buf = []

        r = fakeredis.aioredis.FakeRedis(decode_responses=False)
        async def mock_blpop(keys, timeout=0):
            while not buf:
                await asyncio.sleep(0.005)
            val = buf.pop(0)
            k = keys[0] if isinstance(keys, list) else keys
            return (k, val)
        r.async_blpop = mock_blpop

        async def dispatch(capability, step_name, input_data):
            if step_name == "A":
                return {"ok": True, "sentences": sentences}
            elif step_name == "B":
                for s in sentences:
                    buf.append(json.dumps({"text": s, "len": len(s)}).encode())
                buf.append(json.dumps({"__done__": True, "summary": {"count": len(sentences)}}).encode())
                return {"ok": True}
            elif step_name == "C":
                chunk = input_data.get("chunk", {})
                if chunk:
                    chunks.append(chunk.get("text", ""))
                return {"ok": True}
            return {"ok": True}

        dag = DagDefinition(dag_id="stream_multi", steps=[
            DagStep(step_name="A", capability="http"),
            DagStep(step_name="B", capability="http",
                    step_kind=StepKind.STREAMING, depends_on=["A"],
                    streaming_trigger=StreamingTrigger(
                        buffer_key="stream_multi:{task_id}",
                        flush_on_complete=True,
                        downstream_steps=["C"])),
            DagStep(step_name="C", capability="http", depends_on=["B"]),
        ])

        engine = DagEngine(context_store=None, redis=r)
        ctx = await engine.execute(dag, dispatch, {"task_id": task_id})

        assert ctx.status.value == "completed", f"got {ctx.status.value}"
        assert len(chunks) == len(sentences)

    @pytest.mark.asyncio
    async def test_streaming_with_skip_condition(self):
        """Conditional skip still works with streaming adjacent steps."""
        from src.platform.dag_engine import DagEngine, DagStep
        from src.models.dag import DagDefinition, StreamingTrigger, StepKind

        task_id = "stream_skip"
        chunks = []
        buf = []
        r = fakeredis.aioredis.FakeRedis(decode_responses=False)

        async def mock_blpop(keys, timeout=0):
            while not buf:
                await asyncio.sleep(0.005)
            val = buf.pop(0)
            k = keys[0] if isinstance(keys, list) else keys
            return (k, val)
        r.async_blpop = mock_blpop

        calls = []
        async def dispatch(capability, step_name, input_data):
            calls.append(step_name)
            if step_name == "A":
                return {"ok": True, "sentences": ["hi"]}
            elif step_name == "B":
                buf.append(json.dumps({"text": "hi", "len": 2}).encode())
                buf.append(json.dumps({"__done__": True, "summary": {"count": 1}}).encode())
                return {"ok": True}
            elif step_name == "C":
                chunk = input_data.get("chunk", {})
                if chunk:
                    chunks.append(chunk.get("text", ""))
                return {"ok": True}
            return {"ok": True}

        dag = DagDefinition(dag_id="stream_skip", steps=[
            DagStep(step_name="A", capability="http"),
            DagStep(step_name="B", capability="http",
                    step_kind=StepKind.STREAMING, depends_on=["A"],
                    streaming_trigger=StreamingTrigger(
                        buffer_key="stream_skip:{task_id}",
                        flush_on_complete=True,
                        downstream_steps=["C"])),
            DagStep(step_name="C", capability="http", depends_on=["B"]),
            DagStep(step_name="D", capability="http", depends_on=["C"], condition="False"),
        ])

        engine = DagEngine(context_store=None, redis=r)
        ctx = await engine.execute(dag, dispatch, {"task_id": task_id})

        assert ctx.status.value == "completed"
        assert len(chunks) == 1
        assert "D" not in calls
