"""scheduler_sdk.cli — command-line wrapper for SchedulerClient."""
from __future__ import annotations

import asyncio
import json
import sys

import click


@click.group()
@click.option("--url", default="http://localhost:8000", envvar="SCHEDULER_URL", help="API base URL")
@click.option("--api-key", default="", envvar="SCHEDULER_API_KEY", help="API key")
@click.pass_context
def cli(ctx: click.Context, url: str, api_key: str) -> None:
    """async-scheduler CLI — submit and manage tasks."""
    ctx.ensure_object(dict)
    ctx.obj["url"] = url
    ctx.obj["api_key"] = api_key


def _client(ctx: click.Context):
    from scheduler_sdk.client import SchedulerClient
    return SchedulerClient(ctx.obj["url"], api_key=ctx.obj["api_key"])


@cli.command()
@click.argument("dag_id")
@click.option("--input", "input_json", default="{}", help="Input data as JSON string")
@click.option("--priority", default="normal", type=click.Choice(["low", "normal", "high", "critical"]))
@click.option("--wait", is_flag=True, help="Wait for completion")
@click.pass_context
def submit(ctx: click.Context, dag_id: str, input_json: str, priority: str, wait: bool) -> None:
    """Submit a task."""
    input_data = json.loads(input_json)

    async def _run() -> None:
        async with _client(ctx) as c:
            if wait:
                result = await c.submit_and_wait(dag_id, input_data, priority=priority)
                click.echo(json.dumps(result, indent=2))
            else:
                task = await c.submit_task(dag_id, input_data, priority=priority)
                click.echo(json.dumps(task, indent=2))

    asyncio.run(_run())


@cli.command()
@click.argument("task_id")
@click.pass_context
def get(ctx: click.Context, task_id: str) -> None:
    """Get task status."""
    async def _run() -> None:
        async with _client(ctx) as c:
            task = await c.get_task(task_id)
            click.echo(json.dumps(task, indent=2))

    asyncio.run(_run())


@cli.command()
@click.argument("task_id")
@click.option("--timeout", default=300.0, type=float)
@click.pass_context
def wait(ctx: click.Context, task_id: str, timeout: float) -> None:
    """Wait for a task to complete."""
    async def _run() -> None:
        async with _client(ctx) as c:
            result = await c.wait_for_task(task_id, timeout=timeout)
            click.echo(json.dumps(result, indent=2))

    asyncio.run(_run())


@cli.command()
@click.argument("task_id")
@click.pass_context
def cancel(ctx: click.Context, task_id: str) -> None:
    """Cancel a task."""
    async def _run() -> None:
        async with _client(ctx) as c:
            result = await c.cancel_task(task_id)
            click.echo(json.dumps(result, indent=2))

    asyncio.run(_run())


@cli.command()
@click.pass_context
def health(ctx: click.Context) -> None:
    """Check API health."""
    async def _run() -> None:
        async with _client(ctx) as c:
            result = await c.health()
            click.echo(json.dumps(result, indent=2))

    asyncio.run(_run())


def main() -> None:
    cli(obj={})


if __name__ == "__main__":
    main()
