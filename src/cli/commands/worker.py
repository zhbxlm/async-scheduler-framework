# rebuilt from deepwiki-reference alignment
"""worker commands — kubectl-style CLI for /ops/v1/worker."""
from __future__ import annotations
import json
import click


def _print(data):
    click.echo(json.dumps(data, ensure_ascii=False, indent=2, default=str))


@click.group()
def worker():
    """Manage workers."""
    pass


@worker.command("list")
@click.pass_context
def list_cmd(ctx):
    """List all workers."""
    client = ctx.obj["client"]
    _print(client.get("/ops/v1/worker"))


@worker.command("get")
@click.argument("item_id")
@click.pass_context
def get_cmd(ctx, item_id: str):
    """Get worker details."""
    client = ctx.obj["client"]
    _print(client.get(f"/ops/v1/worker/{item_id}"))


@worker.command("create")
@click.option("--data", default="{}", help="JSON payload")
@click.pass_context
def create_cmd(ctx, data: str):
    """Create a new worker."""
    client = ctx.obj["client"]
    _print(client.post("/ops/v1/worker", json=json.loads(data)))


@worker.command("remove")
@click.argument("item_id")
@click.pass_context
def remove_cmd(ctx, item_id: str):
    """Remove a worker."""
    client = ctx.obj["client"]
    client.delete(f"/ops/v1/worker/{item_id}")
    click.echo(f"Removed {item_id}")
