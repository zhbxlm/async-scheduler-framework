# rebuilt from deepwiki-reference alignment
"""queue commands — kubectl-style CLI for /ops/v1/queue."""
from __future__ import annotations
import json
import click


def _print(data):
    click.echo(json.dumps(data, ensure_ascii=False, indent=2, default=str))


@click.group()
def queue():
    """Manage queues."""
    pass


@queue.command("list")
@click.pass_context
def list_cmd(ctx):
    """List all queues."""
    client = ctx.obj["client"]
    _print(client.get("/ops/v1/queue"))


@queue.command("get")
@click.argument("item_id")
@click.pass_context
def get_cmd(ctx, item_id: str):
    """Get queue details."""
    client = ctx.obj["client"]
    _print(client.get(f"/ops/v1/queue/{item_id}"))


@queue.command("create")
@click.option("--data", default="{}", help="JSON payload")
@click.pass_context
def create_cmd(ctx, data: str):
    """Create a new queue."""
    client = ctx.obj["client"]
    _print(client.post("/ops/v1/queue", json=json.loads(data)))


@queue.command("remove")
@click.argument("item_id")
@click.pass_context
def remove_cmd(ctx, item_id: str):
    """Remove a queue."""
    client = ctx.obj["client"]
    client.delete(f"/ops/v1/queue/{item_id}")
    click.echo(f"Removed {item_id}")
