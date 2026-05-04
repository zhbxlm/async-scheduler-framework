# rebuilt from deepwiki-reference alignment
"""node commands — kubectl-style CLI for /ops/v1/nodes."""
from __future__ import annotations
import json
import click


def _print(data):
    click.echo(json.dumps(data, ensure_ascii=False, indent=2, default=str))


@click.group()
def node():
    """Manage nodes."""
    pass


@node.command("list")
@click.pass_context
def list_cmd(ctx):
    """List all nodes."""
    client = ctx.obj["client"]
    _print(client.get("/ops/v1/nodes"))


@node.command("get")
@click.argument("item_id")
@click.pass_context
def get_cmd(ctx, item_id: str):
    """Get node details."""
    client = ctx.obj["client"]
    _print(client.get(f"/ops/v1/nodes/{item_id}"))


@node.command("create")
@click.option("--data", default="{}", help="JSON payload")
@click.pass_context
def create_cmd(ctx, data: str):
    """Create a new node."""
    client = ctx.obj["client"]
    _print(client.post("/ops/v1/nodes", json=json.loads(data)))


@node.command("remove")
@click.argument("item_id")
@click.pass_context
def remove_cmd(ctx, item_id: str):
    """Remove a node."""
    client = ctx.obj["client"]
    client.delete(f"/ops/v1/nodes/{item_id}")
    click.echo(f"Removed {item_id}")
