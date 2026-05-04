# rebuilt from deepwiki-reference alignment
"""cluster commands — kubectl-style CLI for /ops/v1/clusters."""
from __future__ import annotations
import json
import click


def _print(data):
    click.echo(json.dumps(data, ensure_ascii=False, indent=2, default=str))


@click.group()
def cluster():
    """Manage clusters."""
    pass


@cluster.command("list")
@click.pass_context
def list_cmd(ctx):
    """List all clusters."""
    client = ctx.obj["client"]
    _print(client.get("/ops/v1/clusters"))


@cluster.command("get")
@click.argument("item_id")
@click.pass_context
def get_cmd(ctx, item_id: str):
    """Get cluster details."""
    client = ctx.obj["client"]
    _print(client.get(f"/ops/v1/clusters/{item_id}"))


@cluster.command("create")
@click.option("--data", default="{}", help="JSON payload")
@click.pass_context
def create_cmd(ctx, data: str):
    """Create a new cluster."""
    client = ctx.obj["client"]
    _print(client.post("/ops/v1/clusters", json=json.loads(data)))


@cluster.command("remove")
@click.argument("item_id")
@click.pass_context
def remove_cmd(ctx, item_id: str):
    """Remove a cluster."""
    client = ctx.obj["client"]
    client.delete(f"/ops/v1/clusters/{item_id}")
    click.echo(f"Removed {item_id}")
