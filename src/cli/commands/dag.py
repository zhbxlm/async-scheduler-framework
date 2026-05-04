# rebuilt from deepwiki-reference alignment
"""dag commands — kubectl-style CLI for /ops/v1/dags."""
from __future__ import annotations
import json
import click


def _print(data):
    click.echo(json.dumps(data, ensure_ascii=False, indent=2, default=str))


@click.group()
def dag():
    """Manage dags."""
    pass


@dag.command("list")
@click.pass_context
def list_cmd(ctx):
    """List all dags."""
    client = ctx.obj["client"]
    _print(client.get("/ops/v1/dags"))


@dag.command("get")
@click.argument("item_id")
@click.pass_context
def get_cmd(ctx, item_id: str):
    """Get dag details."""
    client = ctx.obj["client"]
    _print(client.get(f"/ops/v1/dags/{item_id}"))


@dag.command("create")
@click.option("--data", default="{}", help="JSON payload")
@click.pass_context
def create_cmd(ctx, data: str):
    """Create a new dag."""
    client = ctx.obj["client"]
    _print(client.post("/ops/v1/dags", json=json.loads(data)))


@dag.command("remove")
@click.argument("item_id")
@click.pass_context
def remove_cmd(ctx, item_id: str):
    """Remove a dag."""
    client = ctx.obj["client"]
    client.delete(f"/ops/v1/dags/{item_id}")
    click.echo(f"Removed {item_id}")
