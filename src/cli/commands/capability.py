# rebuilt from deepwiki-reference alignment
"""capability commands — kubectl-style CLI for /ops/v1/capabilities."""
from __future__ import annotations
import json
import click


def _print(data):
    click.echo(json.dumps(data, ensure_ascii=False, indent=2, default=str))


@click.group()
def capability():
    """Manage capabilitys."""
    pass


@capability.command("list")
@click.pass_context
def list_cmd(ctx):
    """List all capabilitys."""
    client = ctx.obj["client"]
    _print(client.get("/ops/v1/capabilities"))


@capability.command("get")
@click.argument("item_id")
@click.pass_context
def get_cmd(ctx, item_id: str):
    """Get capability details."""
    client = ctx.obj["client"]
    _print(client.get(f"/ops/v1/capabilities/{item_id}"))


@capability.command("create")
@click.option("--data", default="{}", help="JSON payload")
@click.pass_context
def create_cmd(ctx, data: str):
    """Create a new capability."""
    client = ctx.obj["client"]
    _print(client.post("/ops/v1/capabilities", json=json.loads(data)))


@capability.command("remove")
@click.argument("item_id")
@click.pass_context
def remove_cmd(ctx, item_id: str):
    """Remove a capability."""
    client = ctx.obj["client"]
    client.delete(f"/ops/v1/capabilities/{item_id}")
    click.echo(f"Removed {item_id}")
