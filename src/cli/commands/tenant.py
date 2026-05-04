# rebuilt from deepwiki-reference alignment
"""tenant commands — kubectl-style CLI for /ops/v1/tenants."""
from __future__ import annotations
import json
import click


def _print(data):
    click.echo(json.dumps(data, ensure_ascii=False, indent=2, default=str))


@click.group()
def tenant():
    """Manage tenants."""
    pass


@tenant.command("list")
@click.pass_context
def list_cmd(ctx):
    """List all tenants."""
    client = ctx.obj["client"]
    _print(client.get("/ops/v1/tenants"))


@tenant.command("get")
@click.argument("item_id")
@click.pass_context
def get_cmd(ctx, item_id: str):
    """Get tenant details."""
    client = ctx.obj["client"]
    _print(client.get(f"/ops/v1/tenants/{item_id}"))


@tenant.command("create")
@click.option("--data", default="{}", help="JSON payload")
@click.pass_context
def create_cmd(ctx, data: str):
    """Create a new tenant."""
    client = ctx.obj["client"]
    _print(client.post("/ops/v1/tenants", json=json.loads(data)))


@tenant.command("remove")
@click.argument("item_id")
@click.pass_context
def remove_cmd(ctx, item_id: str):
    """Remove a tenant."""
    client = ctx.obj["client"]
    client.delete(f"/ops/v1/tenants/{item_id}")
    click.echo(f"Removed {item_id}")
