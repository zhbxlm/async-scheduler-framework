# rebuilt from deepwiki-reference alignment
"""schedule commands — kubectl-style CLI for /ops/v1/schedules."""
from __future__ import annotations
import json
import click


def _print(data):
    click.echo(json.dumps(data, ensure_ascii=False, indent=2, default=str))


@click.group()
def schedule():
    """Manage schedules."""
    pass


@schedule.command("list")
@click.pass_context
def list_cmd(ctx):
    """List all schedules."""
    client = ctx.obj["client"]
    _print(client.get("/ops/v1/schedules"))


@schedule.command("get")
@click.argument("item_id")
@click.pass_context
def get_cmd(ctx, item_id: str):
    """Get schedule details."""
    client = ctx.obj["client"]
    _print(client.get(f"/ops/v1/schedules/{item_id}"))


@schedule.command("create")
@click.option("--data", default="{}", help="JSON payload")
@click.pass_context
def create_cmd(ctx, data: str):
    """Create a new schedule."""
    client = ctx.obj["client"]
    _print(client.post("/ops/v1/schedules", json=json.loads(data)))


@schedule.command("remove")
@click.argument("item_id")
@click.pass_context
def remove_cmd(ctx, item_id: str):
    """Remove a schedule."""
    client = ctx.obj["client"]
    client.delete(f"/ops/v1/schedules/{item_id}")
    click.echo(f"Removed {item_id}")
