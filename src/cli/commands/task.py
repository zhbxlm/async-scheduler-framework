# rebuilt from deepwiki-reference alignment
"""task commands — kubectl-style CLI for /api/v1/tasks."""
from __future__ import annotations
import json
import click


def _print(data):
    click.echo(json.dumps(data, ensure_ascii=False, indent=2, default=str))


@click.group()
def task():
    """Manage tasks."""
    pass


@task.command("list")
@click.pass_context
def list_cmd(ctx):
    """List all tasks."""
    client = ctx.obj["client"]
    _print(client.get("/api/v1/tasks"))


@task.command("get")
@click.argument("item_id")
@click.pass_context
def get_cmd(ctx, item_id: str):
    """Get task details."""
    client = ctx.obj["client"]
    _print(client.get(f"/api/v1/tasks/{item_id}"))


@task.command("create")
@click.option("--data", default="{}", help="JSON payload")
@click.pass_context
def create_cmd(ctx, data: str):
    """Create a new task."""
    client = ctx.obj["client"]
    _print(client.post("/api/v1/tasks", json=json.loads(data)))


@task.command("remove")
@click.argument("item_id")
@click.pass_context
def remove_cmd(ctx, item_id: str):
    """Remove a task."""
    client = ctx.obj["client"]
    client.delete(f"/api/v1/tasks/{item_id}")
    click.echo(f"Removed {item_id}")
