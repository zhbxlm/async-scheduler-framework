"""Base CLI utilities — shared by all kubectl-style command groups."""
from __future__ import annotations

import json
import click


def print_json(data):
    """Print data as pretty JSON."""
    click.echo(json.dumps(data, ensure_ascii=False, indent=2, default=str))


class CrudCommandGroup(click.Group):
    """Auto-generates list/get/create/remove commands for a REST resource.
    
    Usage:
    @click.group(cls=CrudCommandGroup, resource="tasks", path="/api/v1/tasks")
    def task():
        pass
    """
    
    def __init__(self, name=None, resource=None, path=None, **kwargs):
        super().__init__(name=name, **kwargs)
        self.resource = resource
        self.path = path
        if self.resource and self.path:
            self._create_default_commands()
    
    def _create_default_commands(self):
        """Create standard CRUD commands."""
        
        @self.command("list")
        @click.pass_context
        def list_cmd(ctx):
            """List all {resource}."""
            client = ctx.obj["client"]
            print_json(client.get(self.path))
        
        @self.command("get")
        @click.argument("item_id")
        @click.pass_context
        def get_cmd(ctx, item_id: str):
            """Get {resource} details."""
            client = ctx.obj["client"]
            print_json(client.get(f"{self.path}/{item_id}"))
        
        @self.command("create")
        @click.option("--data", default="{}", help="JSON payload")
        @click.pass_context
        def create_cmd(ctx, data: str):
            """Create a new {resource}."""
            client = ctx.obj["client"]
            print_json(client.post(self.path, json=json.loads(data)))
        
        @self.command("remove")
        @click.argument("item_id")
        @click.pass_context
        def remove_cmd(ctx, item_id: str):
            """Remove a {resource}."""
            client = ctx.obj["client"]
            client.delete(f"{self.path}/{item_id}")
            click.echo(f"Removed {item_id}")


def crud_group(name, resource, path):
    """Convenience decorator for creating CRUD command groups.
    
    Example:
    @crud_group("task", "tasks", "/api/v1/tasks")
    def task():
        pass
    """
    def decorator(f):
        group = CrudCommandGroup(name=name, resource=resource, path=path)
        return group
    return decorator