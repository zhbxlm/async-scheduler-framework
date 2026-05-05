# rebuilt from deepwiki-reference alignment
"""task commands — kubectl-style CLI for /api/v1/tasks."""
from __future__ import annotations
import click
from src.cli.base import CrudCommandGroup


@click.group(cls=CrudCommandGroup, resource="tasks", path="/api/v1/tasks")
def task():
    """Manage tasks."""
    pass
