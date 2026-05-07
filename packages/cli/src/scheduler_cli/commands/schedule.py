# rebuilt from deepwiki-reference alignment
"""schedule commands — kubectl-style CLI for /ops/v1/schedules."""
from __future__ import annotations
import click
from scheduler_cli.base import CrudCommandGroup


@click.group(cls=CrudCommandGroup, resource="schedules", path="/ops/v1/schedules")
def schedule():
    """Manage schedules."""
