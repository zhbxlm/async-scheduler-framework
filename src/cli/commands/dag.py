# rebuilt from deepwiki-reference alignment
"""dag commands — kubectl-style CLI for /api/v1/dags."""
from __future__ import annotations
import click
from src.cli.base import CrudCommandGroup


@click.group(cls=CrudCommandGroup, resource="dags", path="/api/v1/dags")
def dag():
    """Manage dags."""
