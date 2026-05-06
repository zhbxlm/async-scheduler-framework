# rebuilt from deepwiki-reference alignment
"""cluster commands — kubectl-style CLI for /ops/v1/clusters."""
from __future__ import annotations
import click
from src.cli.base import CrudCommandGroup


@click.group(cls=CrudCommandGroup, resource="clusters", path="/ops/v1/clusters")
def cluster():
    """Manage clusters."""
