# rebuilt from deepwiki-reference alignment
"""queue commands — kubectl-style CLI for /ops/v1/queue."""
from __future__ import annotations
import click
from src.cli.base import CrudCommandGroup


@click.group(cls=CrudCommandGroup, resource="queues", path="/ops/v1/queue")
def queue():
    """Manage queues."""
    pass
