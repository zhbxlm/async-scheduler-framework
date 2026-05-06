# rebuilt from deepwiki-reference alignment
"""node commands — kubectl-style CLI for /ops/v1/nodes."""
from __future__ import annotations
import click
from src.cli.base import CrudCommandGroup


@click.group(cls=CrudCommandGroup, resource="nodes", path="/ops/v1/nodes")
def node():
    """Manage nodes."""
