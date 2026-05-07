# rebuilt from deepwiki-reference alignment
"""capability commands — kubectl-style CLI for /ops/v1/capabilities."""
from __future__ import annotations
import click
from scheduler_cli.base import CrudCommandGroup


@click.group(cls=CrudCommandGroup, resource="capabilities", path="/ops/v1/capabilities")
def capability():
    """Manage capabilities."""
