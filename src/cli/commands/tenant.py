# rebuilt from deepwiki-reference alignment
"""tenant commands — kubectl-style CLI for /ops/v1/tenants."""
from __future__ import annotations
import click
from src.cli.base import CrudCommandGroup


@click.group(cls=CrudCommandGroup, resource="tenants", path="/ops/v1/tenants")
def tenant():
    """Manage tenants."""
    pass
