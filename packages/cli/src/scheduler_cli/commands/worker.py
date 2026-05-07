"""worker commands — kubectl-style CLI.

Deprecation note: worker management moved to node-agent lifecycle.
The CLI command group is kept as a stub to avoid breaking scripts.
"""
from __future__ import annotations
import click


@click.group()
def worker():
    """Manage workers. (Deprecated — use node-agent instead.)"""


@worker.command("list")
@click.pass_context
def list_cmd(ctx):
    """List workers (deprecated)."""
    click.echo("Worker management has moved to node-agent. Use 'async node list' instead.")
