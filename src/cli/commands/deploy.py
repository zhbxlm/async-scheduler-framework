"""deploy commands — kubectl-style CLI.

Deprecation note: deployment moved to node-agent lifecycle.
The CLI command group is kept as a stub to avoid breaking scripts.
"""
from __future__ import annotations
import click


@click.group()
def deploy():
    """Manage deploys. (Deprecated — use node-agent instead.)"""
    pass


@deploy.command("list")
@click.pass_context
def list_cmd(ctx):
    """List deploys (deprecated)."""
    click.echo("Deployment management has moved to node-agent.")
