"""deploy commands."""
import click


@click.group()
def deploy():
    """Manage deploys."""
    pass


@deploy.command()
def list():
    """List deploys."""
    click.echo(f"Listing deploys (not implemented)")


@deploy.command()
@click.argument("id")
def get(id):
    """Get deploy details."""
    click.echo(f"Getting deploy {id} (not implemented)")
