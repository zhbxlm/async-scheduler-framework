"""cluster commands."""
import click


@click.group()
def cluster():
    """Manage clusters."""
    pass


@cluster.command()
def list():
    """List clusters."""
    click.echo(f"Listing clusters (not implemented)")


@cluster.command()
@click.argument("id")
def get(id):
    """Get cluster details."""
    click.echo(f"Getting cluster {id} (not implemented)")
