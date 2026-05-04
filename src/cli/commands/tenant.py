"""tenant commands."""
import click


@click.group()
def tenant():
    """Manage tenants."""
    pass


@tenant.command()
def list():
    """List tenants."""
    click.echo(f"Listing tenants (not implemented)")


@tenant.command()
@click.argument("id")
def get(id):
    """Get tenant details."""
    click.echo(f"Getting tenant {id} (not implemented)")
