import typer
from influxdb_cli.core.influx_client import InfluxClient

app = typer.Typer(name="database")

@app.command(name="create", help="Create a new database.")
def create_database(
        database_name: str = typer.Argument(help="Name of the database to create"),
        retention_policy: bool = typer.Option(
            False, "--retention-policy", "-r",
            help="Create retention policies as defined in the config file")
):
    influx_client = InfluxClient()
    influx_client.create_database(database_name, retention_policy=retention_policy)
    typer.echo("Database created successfully.")

@app.command(name="delete", help="Delete a database.")
def delete_database(
        database_name: str = typer.Argument(help="Name of the database to delete")
):
    influx_client = InfluxClient()
    influx_client.delete_database(database_name)
    typer.echo("Database deleted successfully.")

@app.command(name="show", help="List all databases.")
def list_databases():
    influx_client = InfluxClient()
    databases = influx_client.list_databases()
    typer.echo("Databases:")
    for db in databases:
        typer.echo(f"- {db}")

@app.command(name="show-retention-policies", help="List retention policies for a database.")
def list_retention_policies(
        database_name: str = typer.Argument(help="Name of the database")
):
    influx_client = InfluxClient()
    rps = influx_client.list_retention_policies(database_name)
    typer.echo(f"Retention Policies for database '{database_name}':")
    for rp in rps:
        typer.echo(f"- Name: {rp['name']}, Duration: {rp['duration']}, "
                   f"Replication: {rp['replication']}, Shard Duration: {rp['shard_duration']}, "
                   f"Default: {rp['default']}")

@app.command(name="modify-retention-policy", help="Modify a retention policy.")
def modify_retention_policy(
        database_name: str = typer.Argument(help="Name of the database"),
        retention_policy_name: str = typer.Argument(help="Name of the retention policy to modify"),
        new_duration: str = typer.Option(
            None, "--duration", "-d", help="New duration for the retention policy"),
        new_replication: int = typer.Option(
            None, "--replication", "-r", help="New replication factor for the retention policy")
):
    influx_client = InfluxClient()
    influx_client.modify_retention_policy(
        database_name,
        retention_policy_name,
        new_duration=new_duration,
        new_replication=new_replication
    )
    typer.echo("Retention policy modified successfully.")

@app.command(name="use", help="Set the active database for the session.")
def use_database(
        database_name: str = typer.Argument(help="Name of the database to use")
):
    influx_client = InfluxClient()
    influx_client.switch_database(database_name)
    typer.echo(f"Active database set to '{database_name}'.")
