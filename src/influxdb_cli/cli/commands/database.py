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
        database_name: str = typer.Argument(help="Name of the database to delete"),
        delete_all_databases: bool = typer.Option(
            False, "--all", "-a", help="Delete all databases (use with caution!)")
):
    influx_client = InfluxClient()
    if delete_all_databases:
        confirm = typer.confirm(
            "Are you sure you want to delete ALL databases? This action cannot be undone.")
        if not confirm:
            typer.echo("Operation cancelled.")
            raise typer.Exit()
        databases = influx_client.list_databases()
        for db in databases:
            influx_client.delete_database(db)
        typer.echo("All databases deleted successfully.")
        raise typer.Exit()
    else:
        influx_client.delete_database(database_name)
    typer.echo("Database deleted successfully.")


@app.command(name="list", help="List all databases.")
def list_databases():
    influx_client = InfluxClient()
    databases = influx_client.list_databases()
    typer.echo("Databases:")
    for db in databases:
        typer.echo(f"- {db}")


@app.command(name="list-retention-policies", help="List retention policies for a database.")
def list_retention_policies(
        database_name: str = typer.Option(None, "--database", "-d ", help="Name of the database")
):
    influx_client = InfluxClient()
    database = database_name or influx_client.config.database
    rps = influx_client.list_retention_policies(database)
    typer.echo(f"Retention Policies for database '{database}':")
    for rp in rps:
        typer.echo(f"- Name: {rp['name']}, Duration: {rp['duration']}, "
                   f"Replication: {rp['replication']}, Shard Duration: {rp['shard_duration']}, "
                   f"Default: {rp['default']}")


@app.command(name="use", help="Set the active database for the session.")
def use_database(
        database_name: str = typer.Argument(help="Name of the database to use")
):
    influx_client = InfluxClient()
    influx_client.switch_database(database_name)
    typer.echo(f"Active database set to '{database_name}'.")


@app.command(name="show", help="Show database name in use")
def show_used_db():
    influx_client = InfluxClient()
    typer.echo(f"Database name used: {influx_client.config.database}")


@app.command(name="modify-retention-policy", help="Modify a retention policy.")
def modify_retention_policy(
        database_name: str = typer.Option(None, "--database", "-d",
                                          help="Name of the database, if not specified "
                                               "the current database will be used."),
        retention_policy_name: str = typer.Argument(help="Name of the retention policy to modify"),
        new_duration: str = typer.Option(
            None, "--duration", "-n", help="New duration for the retention policy"),
        new_replication: int = typer.Option(
            None, "--replication", "-r", help="New replication factor for the retention policy")
):
    influx_client = InfluxClient()
    database = database_name or influx_client.config.database
    influx_client.modify_retention_policy(
        database=database,
        retention_policy_name=retention_policy_name,
        new_duration=new_duration,
        new_replication=new_replication
    )
    typer.echo(f"Retention policy modified successfully on {database} database.")


@app.command(name="clean", help="Clean a database by removing all data.")
def clean_database(
        database_name: str = typer.Option(None, "--database-name", "-d",
                                          help="Name of the database to clean, if not specified "
                                               "the current database will be used."),
        exclude_measurements: str = typer.Option(None, "--except", "-e",
                                                 help="Name of the measurement to exclude from cleaning")
):
    influx_client = InfluxClient()
    influx_client.clean_database(
        database_name=database_name or influx_client.config.database,
        exclude_measurements=exclude_measurements
    )
    typer.echo(f"Database '{database_name}' cleaned successfully.")
