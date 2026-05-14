import typer
from typing import Annotated
from rich.console import Console

from influxdb_cli.config.config_manager import load_config, get_user_config_path
from influxdb_cli.core.influx_client import InfluxClient

app = typer.Typer(name="show", rich_markup_mode="markdown", no_args_is_help=True)


@app.command(name="measurements", help="Show measurements in a database.")
def show_measurements(
    database_name: Annotated[
        str | None,
        typer.Option(
            "--database_name",
            "-d",
            help="Name of the database if not specified, the current one will be used.",
        ),
    ] = None,
):
    """List all measurements in the specified database.
    Parameters:
    -----------
    database_name: str
        Name of the database. If not specified, the current one will be used.
    """
    client = InfluxClient()
    client.execute_method_on_db(
        database=database_name,
        method=client.show_measurements,
    )
    measurements = client.show_measurements()
    typer.echo(f"Measurements in database: {database_name or client.config.database}:")
    for measurement in measurements:
        typer.echo(f"- {measurement}")


@app.command(name="measurement", help="Show content of a measurement.")
def show_measurement(
    measurement_name: Annotated[str, typer.Argument(help="Name of the measurement to show.")],
    retention_policy: Annotated[
        str | None,
        typer.Option("--retention-policy", "-r", help="Retention policy of the measurement."),
    ] = None,
    column_names: Annotated[
        str | None, typer.Option("--column", "-c", help="Specific column(s) to display.")
    ] = None,
    from_time: Annotated[
        str | None,
        typer.Option(
            "--from-time",
            "-f",
            help="Start time for the data range. \n\n"
            "Supported formats:\n\n"
            " - '%Y-%m-%dT%H:%M:%SZ'[RFC 3339]: 2024-01-15T14:30:45Z\n\n"
            " - '%Y-%m-%d %H:%M:%S': 2024-01-15 14:30:45\n\n"
            " - '%Y-%m-%dT%H:%M:%S.%fZ': 2024-01-15T14:30:45.123456Z\n\n"
            " - '%Y-%m-%d %H:%M:%S.%f': 2024-01-15 14:30:45.123456\n\n",
        ),
    ] = None,
    to_time: Annotated[
        str | None,
        typer.Option(
            "--to-time",
            "-t",
            help="End time for the data range. "
            "Supported formats are the same like for 'from_time' "
            "parameter",
        ),
    ] = None,
    where_clause: Annotated[
        str | None,
        typer.Option("--where-clause", "-w", help="Additional WHERE clause for filtering data."),
    ] = None,
    limit: Annotated[
        int | None, typer.Option("--limit", "-l", help="Limit the number of results returned.")
    ] = None,
    database_name: Annotated[
        str | None,
        typer.Option(
            "--database_name",
            "-d",
            help="Name of the database. If not specified, the current one will be used.",
        ),
    ] = None,
    path: Annotated[
        str | None, typer.Option("--path", "-p", help="Path to the file to save the measurement.")
    ] = None,
):
    """Show content of a measurement.
    Parameters:
    -----------
    measurement_name: str
        Name of the measurement to show.
    retention_policy: str, optional
        Retention policy of the measurement.
    column_names: str, optional
        Specific column(s) to display.
    from_time: str, optional
        Start time for the data range. Supported formats:
        - '%Y-%m-%dT%H:%M:%SZ' (RFC 3339): 2024-01-15T14:30:45Z
        - '%Y-%m-%d %H:%M:%S': 2024-01-15 14:30:45
        - '%Y-%m-%dT%H:%M:%S.%fZ': 2024-01-15T14:30:45.123456Z
        - '%Y-%m-%d %H:%M:%S.%f': 2024-01-15 14:30:45.123456
    to_time: str, optional
        End time for the data range. Supported formats are the same as for 'from_time' parameter.
    where_clause: str, optional
        Additional WHERE clause for filtering data.
    limit: int, optional
        Limit the number of results returned.
    database_name: str, optional
        Name of the database. If not specified, the current one will be used.
    """
    influx_client = InfluxClient()
    results = influx_client.execute_method_on_db(
        method=influx_client.show_measurement,
        database=database_name,
        measurement_name=measurement_name,
        retention_policy=retention_policy,
        column_names=column_names,
        from_time=from_time,
        to_time=to_time,
        where_clause=where_clause,
        limit=limit,
        path=path,
    )
    if path:
        typer.echo(f"Saved {results} records from measurement '{measurement_name}' to {path}.")
        return
    typer.echo(f"Displayed command result from measurement '{measurement_name}':")
    typer.echo(results)


@app.command(name="database", help="Show database currently in use.")
def show_used_db():
    """Show database currently in use."""
    influx_client = InfluxClient()
    typer.echo(f"Database in use: {influx_client.config.database}")


@app.command(name="databases", help="Show all existing databases.")
def show_databases():
    """Show all existing databases."""
    influx_client = InfluxClient()
    databases = influx_client.list_databases()
    typer.echo("Databases:")
    for db in databases:
        typer.echo(f"- {db}")


@app.command(name="retention-policies", help="Show retention policies for a specific database.")
def show_retention_policies(
    database_name: Annotated[str | None, typer.Argument(help="Name of the database")] = None,
):
    """Show retention policies for a specific database.
    Parameters:
    -----------
        database_name: Name of the database.
    """
    influx_client = InfluxClient()
    table = influx_client.execute_method_on_db(
        method=influx_client.get_retention_policy_table,
        database=database_name,
    )
    if not table:
        typer.echo(f"No retention policies found for database '{database_name}'.")
        return
    Console().print(table)


@app.command("config", help="Show configuration settings.")
def show_config():
    """Show current configuration settings."""
    typer.echo("Current configuration:")
    typer.echo(load_config().model_dump_json(indent=2))


@app.command("config-path", help="Show path to configuration settings.")
def show_path():
    """Show path to configuration settings."""
    typer.echo("Path to the user configuration:")
    typer.echo(get_user_config_path())
