import typer
from influxdb_cli.core.influx_client import InfluxClient

app = typer.Typer(name="measurement")


@app.command(name="list", help="List measurements in a database.")
def list_measurements(
        database_name: str = typer.Option(None, "--database_name", "-d",
                                          help="Name of the database if not using the any "
                                               "database or wanting show measurements from a "
                                               "specific one without checking out.")
):
    """List all measurements in the specified database."""
    client = InfluxClient()
    measurements = client.show_measurements(database_name=database_name)
    typer.echo(f"Measurements in database: {database_name or client.config.database}:")
    for measurement in measurements:
        typer.echo(f"- {measurement}")


@app.command(name="add", help="Count measurements in a database.")
def add_measurements(
        file_path: str = typer.Option(None, "--file-path", "-f",
                                      help=("Path to the file containing "
                                            "measurements to add.")),
        dir_path: str = typer.Option(None, "--dir-path", "-D",
                                     help=("Path to the directory containing "
                                           "files with measurements to add.")),
        measurement_name: str = typer.Option(
            None, "--measurement_name", "-n", help="Obligatory when adding "
                                                   "from directory. Name of the measurement "
                                                   "under the  measurements namespace. If not provided"
                                                   ", the file name will be used as the "
                                                   "measurement name."),
        database_name: str = typer.Option(None, "--database_name", "-d",
                                          help="Name of the database if not using any "
                                               "database or wanting to add measurement to the "
                                               "specific one without checking out.")
):
    """Count all measurements in the specified database."""
    client = InfluxClient()
    if dir_path and file_path:
        typer.echo("Error: Please provide either --dir-path or --file-path, not both.")
        raise typer.Exit(code=1)
    if dir_path:
        client.add_measurement_from_dir(
            file_path=dir_path,
            measurement_name=measurement_name
        )
        typer.echo(f"Created databases and added measurements from directory: {dir_path}.")
        return
    measurements = client.add_measurements(
        database_name=database_name,
        file_path=file_path,
        measurement_name=measurement_name
    )
    typer.echo(f"Added {measurements} measurements to database: "
               f"{database_name or client.config.database}.")


@app.command(name="delete", help="Delete a measurement from a database.")
def delete_measurement(
        measurement_name: str = typer.Argument(help="Name of the measurement to delete."),
        database_name: str = typer.Option(None, "--database_name", "-d",
                                          help="Name of the database if not using the any "
                                               "database or wanting to delete measurement from "
                                               "the specific one without checking out.")
):
    """Delete a measurement from the specified database."""
    client = InfluxClient()
    client.delete_measurement(
        measurement_name=measurement_name,
        database_name=database_name
    )
    typer.echo(f"Measurement '{measurement_name}' deleted from database: "
               f"{database_name or client.config.database}.")


@app.command(name="show", help="Show content of a measurement.")
def show_measurement(
        measurement_name: str = typer.Argument(help="Name of the measurement to show."),
        retention_policy: str = typer.Option(None, "--retention-policy", "-r",
                                             help="Retention policy of the measurement."),
        column_names: str = typer.Option(None, "--column", "-c",
                                         help="Specific column(s) to display."),
        from_time: str = typer.Option(None, "--from-time", "-f",
                                      help="Start time for the data range."
                                           "Supported formats:\n"
                                           " - '%Y-%m-%dT%H:%M:%SZ' (RFC 3339): "
                                           "2024-01-15T14:30:45Z\n"
                                           " - '%Y-%m-%d %H:%M:%S': 2024-01-15 14:30:45\n"
                                           "- '%Y-%m-%dT%H:%M:%S.%fZ': "
                                           "2024-01-15T14:30:45.123456Z\n"
                                           " - '%Y-%m-%d %H:%M:%S.%f': 2024-01-15 14:30:45.123456" ),
        to_time: str = typer.Option(None, "--to-time", "-t",
                                    help="End time for the data range."
                                         "Supported formats are the same like for 'from_time' "
                                         "parameter"),
        where_clause: str = typer.Option(None, "--where-clause", "-w",
                                         help="Additional WHERE clause for filtering data."),
        limit: int = typer.Option(None, "--limit", "-l",
                                  help="Limit the number of results returned."),
        database_name: str = typer.Option(None, "--database_name", "-d",
                                          help="Name of the database if not using the any "
                                               "database or wanting to show measurement from "
                                               "the specific one without checking out."),
        path: str = typer.Option(None, "--path", "-p",
                                 help="Path to the file to save the measurement.")
):
    influx_client = InfluxClient()
    results = influx_client.show_measurement(
        measurement_name=measurement_name,
        retention_policy=retention_policy,
        column_names=column_names,
        from_time=from_time,
        to_time=to_time,
        where_clause=where_clause,
        limit=limit,
        database_name=database_name or influx_client.config.database,
        path=path
    )
    if path:
        typer.echo(f"Saved {results} records from measurement '{measurement_name}' to {path}.")
        return
    typer.echo(f"Displayed command result from measurement '{measurement_name}':")
    typer.echo(results)
