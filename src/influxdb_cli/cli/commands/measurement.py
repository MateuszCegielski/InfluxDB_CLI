from influxdb_cli.core.influx_client import InfluxClient
import typer


app = typer.Typer(name="measurement")

@app.command(name="list", help="List measurements in a database.")
def list_measurements(
        database_name: str = typer.Option(None,"--database_name", "-d",
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
        file_path: str = typer.Option(None,"--file-path", "-f", help=("Path to the file containing "
                                                             "measurements to add.")),
        dir_path: str = typer.Option(None,"--dir-path", "-D", help=("Path to the directory containing "
                                                           "files with measurements to add.")),
        measurement_name: str = typer.Option(
            None,"--measurement_name", "-n", help="Name of the measurement "
            "under the  measurements namespace"),
        database_name: str = typer.Option(None,"--database_name", "-d",
                                          help="Name of the database if not using the any "
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