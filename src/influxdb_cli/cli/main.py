import typer
from influxdb_cli.cli.commands import config, database, measurement

from influxdb_cli.core.influx_client import InfluxClient

app = typer.Typer()

app.add_typer(config.app, name="config", help="Manage configuration settings.")
app.add_typer(database.app, name="database", help="Manage the database.")
app.add_typer(measurement.app, name="measurement", help="Manage the measurement.")

@app.command(name="query", help="Execute a custom InfluxDB query.")
def query():
    influx_client = InfluxClient()
    query_str = typer.prompt("Enter your InfluxDB query")
    result = influx_client.query(query_str)
    typer.echo("Query Result:")
    for point in result.get_points():
        typer.echo(point)


if __name__ == "__main__":
    app()