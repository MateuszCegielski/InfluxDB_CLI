import typer
from influxdb_cli.cli.commands import config, database, measurement


app = typer.Typer()

app.add_typer(config.app, name="config", help="Manage configuration settings.")
app.add_typer(database.app, name="database", help="Manage the database.")
app.add_typer(measurement.app, name="measurement", help="Manage the measurement.")

if __name__ == "__main__":
    app()