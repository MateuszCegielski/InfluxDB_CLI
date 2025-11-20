import typer
from influxdb_cli.cli.commands import config, database


app = typer.Typer()

app.add_typer(config.app, name="config", help="Manage configuration settings.")
app.add_typer(database.app, name="database", help="Manage the database.")

if __name__ == "__main__":
    app()