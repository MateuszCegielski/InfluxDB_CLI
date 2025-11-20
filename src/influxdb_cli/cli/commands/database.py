import typer

app = typer.Typer(name="database")

@app.command(help="I need help!!!")
def create_database():
    typer.echo("Creating database")