import types
from typer.testing import CliRunner
import influxdb_cli.cli.commands.delete as delete_module

runner = CliRunner()


class DummyClient:
    def __init__(self):
        self.config = types.SimpleNamespace(database="db_current")
        self.deleted = []
        self.calls = []

    def execute_method_on_db(self, method, database=None, **kwargs):
        self.calls.append((method, database, kwargs))
        if method == self.clean_database:
            return "Deleted 2 measurement(s) from database 'db_current'."
        return None

    def delete_measurement(self, measurement_name=None):
        self.deleted.append(measurement_name)

    def clean_database(self, exclude_measurements=None):
        return "cleaned"

    def delete_database(self, database):
        if database == "missing":
            raise ValueError("missing")
        self.deleted.append(database)

    def list_databases(self):
        return ["db1", "db2"]


def test_delete_help():
    result = runner.invoke(delete_module.app, ["--help"])
    assert result.exit_code == 0
    assert "measurement" in result.stdout
    assert "measurements" in result.stdout
    assert "database" in result.stdout


def test_delete_measurement_direct(monkeypatch, capsys):
    dummy = DummyClient()
    monkeypatch.setattr(delete_module, "InfluxClient", lambda: dummy)

    delete_module.delete_measurement("m1", database_name="db1")

    captured = capsys.readouterr()
    assert "Measurement 'm1' deleted from database" in captured.out
    assert len(dummy.calls) == 1
    method, database, _ = dummy.calls[0]
    assert method == dummy.delete_measurement
    assert database == "db1"


def test_delete_all_measurements_direct(monkeypatch, capsys):
    dummy = DummyClient()
    monkeypatch.setattr(delete_module, "InfluxClient", lambda: dummy)

    delete_module.delete_all_measurements(database_name="db1", exclude_measurements=["m2"])

    captured = capsys.readouterr()
    assert "Deleted 2 measurement(s)" in captured.out
    assert len(dummy.calls) == 1
    method, database, kwargs = dummy.calls[0]
    assert method == dummy.clean_database
    assert database == "db1"
    assert kwargs["exclude_measurements"] == ["m2"]


def test_delete_database_single_direct(monkeypatch, capsys):
    dummy = DummyClient()
    monkeypatch.setattr(delete_module, "InfluxClient", lambda: dummy)

    delete_module.delete_database(database_name="db1", delete_all_databases=False)

    captured = capsys.readouterr()
    assert "Database 'db1' deleted successfully." in captured.out


def test_delete_database_all_cli_confirm_yes(monkeypatch):
    dummy = DummyClient()
    monkeypatch.setattr(delete_module, "InfluxClient", lambda: dummy)
    monkeypatch.setattr(delete_module.typer, "confirm", lambda _: True)

    result = runner.invoke(delete_module.app, ["database", "--all"])
    assert result.exit_code == 0
    assert "Database 'db1' deleted successfully." in result.stdout
    assert "Database 'db2' deleted successfully." in result.stdout