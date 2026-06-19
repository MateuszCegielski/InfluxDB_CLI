import types
from typer.testing import CliRunner
import influxdb_cli.cli.main as main_module

runner = CliRunner()


class DummyResult:
    def get_points(self):
        return [{"a": 1}, {"a": 2}]


class DummyClient:
    def __init__(self):
        self.config = types.SimpleNamespace(database="db_current")
        self.calls = []

    def query(self, q):
        self.calls.append(("query", q))
        return DummyResult()

    def ensure_database_exists(self, db):
        self.calls.append(("ensure_database_exists", db))

    def switch_database(self, db):
        self.calls.append(("switch_database", db))
        self.config.database = db


def test_main_help():
    result = runner.invoke(main_module.app, ["--help"])
    assert result.exit_code == 0
    assert "create" in result.stdout
    assert "add" in result.stdout
    assert "show" in result.stdout
    assert "modify" in result.stdout
    assert "delete" in result.stdout


def test_main_query_direct(monkeypatch, capsys):
    dummy = DummyClient()
    monkeypatch.setattr(main_module, "InfluxClient", lambda: dummy)
    monkeypatch.setattr(main_module.typer, "prompt", lambda _: "SELECT * FROM m1 LIMIT 2")

    main_module.query()

    captured = capsys.readouterr()
    assert "Query Result:" in captured.out
    assert "{'a': 1}" in captured.out
    assert "{'a': 2}" in captured.out
    assert dummy.calls[0] == ("query", "SELECT * FROM m1 LIMIT 2")


def test_main_use_database_direct(monkeypatch, capsys):
    dummy = DummyClient()
    monkeypatch.setattr(main_module, "InfluxClient", lambda: dummy)

    main_module.use_database("db1")

    captured = capsys.readouterr()
    assert "Active database set to 'db1'." in captured.out
    assert ("ensure_database_exists", "db1") in dummy.calls
    assert ("switch_database", "db1") in dummy.calls