from typer.testing import CliRunner
import influxdb_cli.cli.commands.modify as modify_module


runner = CliRunner()


class DummyClient:
    def __init__(self):
        self.calls = []

    def modify_retention_policy(self, **kwargs):
        return "unused"

    def execute_method_on_db(self, method, database=None, **kwargs):
        self.calls.append(
            {
                "method": method,
                "database": database,
                "kwargs": kwargs,
            }
        )
        return "Modify Retention Policies on test_db database successfully."


def test_modify_group_help():
    result = runner.invoke(modify_module.app, ["--help"])
    assert result.exit_code == 0
    assert "retention-policy" in result.stdout


def test_modify_retention(monkeypatch, capsys):
    dummy = DummyClient()
    monkeypatch.setattr(modify_module, "InfluxClient", lambda: dummy)

    modify_module.modify_retention_policy(
        retention_policy_name="rp_7d",
        new_duration="30d",
        new_replication=2,
        set_default=True,
        database_name="my_db",
    )

    captured = capsys.readouterr()
    assert "Modify Retention Policies on test_db database successfully." in captured.out

    assert len(dummy.calls) == 1
    call = dummy.calls[0]

    assert call["database"] == "my_db"
    assert call["kwargs"]["retention_policy_name"] == "rp_7d"
    assert call["kwargs"]["new_duration"] == "30d"
    assert call["kwargs"]["new_replication"] == 2
    assert call["kwargs"]["set_default"] is True
    assert call["method"] == dummy.modify_retention_policy