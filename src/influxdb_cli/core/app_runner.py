import json
import pathlib
import subprocess
import time
from time import sleep

from influxdb_cli.core.influx_client import InfluxClient


def path_passer(path: str) -> pathlib.Path:
    """Validate the path.
    Parameters
    ----------
    path : str
        Path to the file.
    Returns
    -------
    pathlib.Path
        Validated path object.
    Raises
    ------
    FileNotFoundError
        If the config file does not exist.
    """
    config_path = pathlib.Path(path)
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found at {path}")
    return config_path


class AppRunner:
    """Class to run tests on application."""
    def __init__(self,
                 app_config_path: str,
                 docker_container_name: str,
                 influxdb_cli: InfluxClient):
        self.app_config_path = path_passer(app_config_path)
        self.influxdb_cli = influxdb_cli
        self.default_results_dir = self.get_results_dir()
        self.default_database = self.get_default_database()
        command = subprocess.check_output(["docker", "ps"])
        if docker_container_name in command.decode("utf-8"):
            self.docker_container_name = docker_container_name
        else:
            raise ValueError(f"Docker container {docker_container_name} is not running.")

    def get_results_dir(self) -> str:
        """Get the results directory from the application config.
        Returns
        -------
        str
            Path to the result's directory.
        """
        with open(self.app_config_path, "r") as f:
            config_data = json.load(f)
        return config_data["paths"]["output_dir"]

    def get_default_database(self) -> str:
        """Get the default database from the application config.
        Returns
        -------
        str
            Name of the default database.
        """
        with open(self.app_config_path, "r") as f:
            config_data = json.load(f)
        return config_data["data_management"]["influxdb"]["client_data"]["database"]

    def get_logs(self, since_sec: int = 30) -> str:
        """Get the logs from the docker container.
        Parameters
        ----------
        since_sec : str
            Time duration to get logs since.
        Returns
        -------
        str
            Logs from the docker container.
        """
        command = subprocess.check_output(
            ["docker", "logs", "--since", f"{since_sec}s", self.docker_container_name])
        return command.decode("utf-8")

    def restore_config(self):
        """Restore the original application configuration."""
        self.switch_database(self.default_database)
        with open(self.app_config_path, "r") as f:
            config_data = json.load(f)
        config_data["paths"]["output_dir"] = self.default_results_dir
        with open(self.app_config_path, "w") as f:
            json.dump(config_data, f, indent=4)

    def clean_up(self):
        databases = self.get_test_databases()
        for db in databases:
            self.influxdb_cli.clean_database(
                database_name=db,
                exclude_measurements=["driveline_power_data"]
            )
            self.influxdb_cli.add_first_timestamp_to_batch_measurement(
                database_name=db,
                measurement_name="driveline_power_data",
            )

    def restart_container(self):
        """Restart the docker container."""
        subprocess.run(["docker", "restart", self.docker_container_name])

    def is_run_complete(self, check_interval_sec) -> bool:
        """Check if the run is complete.
        Returns
        -------
        bool
            True if the run is complete, False otherwise.
        """
        logs = self.get_logs(check_interval_sec)
        if "Driveline is not rotating, batch skipped." in logs:
            return True
        return False

    def get_test_databases(self, prefix: str = "test") -> list:
        """Get the list of test databases.
        Returns
        -------
        list
            List of test database names.
        """
        result = self.influxdb_cli.query(f"SHOW DATABASES")
        test_databases = [db['name'] for db in result.get_points() if db['name'].startswith(prefix)]
        if not test_databases:
            raise ValueError("No test databases found.")
        print("Databases found:", test_databases)
        return test_databases

    def switch_database(self, database_name: str | None):
        """Switch the database in the application config.
        Parameters
        ----------
        database_name : str | None
            Name of the database to switch to. If None, switch to default database.
        """
        with open(self.app_config_path, "r") as f:
            config_data = json.load(f)
        if database_name is None:
            config_data["data_management"]["influxdb"][
                "client_data"]["database"] = self.default_database
            config_data["paths"]["output_dir"] = self.default_results_dir
        else:
            config_data["data_management"]["influxdb"]["client_data"]["database"] = database_name
            config_data["paths"]["output_dir"] = f"{self.default_results_dir}/{database_name}"
        with open(self.app_config_path, "w") as f:
            json.dump(config_data, f, indent=4)

        self.restart_container()

    def run_process(self, check_interval_sec: int = 60):
        """Run the tests on application and monitor logs until complete.
        Parameters
        ----------
        check_interval_sec : int
            Time interval in seconds to check the logs.
        """
        while not self.is_run_complete(check_interval_sec):
            print("Logs:")
            print(self.get_logs(check_interval_sec))
            print("Run not complete, waiting...")
            time.sleep(check_interval_sec)
        print("Run complete.")

    def run(self, check_interval_sec: int = 30):
        """Run tests on all test databases."""
        try:
            test_databases = self.get_test_databases()
            for db in test_databases:
                print(f"Switching to database: {db}")
                self.switch_database(db)
                sleep(check_interval_sec)
                print(f"Running process for database: {db}")
                self.run_process(check_interval_sec=check_interval_sec)
                print(f"Completed process for database: {db}")
        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            print("Restoring original configuration.")
            self.restore_config()
            self.restart_container()
            print("Configuration restored.")
