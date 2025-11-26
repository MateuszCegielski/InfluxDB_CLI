import yaml
from pathlib import Path
from platformdirs import user_config_dir
from pydantic import BaseModel, Field

class InvalidConfigError(Exception):
    def __init__(self, message: str = "Configuration file is invalid.") -> None:
        super().__init__(message)

APP_NAME = 'influxdb_cli'

class ConfigModel(BaseModel):
    host: str = Field(description="InfluxDB host address")
    port: int = Field(description="InfluxDB port number")
    retention_policies: list[dict] = Field(
        default=[],
        description="List of retention policies to create with databases"
    )
    database: str | None = Field(default=None, description="Default database to use")


def get_user_config_path():
    config_dir = Path(user_config_dir(APP_NAME))
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir / 'config.yaml'

def load_default_config() -> dict:
    with open(Path(__file__).parent / 'default_config.yaml', 'r') as file:
        return yaml.safe_load(file)

def load_user_config() -> dict:
    user_config_path = get_user_config_path()
    if user_config_path.exists():
        with open(user_config_path, 'r') as file:
            return yaml.safe_load(file)
    else:
        with open(user_config_path, 'w+') as file:
            default_config = load_default_config()
            yaml.safe_dump(default_config, file)
            return {}

def save_config(config: ConfigModel):
    user_config_path = get_user_config_path()
    with open(user_config_path, 'w') as file:
        yaml.safe_dump(config.model_dump(), file)
    return

def _validate_dict_keys(dictionary: dict, required_keys: list[str]):
    for key in required_keys:
        if key not in dictionary:
            raise InvalidConfigError(f"Missing required key: {key}")
    return

def _validate_config(config: dict) -> bool:
    required_keys = ['host', 'port']
    _validate_dict_keys(dictionary=config, required_keys=required_keys)
    if "retention_policies" in config:
        for rp in config["retention_policies"]:
            _validate_dict_keys(
                dictionary=rp,
                required_keys=['name', 'duration', 'replication', 'shard_duration', 'default'])
    return True

def load_config()  -> ConfigModel:
    default_config = load_default_config()
    user_config = load_user_config()
    return ConfigModel(**{**default_config, **user_config})

if __name__ == "__main__":
    print(load_config().model_dump_json(indent=2))