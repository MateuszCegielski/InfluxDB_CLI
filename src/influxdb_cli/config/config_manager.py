import yaml
from pathlib import Path
from platformdirs import user_config_dir

APP_NAME = 'influxdb_cli'


def get_user_config_path():
    config_dir = Path(user_config_dir(APP_NAME))
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir / 'config.yaml'

def load_default_config() -> dict:
    with open(Path().cwd() / 'default_config.yaml', 'r') as file:
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

def load_config()  -> dict:
    default_config = load_default_config()
    user_config = load_user_config()
    return {**default_config, **user_config}
