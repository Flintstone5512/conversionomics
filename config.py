import os
import re
import yaml
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

_ENV_VAR = re.compile(r"\$\{([^}]+)\}")


def _resolve_env(value: str) -> str:
    def replacer(m):
        key = m.group(1)
        v = os.environ.get(key)
        if v is None:
            raise EnvironmentError(f"Required env var '{key}' is not set. Check your .env file.")
        return v
    return _ENV_VAR.sub(replacer, value)


def _walk(obj):
    if isinstance(obj, dict):
        return {k: _walk(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk(i) for i in obj]
    if isinstance(obj, str):
        return _resolve_env(obj)
    return obj


def load_config(path: str = "config.yaml") -> dict:
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path.resolve()}")
    with cfg_path.open() as f:
        raw = yaml.safe_load(f)
    return _walk(raw)
