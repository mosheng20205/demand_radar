from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any


def load_config(path: str | Path) -> dict[str, Any]:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        config = json.load(handle)

    base_dir = config_path.parent.parent if config_path.parent.name == "config" else config_path.parent
    config = _resolve_env_values(config)
    config["_base_dir"] = str(base_dir)
    return config


def resolve_path(base_dir: str | Path, location: str) -> Path:
    path = Path(location)
    if path.is_absolute():
        return path
    return Path(base_dir) / path


_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def _resolve_env_string(value: str) -> str:
    def _replace(match: re.Match[str]) -> str:
        var_name = match.group(1)
        return os.getenv(var_name, "")

    return _ENV_PATTERN.sub(_replace, value)


def _resolve_env_values(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _resolve_env_values(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_resolve_env_values(item) for item in value]
    if isinstance(value, str):
        return _resolve_env_string(value)
    return value
