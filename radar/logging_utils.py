from __future__ import annotations

import logging
from pathlib import Path


def setup_logging(log_path: str | Path, level: str = "INFO") -> None:
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    if root.handlers:
        for handler in list(root.handlers):
            root.removeHandler(handler)

    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

    file_handler = logging.FileHandler(path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root.addHandler(file_handler)
    root.addHandler(console_handler)
