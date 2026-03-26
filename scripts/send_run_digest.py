from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from radar.config import load_config
from radar.run_digest import render_run_digest, send_run_digest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send a human-readable WeCom digest for the latest full run.")
    parser.add_argument("--config", default="config/sources.real.sample.json", help="Path to config JSON")
    parser.add_argument("--preview", action="store_true", help="Print the markdown instead of sending")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config)
    markdown = render_run_digest(config)

    if args.preview:
        sys.stdout.buffer.write(markdown.encode("utf-8", errors="ignore"))
        sys.stdout.buffer.write(b"\n")
        return

    sent = send_run_digest(config)
    print(f"run_digest_sent={sent}")


if __name__ == "__main__":
    main()
