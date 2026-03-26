from __future__ import annotations

import argparse
import time

from radar.config import load_config
from radar.logging_utils import setup_logging
from radar.pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the demand radar MVP.")
    parser.add_argument("--config", required=True, help="Path to config JSON")
    parser.add_argument("--export", required=False, help="Override export CSV path")
    parser.add_argument("--interval-seconds", type=int, required=False, help="Run in a loop with this interval")
    parser.add_argument("--max-runs", type=int, required=False, help="Stop after N runs when interval mode is enabled")
    return parser


def _print_result(result: dict[str, int]) -> None:
    print(
        "Demand radar completed: "
        f"fetched={result['fetched']} inserted={result['inserted']} "
        f"skipped={result['skipped']} exported={result['exported']} "
        f"health_exported={result['health_exported']} "
        f"top_leads_exported={result['top_leads_exported']} theme_exported={result['theme_exported']} "
        f"product_exported={result['product_exported']} "
        f"notified_leads={result['notified_leads']} notifications_sent={result['notifications_sent']} "
        f"failure_alerts_sent={result['failure_alerts_sent']} daily_summary_sent={result['daily_summary_sent']} "
        f"run_digest_sent={result['run_digest_sent']} "
        f"source_errors={result['source_errors']}"
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = load_config(args.config)
    base_dir = config["_base_dir"]
    log_path = config.get("log_path", "logs/demand_radar.log")
    setup_logging(f"{base_dir}/{log_path}", level=config.get("log_level", "INFO"))

    interval = int(args.interval_seconds or 0)
    if interval <= 0:
        result = run_pipeline(config, export_path=args.export)
        _print_result(result)
        return

    run_count = 0
    while True:
        result = run_pipeline(config, export_path=args.export)
        _print_result(result)
        run_count += 1
        if args.max_runs and run_count >= args.max_runs:
            break
        time.sleep(interval)


if __name__ == "__main__":
    main()
