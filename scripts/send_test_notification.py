from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from radar.config import load_config
from radar.models import Lead
from radar.notify import send_daily_summary, send_failure_alert, send_notifications


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send a test notification using the configured channels.")
    parser.add_argument("--config", default="config/sources.real.sample.json", help="Path to config JSON")
    parser.add_argument(
        "--kind",
        choices=("lead", "failure", "daily"),
        default="lead",
        help="Notification type to send",
    )
    return parser


def build_test_lead() -> Lead:
    return Lead(
        source_name="system_test_wecom",
        source_kind="manual",
        category="notification_test",
        title="企业微信 Markdown 通知排版测试",
        content="这是一条真实测试通知，用来确认 markdown 标题、摘要、命中规则和详情链接的最终展示效果。",
        url="https://www.goofish.com/search?q=python%20automation",
        score=66,
        opportunity_strength=66,
        priority="P1",
        price_text="待沟通",
        delivery_text="2天内",
        matched_rules=["wecom_markdown", "layout_refresh"],
    )


def build_daily_stats() -> dict:
    return {
        "new_leads": 7,
        "max_score": 88,
        "source_runs": [
            ("zbj_search_python", "success", 3),
            ("zbj_search_python", "skipped", 1),
            ("xianyu_real", "success", 2),
            ("v2ex_programmer", "error", 1),
        ],
    }


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config)
    if args.kind == "lead":
        sent = send_notifications(config, [build_test_lead()])
    elif args.kind == "failure":
        sent = send_failure_alert(
            config,
            "xianyu_real",
            3,
            "二页点击成功，但后续详情接口返回 ERR_ABORTED，已进入冷却观察。",
            "2026-03-25 22:30:00",
        )
    else:
        sent = send_daily_summary(config, build_daily_stats())
    print(f"test_notifications_sent={sent}")


if __name__ == "__main__":
    main()
