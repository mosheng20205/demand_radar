from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from radar.config import load_config, resolve_path


@dataclass(slots=True)
class RunSummary:
    source_count: int
    success_count: int
    failed_count: int
    fetched: int
    inserted: int
    skipped: int
    finished_at: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send a human-readable WeCom digest for the latest full run.")
    parser.add_argument("--config", default="config/sources.real.sample.json", help="Path to config JSON")
    parser.add_argument("--preview", action="store_true", help="Print the markdown instead of sending")
    return parser


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _escape_wecom(value: str) -> str:
    return (value or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _truncate(value: str, limit: int = 44) -> str:
    text = " ".join((value or "").replace("\r", " ").replace("\n", " ").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _post_json(url: str, payload: dict) -> None:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=20):
        return


def _latest_source_rows(conn: sqlite3.Connection, source_names: list[str]) -> list[tuple]:
    if not source_names:
        return []
    placeholders = ",".join("?" for _ in source_names)
    return conn.execute(
        f"""
        SELECT source_name, status, fetched_count, inserted_count, skipped_count, duration_ms, created_at
        FROM source_runs
        WHERE id IN (
            SELECT MAX(id) FROM source_runs WHERE source_name IN ({placeholders}) GROUP BY source_name
        )
        ORDER BY inserted_count DESC, fetched_count DESC, source_name ASC
        """,
        source_names,
    ).fetchall()


def _build_run_summary(rows: list[tuple], finished_at: str) -> RunSummary:
    source_count = len(rows)
    success_count = sum(1 for row in rows if str(row[1]) == "success")
    failed_count = sum(1 for row in rows if str(row[1]) != "success")
    fetched = sum(int(row[2] or 0) for row in rows)
    inserted = sum(int(row[3] or 0) for row in rows)
    skipped = sum(int(row[4] or 0) for row in rows)
    return RunSummary(
        source_count=source_count,
        success_count=success_count,
        failed_count=failed_count,
        fetched=fetched,
        inserted=inserted,
        skipped=skipped,
        finished_at=finished_at,
    )


def _top_insert_sources(rows: list[tuple], limit: int = 6) -> list[tuple[str, int, int, int]]:
    items: list[tuple[str, int, int, int]] = []
    for source_name, _, fetched_count, inserted_count, skipped_count, _, _ in rows:
        inserted = int(inserted_count or 0)
        if inserted <= 0:
            continue
        items.append((str(source_name), inserted, int(fetched_count or 0), int(skipped_count or 0)))
    return items[:limit]


def _unique_top_leads(rows: list[dict[str, str]], limit: int = 5) -> list[dict[str, str]]:
    seen: set[str] = set()
    unique: list[dict[str, str]] = []
    for row in rows:
        normalized_url = row.get("url", "").strip()
        normalized_title = row.get("opportunity_title", "").strip().lower()
        key = normalized_url or normalized_title
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
        if len(unique) >= limit:
            break
    return unique


def build_digest_markdown(
    run_summary: RunSummary,
    export_rows: int,
    top_insert_sources: list[tuple[str, int, int, int]],
    theme_rows: list[dict[str, str]],
    direction_rows: list[dict[str, str]],
    top_leads: list[dict[str, str]],
) -> str:
    lines = [
        "# Demand Radar 全量运行日报",
        f"<font color=\"comment\">运行完成时间：{_escape_wecom(run_summary.finished_at)}</font>",
        "",
        "## 1. 总览",
        f"<font color=\"info\">来源成功 {run_summary.success_count}/{run_summary.source_count}</font>  "
        f"<font color=\"warning\">失败 {run_summary.failed_count}</font>",
        f"抓取 {run_summary.fetched} 条 | 新增 {run_summary.inserted} 条 | 跳过 {run_summary.skipped} 条 | 导出 {export_rows} 条",
        "",
        "## 2. 本轮新增贡献最大的来源",
    ]

    if top_insert_sources:
        for index, (source_name, inserted, fetched, skipped) in enumerate(top_insert_sources, start=1):
            lines.append(
                f"{index}. {_escape_wecom(source_name)} | 新增 {inserted} | 抓取 {fetched} | 跳过 {skipped}"
            )
    else:
        lines.append("<font color=\"comment\">本轮没有新增来源</font>")

    lines.extend(["", "## 3. 当前机会主题 Top 3"])
    for index, row in enumerate(theme_rows[:3], start=1):
        theme = _escape_wecom(row.get("theme", ""))
        lead_count = row.get("lead_count", "0")
        total_score = row.get("total_score", "0")
        max_score = row.get("max_score", "0")
        lines.append(
            f"{index}. <font color=\"warning\">{theme}</font> | 线索 {lead_count} | 总分 {total_score} | 最高 {max_score}"
        )

    lines.extend(["", "## 4. 建议优先跟进的产品方向"])
    for index, row in enumerate(direction_rows[:3], start=1):
        direction = _escape_wecom(row.get("direction", ""))
        offer = _escape_wecom(_truncate(row.get("core_offer", ""), 54))
        pricing = _escape_wecom(row.get("pricing_range", ""))
        lines.append(f"{index}. <font color=\"info\">{direction}</font>")
        if offer:
            lines.append(offer)
        if pricing:
            lines.append(f"<font color=\"comment\">定价建议：{pricing}</font>")

    lines.extend(["", "## 5. 重点线索 Top 5"])
    for index, row in enumerate(top_leads, start=1):
        title = _escape_wecom(_truncate(row.get("opportunity_title", ""), 48))
        source_name = _escape_wecom(row.get("source_name", ""))
        score = row.get("score", "0")
        strength = row.get("opportunity_strength", "0")
        price_text = _escape_wecom(row.get("price_text", "") or "待沟通")
        url = row.get("url", "")
        lines.append(
            f"{index}. <font color=\"warning\">{score}分</font> / 强度 {strength} / {_escape_wecom(title)}"
        )
        lines.append(f"{source_name} | 价格 {price_text}")
        if url:
            lines.append(f"[查看详情]({url})")
        lines.append("")

    lines.extend(
        [
            "## 6. 备注",
            "<font color=\"comment\">这条日报是基于最新一次全量运行结果汇总的人工可读版本，不等同于默认线索播报。</font>",
        ]
    )
    return "\n".join(lines).rstrip()


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config)
    base_dir = Path(config["_base_dir"])
    notifications = config.get("notifications", {})
    wecom = notifications.get("wecom", {})
    webhook_url = str(wecom.get("webhook_url") or "").strip()
    if not webhook_url and not args.preview:
        raise SystemExit("WeCom webhook is empty in the resolved config.")

    db_path = resolve_path(base_dir, config["database_path"])
    export_path = resolve_path(base_dir, config.get("export_path", "exports/leads.real.csv"))
    top_leads_path = resolve_path(base_dir, config.get("top_leads_export_path", "exports/top20_leads.csv"))
    theme_path = resolve_path(base_dir, config.get("theme_export_path", "exports/opportunity_themes.csv"))
    direction_path = resolve_path(base_dir, config.get("product_directions_export_path", "exports/product_directions.csv"))
    log_path = resolve_path(base_dir, config.get("log_path", "logs/demand_radar.log"))
    source_names = [str(source.get("name") or "").strip() for source in config.get("sources", []) if source.get("name")]
    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    if log_path.exists():
        finished_at = datetime.fromtimestamp(log_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(db_path)
    latest_rows = _latest_source_rows(conn, source_names)
    conn.close()

    run_summary = _build_run_summary(latest_rows, finished_at)
    export_rows = len(_read_csv_rows(export_path))
    top_insert_sources = _top_insert_sources(latest_rows)
    theme_rows = _read_csv_rows(theme_path)
    direction_rows = _read_csv_rows(direction_path)
    top_leads = _unique_top_leads(_read_csv_rows(top_leads_path))

    markdown = build_digest_markdown(
        run_summary=run_summary,
        export_rows=export_rows,
        top_insert_sources=top_insert_sources,
        theme_rows=theme_rows,
        direction_rows=direction_rows,
        top_leads=top_leads,
    )

    if args.preview:
        sys.stdout.buffer.write(markdown.encode("utf-8", errors="ignore"))
        sys.stdout.buffer.write(b"\n")
        return

    _post_json(webhook_url, {"msgtype": "markdown", "markdown": {"content": markdown}})
    print("run_digest_sent=1")


if __name__ == "__main__":
    main()
