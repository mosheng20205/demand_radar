from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta

from radar.analysis import (
    build_product_directions,
    build_theme_leaderboard,
    export_product_directions,
    export_theme_leaderboard,
    export_top_leads,
    select_top_leads,
)
from radar.fetchers import fetch_source
from radar.notify import send_daily_summary, send_failure_alert, send_notifications
from radar.scoring import score_lead
from radar.storage import (
    ensure_database,
    export_csv,
    export_source_health_csv,
    get_daily_report_stats,
    get_report_state,
    get_source_health,
    load_leads,
    mark_failure_alert_sent,
    record_source_run,
    set_report_state,
    should_skip_for_cooldown,
    upsert_leads,
)


logger = logging.getLogger(__name__)


def _cooldown_settings(config: dict) -> dict:
    return config.get(
        "failure_control",
        {
            "alert_threshold": 3,
            "cooldown_minutes": 180,
            "backoff_multiplier": 1.5,
        },
    )


def _should_notify_lead(lead, config: dict) -> bool:
    if lead.category == "demand_market":
        return True

    notification_filters = config.get("notification_filters", {})
    zbj_service_min_sales = int(notification_filters.get("zbj_service_min_sales", 20) or 20)

    if lead.category == "service_titles" and str(lead.source_name or "").startswith("zbj_search_"):
        total_sales = max(int(lead.cumulative_sale_count or 0), int(lead.sale_count or 0))
        return total_sales >= zbj_service_min_sales

    return True


def run_pipeline(config: dict, export_path: str | None = None) -> dict[str, int]:
    base_dir = config["_base_dir"]
    db_path = config["database_path"]
    min_score = int(config.get("min_score", 0))
    conn = ensure_database(f"{base_dir}/{db_path}")

    fetched_total = 0
    inserted_total = 0
    skipped_total = 0
    inserted_leads_all = []
    source_errors = 0
    failure_alerts_sent = 0

    for source in config.get("sources", []):
        started = time.perf_counter()
        fetched_count = 0
        inserted_count = 0
        skipped_count = 0
        now = datetime.now()
        skip_for_cooldown, cooldown_until = should_skip_for_cooldown(conn, source["name"], now)
        if skip_for_cooldown:
            record_source_run(
                conn,
                source_name=source["name"],
                status="cooldown",
                fetched_count=0,
                inserted_count=0,
                skipped_count=0,
                error_message="cooldown active",
                duration_ms=0,
                count_run=False,
                cooldown_until=cooldown_until,
            )
            logger.warning("source=%s status=cooldown cooldown_until=%s", source["name"], cooldown_until)
            continue

        try:
            leads = fetch_source(base_dir, source, config)
            fetched_count = len(leads)
            fetched_total += fetched_count
            scored_leads = [score_lead(lead, config) for lead in leads]
            inserted_count, skipped_count, inserted_leads = upsert_leads(conn, scored_leads)
            inserted_total += inserted_count
            skipped_total += skipped_count
            inserted_leads_all.extend(inserted_leads)
            duration_ms = int((time.perf_counter() - started) * 1000)
            record_source_run(
                conn,
                source_name=source["name"],
                status="success",
                fetched_count=fetched_count,
                inserted_count=inserted_count,
                skipped_count=skipped_count,
                error_message="",
                duration_ms=duration_ms,
            )
            logger.info(
                "source=%s status=success fetched=%s inserted=%s skipped=%s duration_ms=%s",
                source["name"],
                fetched_count,
                inserted_count,
                skipped_count,
                duration_ms,
            )
        except Exception as exc:
            source_errors += 1
            duration_ms = int((time.perf_counter() - started) * 1000)
            previous_health = get_source_health(conn, source["name"]) or {}
            previous_consecutive = int(previous_health.get("consecutive_failures", 0) or 0)
            failure_settings = _cooldown_settings(config)
            next_consecutive = previous_consecutive + 1
            alert_threshold = int(failure_settings.get("alert_threshold", 3))
            cooldown_minutes = int(failure_settings.get("cooldown_minutes", 180))
            backoff_multiplier = float(failure_settings.get("backoff_multiplier", 1.5))
            extra_failures = max(0, next_consecutive - alert_threshold)
            cooldown_multiplier = max(1.0, backoff_multiplier**extra_failures)
            cooldown_until_value = ""
            if next_consecutive >= alert_threshold:
                cooldown_until_value = (now + timedelta(minutes=int(cooldown_minutes * cooldown_multiplier))).isoformat()
            record_source_run(
                conn,
                source_name=source["name"],
                status="failed",
                fetched_count=fetched_count,
                inserted_count=inserted_count,
                skipped_count=skipped_count,
                error_message=str(exc),
                duration_ms=duration_ms,
                cooldown_until=cooldown_until_value,
            )
            logger.exception("source=%s status=failed duration_ms=%s", source["name"], duration_ms)
            health = get_source_health(conn, source["name"]) or {}
            consecutive_failures = int(health.get("consecutive_failures", 0) or 0)
            last_alert_streak = int(health.get("last_failure_alert_streak", 0) or 0)
            if consecutive_failures >= alert_threshold and consecutive_failures > last_alert_streak:
                failure_alerts_sent += send_failure_alert(
                    config,
                    source["name"],
                    consecutive_failures,
                    str(exc),
                    cooldown_until_value,
                )
                mark_failure_alert_sent(conn, source["name"], consecutive_failures)

    output_path = export_path or config.get("export_path")
    exported = export_csv(conn, f"{base_dir}/{output_path}", min_score) if output_path else 0
    health_export_path = config.get("health_export_path", "exports/source_health.csv")
    health_exported = export_source_health_csv(conn, f"{base_dir}/{health_export_path}")
    current_leads = load_leads(conn, min_score=min_score)
    top_leads_path = config.get("top_leads_export_path", "exports/top20_leads.csv")
    top_leads = select_top_leads(current_leads, limit=int(config.get("top_leads_limit", 20)))
    top_leads_exported = export_top_leads(f"{base_dir}/{top_leads_path}", top_leads)
    theme_rules = config.get("theme_rules")
    theme_leaderboard = build_theme_leaderboard(current_leads, theme_rules)
    theme_export_path = config.get("theme_export_path", "exports/opportunity_themes.csv")
    theme_exported = export_theme_leaderboard(f"{base_dir}/{theme_export_path}", theme_leaderboard)
    product_directions = build_product_directions(top_leads, limit=int(config.get("product_directions_limit", 5)))
    product_export_path = config.get("product_directions_export_path", "exports/product_directions.csv")
    product_exported = export_product_directions(f"{base_dir}/{product_export_path}", product_directions)
    notify_min_score = int(config.get("notify_min_score", min_score))
    leads_to_notify = [
        lead
        for lead in inserted_leads_all
        if lead.score >= notify_min_score and _should_notify_lead(lead, config)
    ]
    notifications_sent = send_notifications(config, leads_to_notify) if leads_to_notify else 0
    daily_summary_sent = 0
    daily_summary_config = config.get("daily_summary", {})
    if daily_summary_config.get("enabled", True):
        summary_hour = int(daily_summary_config.get("hour_local", 21))
        today_key = datetime.now().strftime("%Y-%m-%d")
        last_sent = get_report_state(conn, "daily_summary_last_sent")
        if datetime.now().hour >= summary_hour and last_sent != today_key:
            stats = get_daily_report_stats(conn)
            daily_summary_sent = send_daily_summary(config, stats)
            set_report_state(conn, "daily_summary_last_sent", today_key)
    conn.close()

    return {
        "fetched": fetched_total,
        "inserted": inserted_total,
        "skipped": skipped_total,
        "exported": exported,
        "health_exported": health_exported,
        "top_leads_exported": top_leads_exported,
        "theme_exported": theme_exported,
        "product_exported": product_exported,
        "notified_leads": len(leads_to_notify),
        "notifications_sent": notifications_sent,
        "failure_alerts_sent": failure_alerts_sent,
        "daily_summary_sent": daily_summary_sent,
        "source_errors": source_errors,
    }
