from __future__ import annotations

import csv
import hashlib
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from radar.models import Lead


SCHEMA = """
CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    category TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    url TEXT NOT NULL,
    published_at TEXT NOT NULL,
    score INTEGER NOT NULL,
    opportunity_strength INTEGER NOT NULL DEFAULT 0,
    priority TEXT NOT NULL,
    price_text TEXT NOT NULL DEFAULT '',
    price_value REAL NOT NULL DEFAULT 0,
    sale_count INTEGER NOT NULL DEFAULT 0,
    cumulative_sale_count INTEGER NOT NULL DEFAULT 0,
    comment_count INTEGER NOT NULL DEFAULT 0,
    good_comment_count INTEGER NOT NULL DEFAULT 0,
    delivery_text TEXT NOT NULL DEFAULT '',
    matched_rules TEXT NOT NULL,
    fingerprint TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS source_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL,
    status TEXT NOT NULL,
    fetched_count INTEGER NOT NULL DEFAULT 0,
    inserted_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT NOT NULL DEFAULT '',
    duration_ms INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS source_health (
    source_name TEXT PRIMARY KEY,
    total_runs INTEGER NOT NULL DEFAULT 0,
    success_runs INTEGER NOT NULL DEFAULT 0,
    failure_runs INTEGER NOT NULL DEFAULT 0,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    total_fetched INTEGER NOT NULL DEFAULT 0,
    total_inserted INTEGER NOT NULL DEFAULT 0,
    last_status TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    cooldown_until TEXT NOT NULL DEFAULT '',
    last_failure_alert_streak INTEGER NOT NULL DEFAULT 0,
    last_duration_ms INTEGER NOT NULL DEFAULT 0,
    last_run_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS report_state (
    report_key TEXT PRIMARY KEY,
    report_value TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS notification_history (
    notification_type TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    PRIMARY KEY (notification_type, dedupe_key)
);
"""


def ensure_database(path: str | Path) -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    _migrate_schema(conn)
    conn.commit()
    return conn


def _migrate_schema(conn: sqlite3.Connection) -> None:
    lead_columns = {row[1] for row in conn.execute("PRAGMA table_info(leads)")}
    required_lead_columns = {
        "opportunity_strength": "ALTER TABLE leads ADD COLUMN opportunity_strength INTEGER NOT NULL DEFAULT 0",
        "price_text": "ALTER TABLE leads ADD COLUMN price_text TEXT NOT NULL DEFAULT ''",
        "price_value": "ALTER TABLE leads ADD COLUMN price_value REAL NOT NULL DEFAULT 0",
        "sale_count": "ALTER TABLE leads ADD COLUMN sale_count INTEGER NOT NULL DEFAULT 0",
        "cumulative_sale_count": "ALTER TABLE leads ADD COLUMN cumulative_sale_count INTEGER NOT NULL DEFAULT 0",
        "comment_count": "ALTER TABLE leads ADD COLUMN comment_count INTEGER NOT NULL DEFAULT 0",
        "good_comment_count": "ALTER TABLE leads ADD COLUMN good_comment_count INTEGER NOT NULL DEFAULT 0",
        "delivery_text": "ALTER TABLE leads ADD COLUMN delivery_text TEXT NOT NULL DEFAULT ''",
    }
    for column_name, statement in required_lead_columns.items():
        if column_name not in lead_columns:
            conn.execute(statement)

    existing_columns = {row[1] for row in conn.execute("PRAGMA table_info(source_health)")}
    required_columns = {
        "consecutive_failures": "ALTER TABLE source_health ADD COLUMN consecutive_failures INTEGER NOT NULL DEFAULT 0",
        "cooldown_until": "ALTER TABLE source_health ADD COLUMN cooldown_until TEXT NOT NULL DEFAULT ''",
        "last_failure_alert_streak": "ALTER TABLE source_health ADD COLUMN last_failure_alert_streak INTEGER NOT NULL DEFAULT 0",
    }
    for column_name, statement in required_columns.items():
        if column_name not in existing_columns:
            conn.execute(statement)


def _fingerprint(lead: Lead) -> str:
    return hashlib.sha256(lead.fingerprint_text.encode("utf-8")).hexdigest()


def upsert_leads(conn: sqlite3.Connection, leads: list[Lead]) -> tuple[int, int, list[Lead]]:
    inserted = 0
    skipped = 0
    inserted_leads: list[Lead] = []
    for lead in leads:
        try:
            conn.execute(
                """
                INSERT INTO leads (
                    source_name, source_kind, category, title, content, url, published_at,
                    score, opportunity_strength, priority, price_text, price_value, sale_count,
                    cumulative_sale_count, comment_count, good_comment_count, delivery_text,
                    matched_rules, fingerprint
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lead.source_name,
                    lead.source_kind,
                    lead.category,
                    lead.title,
                    lead.content,
                    lead.url,
                    lead.published_at,
                    lead.score,
                    lead.opportunity_strength,
                    lead.priority,
                    lead.price_text,
                    lead.price_value,
                    lead.sale_count,
                    lead.cumulative_sale_count,
                    lead.comment_count,
                    lead.good_comment_count,
                    lead.delivery_text,
                    ",".join(lead.matched_rules),
                    _fingerprint(lead),
                ),
            )
            inserted += 1
            inserted_leads.append(lead)
        except sqlite3.IntegrityError:
            skipped += 1
    conn.commit()
    return inserted, skipped, inserted_leads


def export_csv(conn: sqlite3.Connection, path: str | Path, min_score: int) -> int:
    export_path = Path(path)
    export_path.parent.mkdir(parents=True, exist_ok=True)
    rows = conn.execute(
        """
        SELECT source_name, category, title, content, url, published_at, score, opportunity_strength, priority,
               price_text, price_value, sale_count, cumulative_sale_count, comment_count, good_comment_count,
               delivery_text, matched_rules
        FROM leads
        WHERE score >= ?
        ORDER BY opportunity_strength DESC, score DESC, id DESC
        """,
        (min_score,),
    ).fetchall()
    with export_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "source_name",
                "category",
                "title",
                "content",
                "url",
                "published_at",
                "score",
                "opportunity_strength",
                "priority",
                "price_text",
                "price_value",
                "sale_count",
                "cumulative_sale_count",
                "comment_count",
                "good_comment_count",
                "delivery_text",
                "matched_rules",
            ]
        )
        writer.writerows(rows)
    return len(rows)


def load_leads(conn: sqlite3.Connection, min_score: int = 0) -> list[Lead]:
    rows = conn.execute(
        """
        SELECT source_name, source_kind, category, title, content, url, published_at, score, opportunity_strength,
               priority, price_text, price_value, sale_count, cumulative_sale_count, comment_count,
               good_comment_count, delivery_text, matched_rules
        FROM leads
        WHERE score >= ?
        ORDER BY opportunity_strength DESC, score DESC, id DESC
        """,
        (min_score,),
    ).fetchall()
    leads: list[Lead] = []
    for row in rows:
        matched_rules = [item for item in str(row[17]).split(",") if item]
        leads.append(
            Lead(
                source_name=row[0],
                source_kind=row[1],
                category=row[2],
                title=row[3],
                content=row[4],
                url=row[5],
                published_at=row[6],
                score=int(row[7]),
                opportunity_strength=int(row[8] or 0),
                priority=row[9],
                price_text=str(row[10] or ""),
                price_value=float(row[11] or 0.0),
                sale_count=int(row[12] or 0),
                cumulative_sale_count=int(row[13] or 0),
                comment_count=int(row[14] or 0),
                good_comment_count=int(row[15] or 0),
                delivery_text=str(row[16] or ""),
                matched_rules=matched_rules,
            )
        )
    return leads


def record_source_run(
    conn: sqlite3.Connection,
    *,
    source_name: str,
    status: str,
    fetched_count: int,
    inserted_count: int,
    skipped_count: int,
    error_message: str,
    duration_ms: int,
    count_run: bool = True,
    cooldown_until: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO source_runs (
            source_name, status, fetched_count, inserted_count, skipped_count, error_message, duration_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (source_name, status, fetched_count, inserted_count, skipped_count, error_message, duration_ms),
    )

    current = conn.execute(
        """
        SELECT total_runs, success_runs, failure_runs, consecutive_failures, total_fetched, total_inserted,
               last_failure_alert_streak
        FROM source_health WHERE source_name = ?
        """,
        (source_name,),
    ).fetchone()

    total_runs = 1 if count_run else 0
    success_runs = 1 if status == "success" else 0
    failure_runs = 1 if status == "failed" else 0
    consecutive_failures = 0 if status == "success" else 1 if status == "failed" else 0
    total_fetched = fetched_count
    total_inserted = inserted_count
    last_failure_alert_streak = 0
    if current:
        total_runs += int(current[0])
        success_runs += int(current[1])
        failure_runs += int(current[2])
        previous_consecutive_failures = int(current[3])
        total_fetched += int(current[4])
        total_inserted += int(current[5])
        last_failure_alert_streak = int(current[6])
        if status == "failed":
            consecutive_failures = previous_consecutive_failures + 1
        elif status == "cooldown":
            consecutive_failures = previous_consecutive_failures
            last_failure_alert_streak = int(current[6])
        elif status == "success":
            last_failure_alert_streak = 0

    conn.execute(
        """
        INSERT INTO source_health (
            source_name, total_runs, success_runs, failure_runs, consecutive_failures, total_fetched, total_inserted,
            last_status, last_error, cooldown_until, last_failure_alert_streak, last_duration_ms, last_run_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(source_name) DO UPDATE SET
            total_runs = excluded.total_runs,
            success_runs = excluded.success_runs,
            failure_runs = excluded.failure_runs,
            consecutive_failures = excluded.consecutive_failures,
            total_fetched = excluded.total_fetched,
            total_inserted = excluded.total_inserted,
            last_status = excluded.last_status,
            last_error = excluded.last_error,
            cooldown_until = excluded.cooldown_until,
            last_failure_alert_streak = excluded.last_failure_alert_streak,
            last_duration_ms = excluded.last_duration_ms,
            last_run_at = CURRENT_TIMESTAMP
        """,
        (
            source_name,
            total_runs,
            success_runs,
            failure_runs,
            consecutive_failures,
            total_fetched,
            total_inserted,
            status,
            error_message[:500],
            cooldown_until,
            last_failure_alert_streak,
            duration_ms,
        ),
    )
    conn.commit()


def get_source_health(conn: sqlite3.Connection, source_name: str) -> dict | None:
    row = conn.execute(
        """
        SELECT source_name, total_runs, success_runs, failure_runs, consecutive_failures, total_fetched, total_inserted,
               last_status, last_error, cooldown_until, last_failure_alert_streak, last_duration_ms, last_run_at
        FROM source_health WHERE source_name = ?
        """,
        (source_name,),
    ).fetchone()
    if not row:
        return None
    keys = [
        "source_name",
        "total_runs",
        "success_runs",
        "failure_runs",
        "consecutive_failures",
        "total_fetched",
        "total_inserted",
        "last_status",
        "last_error",
        "cooldown_until",
        "last_failure_alert_streak",
        "last_duration_ms",
        "last_run_at",
    ]
    return dict(zip(keys, row))


def mark_failure_alert_sent(conn: sqlite3.Connection, source_name: str, streak: int) -> None:
    conn.execute(
        "UPDATE source_health SET last_failure_alert_streak = ? WHERE source_name = ?",
        (streak, source_name),
    )
    conn.commit()


def should_skip_for_cooldown(conn: sqlite3.Connection, source_name: str, now: datetime) -> tuple[bool, str]:
    row = conn.execute("SELECT cooldown_until FROM source_health WHERE source_name = ?", (source_name,)).fetchone()
    if not row or not row[0]:
        return False, ""
    try:
        cooldown_until = datetime.fromisoformat(str(row[0]))
    except ValueError:
        return False, ""
    if cooldown_until > now:
        return True, cooldown_until.isoformat()
    return False, ""


def get_report_state(conn: sqlite3.Connection, report_key: str) -> str:
    row = conn.execute("SELECT report_value FROM report_state WHERE report_key = ?", (report_key,)).fetchone()
    return str(row[0]) if row else ""


def set_report_state(conn: sqlite3.Connection, report_key: str, report_value: str) -> None:
    conn.execute(
        """
        INSERT INTO report_state (report_key, report_value)
        VALUES (?, ?)
        ON CONFLICT(report_key) DO UPDATE SET report_value = excluded.report_value
        """,
        (report_key, report_value),
    )
    conn.commit()


def get_daily_report_stats(conn: sqlite3.Connection) -> dict:
    since_expr = "datetime('now', '-1 day')"
    lead_stats = conn.execute(
        f"""
        SELECT COUNT(*), COALESCE(MAX(score), 0)
        FROM leads
        WHERE created_at >= {since_expr}
        """
    ).fetchone()
    source_stats = conn.execute(
        f"""
        SELECT source_name, status, COUNT(*)
        FROM source_runs
        WHERE created_at >= {since_expr}
        GROUP BY source_name, status
        ORDER BY source_name ASC
        """
    ).fetchall()
    return {
        "new_leads": int(lead_stats[0] or 0),
        "max_score": int(lead_stats[1] or 0),
        "source_runs": source_stats,
    }


def export_source_health_csv(conn: sqlite3.Connection, path: str | Path) -> int:
    export_path = Path(path)
    export_path.parent.mkdir(parents=True, exist_ok=True)
    rows = conn.execute(
        """
        SELECT
            source_name, total_runs, success_runs, failure_runs, consecutive_failures, total_fetched, total_inserted,
            last_status, last_error, cooldown_until, last_duration_ms, last_run_at
        FROM source_health
        ORDER BY failure_runs DESC, total_runs DESC, source_name ASC
        """
    ).fetchall()
    with export_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "source_name",
                "total_runs",
                "success_runs",
                "failure_runs",
                "consecutive_failures",
                "success_rate",
                "total_fetched",
                "total_inserted",
                "last_status",
                "last_error",
                "cooldown_until",
                "last_duration_ms",
                "last_run_at",
            ]
        )
        for row in rows:
            total_runs = int(row[1]) or 1
            success_rate = round(int(row[2]) / total_runs, 4)
            writer.writerow(
                [row[0], row[1], row[2], row[3], row[4], success_rate, row[5], row[6], row[7], row[8], row[9], row[10], row[11]]
            )
    return len(rows)


def _parse_timestamp(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for candidate in (text, text.replace(" ", "T")):
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            continue
    return None


def get_recent_notification_keys(
    conn: sqlite3.Connection,
    notification_type: str,
    dedupe_keys: list[str],
    window_hours: int,
) -> set[str]:
    if not dedupe_keys or window_hours <= 0:
        return set()

    placeholders = ",".join("?" for _ in dedupe_keys)
    rows = conn.execute(
        f"""
        SELECT dedupe_key, sent_at
        FROM notification_history
        WHERE notification_type = ?
          AND dedupe_key IN ({placeholders})
        """,
        [notification_type, *dedupe_keys],
    ).fetchall()

    cutoff = datetime.now() - timedelta(hours=window_hours)
    recent_keys: set[str] = set()
    for dedupe_key, sent_at in rows:
        sent_at_value = _parse_timestamp(str(sent_at or ""))
        if sent_at_value and sent_at_value >= cutoff:
            recent_keys.add(str(dedupe_key))
    return recent_keys


def record_notification_sent(conn: sqlite3.Connection, notification_type: str, dedupe_keys: list[str]) -> None:
    unique_keys = sorted({key for key in dedupe_keys if key})
    if not unique_keys:
        return

    now_value = datetime.now().isoformat(timespec="seconds")
    for dedupe_key in unique_keys:
        conn.execute(
            """
            INSERT INTO notification_history (notification_type, dedupe_key, sent_at)
            VALUES (?, ?, ?)
            ON CONFLICT(notification_type, dedupe_key) DO UPDATE SET
                sent_at = excluded.sent_at
            """,
            (notification_type, dedupe_key, now_value),
        )
    conn.commit()
