from __future__ import annotations

from collections import Counter, defaultdict
import json
import smtplib
import ssl
import urllib.request
from email.message import EmailMessage

from radar.models import Lead


def _truncate(value: str, limit: int) -> str:
    text = " ".join((value or "").replace("\r", " ").replace("\n", " ").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _escape_wecom(value: str) -> str:
    return (value or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _lead_summary(lead: Lead, *, limit: int = 72) -> str:
    if lead.content:
        return _truncate(lead.content, limit)
    return ""


def _source_summary(leads: list[Lead]) -> str:
    counter = Counter(lead.source_name for lead in leads)
    parts = [f"{name}:{count}" for name, count in counter.most_common(5)]
    return " | ".join(parts)


def _top_leads(leads: list[Lead], limit: int) -> list[Lead]:
    return sorted(leads, key=lambda item: (item.score, item.opportunity_strength), reverse=True)[:limit]


def _lead_meta_parts(lead: Lead) -> list[str]:
    parts = [f"{lead.priority} / {lead.score}分", lead.source_name]
    if lead.price_text:
        parts.append(f"价格 {lead.price_text}")
    if lead.delivery_text:
        parts.append(f"交付 {lead.delivery_text}")
    if lead.matched_rules:
        parts.append(f"命中 {', '.join(lead.matched_rules[:3])}")
    return parts


def _lead_brief_parts(lead: Lead) -> list[str]:
    parts = [lead.source_name]
    if lead.price_text:
        parts.append(f"价格 {lead.price_text}")
    if lead.delivery_text:
        parts.append(f"交付 {lead.delivery_text}")
    return parts


def _format_lines(leads: list[Lead]) -> list[str]:
    lines: list[str] = []
    ranked = _top_leads(leads, 8)
    for index, lead in enumerate(ranked, start=1):
        lines.append(f"{index}. [{lead.priority}] {lead.title}")
        lines.append(f"   score={lead.score} source={lead.source_name} url={lead.url}")
        if lead.price_text:
            lines.append(f"   price={lead.price_text}")
        if lead.matched_rules:
            lines.append(f"   rules={', '.join(lead.matched_rules[:5])}")
        summary = _lead_summary(lead, limit=160)
        if summary:
            lines.append(f"   {summary}")
    if len(leads) > len(ranked):
        lines.append(f"   ... 其余 {len(leads) - len(ranked)} 条请查看导出结果")
    return lines


def build_message(leads: list[Lead]) -> str:
    if not leads:
        return "Demand Radar 本次没有新的高分线索。"

    top_score = max(lead.score for lead in leads)
    lines = [
        "Demand Radar 发现新的高分线索",
        f"新增条数: {len(leads)}",
        f"最高分: {top_score}",
        f"来源分布: {_source_summary(leads)}",
        "",
        "重点线索:",
    ]
    lines.extend(_format_lines(leads))
    return "\n".join(lines)


def build_failure_alert(source_name: str, consecutive_failures: int, error_message: str, cooldown_until: str) -> str:
    lines = [
        "Demand Radar 来源失败告警",
        f"来源: {source_name}",
        f"连续失败: {consecutive_failures}",
        f"冷却到: {cooldown_until or '未设置'}",
        f"最近错误: {_truncate(error_message or '无', 300)}",
    ]
    return "\n".join(lines)


def _group_source_runs(source_runs: list[tuple[str, str, int]]) -> list[str]:
    grouped: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for source_name, status, count in source_runs:
        grouped[str(source_name)][str(status)] += int(count or 0)

    lines: list[str] = []
    for source_name in sorted(grouped):
        parts = [f"{status}:{grouped[source_name][status]}" for status in sorted(grouped[source_name])]
        lines.append(f"- {source_name} {' | '.join(parts)}")
    return lines


def _status_overview(source_runs: list[tuple[str, str, int]]) -> str:
    totals: dict[str, int] = defaultdict(int)
    for _, status, count in source_runs:
        totals[str(status)] += int(count or 0)
    if not totals:
        return "暂无运行记录"
    parts = [f"{status} {totals[status]}" for status in sorted(totals)]
    return " | ".join(parts)


def _format_wecom_source_runs(source_runs: list[tuple[str, str, int]]) -> list[str]:
    grouped: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for source_name, status, count in source_runs:
        grouped[str(source_name)][str(status)] += int(count or 0)

    lines: list[str] = []
    for index, source_name in enumerate(sorted(grouped), start=1):
        parts = [f"{status} {grouped[source_name][status]}" for status in sorted(grouped[source_name])]
        lines.append(f"{index}. {_escape_wecom(source_name)} | {_escape_wecom(' | '.join(parts))}")
    return lines


def build_daily_summary_message(stats: dict) -> str:
    lines = [
        "Demand Radar 每日汇总",
        f"过去24小时新增线索: {stats.get('new_leads', 0)}",
        f"过去24小时最高分: {stats.get('max_score', 0)}",
        "",
        "来源运行统计:",
    ]
    lines.extend(_group_source_runs(stats.get("source_runs", [])))
    return "\n".join(lines)


def build_wecom_markdown(leads: list[Lead]) -> str:
    if not leads:
        return "\n".join(
            [
                "# Demand Radar 线索播报",
                "<font color=\"comment\">本次没有新的高分线索</font>",
            ]
        )

    top_score = max(lead.score for lead in leads)
    ranked = _top_leads(leads, 4)
    lines = [
        "# Demand Radar 线索播报",
        f"<font color=\"info\">新增 {len(leads)} 条</font>  <font color=\"warning\">最高 {top_score} 分</font>",
        f"<font color=\"comment\">来源：{_escape_wecom(_source_summary(leads))}</font>",
        "",
        "## 重点线索",
    ]

    for index, lead in enumerate(ranked, start=1):
        title = _escape_wecom(_truncate(lead.title, 48))
        brief = _escape_wecom(" | ".join(_lead_brief_parts(lead)))
        rules = _escape_wecom(", ".join(lead.matched_rules[:2]))
        lines.append(f"{index}. <font color=\"warning\">{lead.priority}</font> <font color=\"info\">{lead.score}分</font> {title}")
        if brief:
            lines.append(brief)
        if rules:
            lines.append(f"<font color=\"comment\">命中：{rules}</font>")
        summary = _lead_summary(lead, limit=88)
        if summary:
            lines.append(_escape_wecom(summary))
        lines.append(f"[查看详情]({lead.url})")
        lines.append("")

    if len(leads) > len(ranked):
        lines.append(f"<font color=\"comment\">其余 {len(leads) - len(ranked)} 条请查看导出结果</font>")
    return "\n".join(lines).rstrip()


def build_wecom_failure_alert(source_name: str, consecutive_failures: int, error_message: str, cooldown_until: str) -> str:
    error_text = _escape_wecom(_truncate(error_message or "无", 220))
    return "\n".join(
        [
            "# Demand Radar 失败告警",
            f"<font color=\"warning\">来源：{_escape_wecom(source_name)}</font>",
            f"<font color=\"warning\">连续失败 {consecutive_failures} 次</font>",
            f"<font color=\"comment\">冷却到：{_escape_wecom(cooldown_until or '未设置')}</font>",
            "",
            "## 错误摘要",
            error_text,
        ]
    )


def build_wecom_daily_summary(stats: dict) -> str:
    source_runs = stats.get("source_runs", [])
    lines = [
        "# Demand Radar 每日汇总",
        f"<font color=\"info\">24h 新增线索：{stats.get('new_leads', 0)}</font>",
        f"<font color=\"warning\">24h 最高分：{stats.get('max_score', 0)}</font>",
        f"<font color=\"comment\">运行概览：{_escape_wecom(_status_overview(source_runs))}</font>",
        "",
        "## 来源运行",
    ]
    source_lines = _format_wecom_source_runs(source_runs)
    if source_lines:
        lines.extend(source_lines[:12])
    else:
        lines.append("<font color=\"comment\">今日暂无来源运行记录</font>")
    return "\n".join(lines)


def build_wecom_payload(leads: list[Lead]) -> dict:
    return {"msgtype": "markdown", "markdown": {"content": build_wecom_markdown(leads)}}


def build_feishu_payload(leads: list[Lead]) -> dict:
    return {"msg_type": "text", "content": {"text": build_message(leads)}}


def _post_json(url: str, payload: dict) -> None:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=20):
        return


def _send_text_via_channels(config: dict, subject: str, body: str, *, wecom_markdown: str | None = None) -> int:
    notifications = config.get("notifications", {})
    count = 0

    wecom = notifications.get("wecom", {})
    if wecom.get("enabled") and wecom.get("webhook_url"):
        if wecom_markdown:
            _post_json(wecom["webhook_url"], {"msgtype": "markdown", "markdown": {"content": wecom_markdown}})
        else:
            _post_json(wecom["webhook_url"], {"msgtype": "text", "text": {"content": f"{subject}\n\n{body}"}})
        count += 1

    feishu = notifications.get("feishu", {})
    if feishu.get("enabled") and feishu.get("webhook_url"):
        _post_json(feishu["webhook_url"], {"msg_type": "text", "content": {"text": f"{subject}\n\n{body}"}})
        count += 1

    email = notifications.get("email", {})
    if email.get("enabled") and email.get("host") and email.get("to_addrs"):
        send_email_text(email, subject, body)
        count += 1

    return count


def send_wecom(webhook_url: str, leads: list[Lead]) -> None:
    if webhook_url:
        _post_json(webhook_url, build_wecom_payload(leads))


def send_feishu(webhook_url: str, leads: list[Lead]) -> None:
    if webhook_url:
        _post_json(webhook_url, build_feishu_payload(leads))


def send_email(email_config: dict, leads: list[Lead]) -> None:
    send_email_text(email_config, f"Demand Radar 新线索 {len(leads)} 条", build_message(leads))


def send_email_text(email_config: dict, subject: str, body: str) -> None:
    if not email_config.get("host") or not email_config.get("to_addrs"):
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = email_config["from_addr"]
    msg["To"] = ", ".join(email_config["to_addrs"])
    msg.set_content(body)

    use_ssl = bool(email_config.get("use_ssl", True))
    host = email_config["host"]
    port = int(email_config.get("port", 465 if use_ssl else 587))
    username = email_config.get("username", "")
    password = email_config.get("password", "")

    if use_ssl:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(host, port, context=context, timeout=20) as server:
            if username:
                server.login(username, password)
            server.send_message(msg)
    else:
        with smtplib.SMTP(host, port, timeout=20) as server:
            server.starttls(context=ssl.create_default_context())
            if username:
                server.login(username, password)
            server.send_message(msg)


def send_notifications(config: dict, leads: list[Lead]) -> int:
    notifications = config.get("notifications", {})
    count = 0

    wecom = notifications.get("wecom", {})
    if wecom.get("enabled") and wecom.get("webhook_url"):
        send_wecom(wecom["webhook_url"], leads)
        count += 1

    feishu = notifications.get("feishu", {})
    if feishu.get("enabled") and feishu.get("webhook_url"):
        send_feishu(feishu["webhook_url"], leads)
        count += 1

    email = notifications.get("email", {})
    if email.get("enabled") and email.get("host") and email.get("to_addrs"):
        send_email(email, leads)
        count += 1

    return count


def send_failure_alert(config: dict, source_name: str, consecutive_failures: int, error_message: str, cooldown_until: str) -> int:
    body = build_failure_alert(source_name, consecutive_failures, error_message, cooldown_until)
    return _send_text_via_channels(
        config,
        "Demand Radar 失败告警",
        body,
        wecom_markdown=build_wecom_failure_alert(source_name, consecutive_failures, error_message, cooldown_until),
    )


def send_daily_summary(config: dict, stats: dict) -> int:
    body = build_daily_summary_message(stats)
    return _send_text_via_channels(
        config,
        "Demand Radar 每日汇总",
        body,
        wecom_markdown=build_wecom_daily_summary(stats),
    )
