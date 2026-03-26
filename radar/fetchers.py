from __future__ import annotations

import json
import re
import time
import urllib.request
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from radar.cdp_fetcher import fetch_page_via_cdp
from radar.cloak_client import build_cloak_client
from radar.config import resolve_path
from radar.models import Lead
from radar.site_fetchers import (
    fetch_ccgp_procurement_list,
    fetch_cnode_topics,
    fetch_xianyu_service_list,
    fetch_json_items,
    fetch_remoteok_jobs,
    fetch_sourceforge_reviews,
    fetch_v2ex_json_feed,
    fetch_v2ex_rss,
    fetch_v2ex_topics_html,
    fetch_zbj_content_hub,
    fetch_zbj_demand_detail,
    fetch_zbj_demand_hall,
    fetch_zbj_list,
    fetch_zbj_search_list,
    fetch_zbj_search_state,
    fetch_zbj_service_detail,
)


def _read_text(base_dir: str | Path, location: str, runtime: dict | None = None) -> str:
    runtime = runtime or {}
    retries = int(runtime.get("retries", 2))
    timeout = int(runtime.get("timeout_seconds", 20))
    backoff = float(runtime.get("backoff_seconds", 2))
    user_agent = runtime.get("user_agent", "DemandRadar/0.1")
    method = str(runtime.get("method") or "GET").strip().upper() or "GET"
    headers = dict(runtime.get("headers", {}))
    request_data: bytes | None = None

    if runtime.get("json_body") is not None:
        request_data = json.dumps(runtime["json_body"], ensure_ascii=False).encode("utf-8")
        headers.setdefault("Content-Type", "application/json")
    elif runtime.get("post_data") is not None:
        request_data = str(runtime.get("post_data") or "").encode("utf-8")
        headers.setdefault("Content-Type", "application/json")

    if location.startswith(("http://", "https://")):
        request_headers = {"User-Agent": user_agent}
        request_headers.update({str(key): str(value) for key, value in headers.items() if value})
        request = urllib.request.Request(
            location,
            data=request_data,
            headers=request_headers,
            method=method,
        )
        last_error: Exception | None = None
        for attempt in range(retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    return response.read().decode("utf-8", errors="ignore")
            except Exception as exc:
                last_error = exc
                if attempt >= retries:
                    break
                time.sleep(backoff * (attempt + 1))
        if last_error:
            raise last_error
    path = resolve_path(base_dir, location)
    return path.read_text(encoding="utf-8")


def _runtime_config(config: dict | None, source: dict) -> dict:
    runtime = {}
    if config:
        runtime.update(config.get("fetch", {}))
    runtime.update(source.get("fetch", {}))
    runtime_headers = dict(config.get("fetch", {}).get("headers", {})) if config else {}
    runtime_headers.update(source.get("headers", {}))
    if runtime_headers:
        runtime["headers"] = runtime_headers
    return runtime


def _cloak_runtime_config(config: dict | None, source: dict) -> dict:
    runtime = {}
    if config:
        runtime.update(config.get("cloak", {}))
    runtime.update(source.get("cloak", {}))
    return runtime


def _source_attempts(source: dict) -> list[dict]:
    attempts = [source]
    for fallback in source.get("fallbacks", []):
        candidate = dict(source)
        candidate.update({key: value for key, value in fallback.items() if key != "fetch"})
        merged_fetch = dict(source.get("fetch", {}))
        merged_fetch.update(fallback.get("fetch", {}))
        if merged_fetch:
            candidate["fetch"] = merged_fetch
        candidate["fallbacks"] = []
        candidate["name"] = source["name"]
        candidate["category"] = source["category"]
        attempts.append(candidate)
    return attempts


def _with_page_param(location: str, page_param: str, page_value: int | str) -> str:
    if not location.startswith(("http://", "https://")):
        return location
    split_result = urlsplit(location)
    query = dict(parse_qsl(split_result.query, keep_blank_values=True))
    query[page_param] = str(page_value)
    return urlunsplit(
        (
            split_result.scheme,
            split_result.netloc,
            split_result.path,
            urlencode(query),
            split_result.fragment,
        )
    )


def _paginated_attempts(source: dict) -> list[dict]:
    pagination = source.get("pagination") or {}
    if not isinstance(pagination, dict) or not pagination:
        return [source]

    page_numbers = pagination.get("page_numbers")
    if not page_numbers:
        start = int(pagination.get("start_page", 1) or 1)
        end = int(pagination.get("end_page", start) or start)
        page_numbers = list(range(start, end + 1))
    if not isinstance(page_numbers, list) or not page_numbers:
        return [source]

    page_param = str(pagination.get("page_param") or "pageNum")
    location_template = str(pagination.get("location_template") or "").strip()
    default_url_template = str(pagination.get("default_url_template") or "").strip()
    attempts: list[dict] = []
    for page_number in page_numbers:
        candidate = dict(source)
        candidate["page_number"] = page_number
        if location_template:
            candidate["location"] = location_template.format(page=page_number)
        else:
            candidate["location"] = _with_page_param(str(source["location"]), page_param, page_number)
        if default_url_template:
            candidate["default_url"] = default_url_template.format(page=page_number)
        elif source.get("default_url"):
            candidate["default_url"] = _with_page_param(str(source["default_url"]), page_param, page_number)
        attempts.append(candidate)
    return attempts


def _merge_detail_into_lead(lead: Lead, detail: Lead) -> Lead:
    if detail.content and len(detail.content) > len(lead.content):
        lead.content = detail.content
    if detail.published_at:
        lead.published_at = detail.published_at
    if detail.price_text:
        lead.price_text = detail.price_text
    if detail.price_value:
        lead.price_value = detail.price_value
    if detail.sale_count:
        lead.sale_count = detail.sale_count
    if detail.cumulative_sale_count:
        lead.cumulative_sale_count = detail.cumulative_sale_count
    if detail.comment_count:
        lead.comment_count = detail.comment_count
    if detail.good_comment_count:
        lead.good_comment_count = detail.good_comment_count
    if detail.delivery_text:
        lead.delivery_text = detail.delivery_text
    return lead


def _enrich_with_detail(
    leads: list[Lead],
    base_dir: str | Path,
    source: dict,
    config: dict | None = None,
) -> list[Lead]:
    detail_fetch = source.get("detail_fetch") or {}
    if not isinstance(detail_fetch, dict) or not detail_fetch.get("enabled", False):
        return leads

    max_items = int(detail_fetch.get("max_items", len(leads)) or len(leads))
    enriched: list[Lead] = []
    for index, lead in enumerate(leads):
        if index >= max_items or not lead.url.startswith(("http://", "https://")):
            enriched.append(lead)
            continue
        detail_source = dict(detail_fetch)
        detail_source.pop("enabled", None)
        detail_source["name"] = source["name"]
        detail_source["category"] = source["category"]
        detail_source["location"] = lead.url
        detail_source["default_url"] = lead.url
        detail_source.pop("detail_fetch", None)
        try:
            detail_leads = fetch_source(base_dir, detail_source, config)
        except Exception:
            enriched.append(lead)
            continue
        if detail_leads:
            enriched.append(_merge_detail_into_lead(lead, detail_leads[0]))
        else:
            enriched.append(lead)
    return enriched


def _dedupe_leads(leads: list[Lead]) -> list[Lead]:
    seen: set[str] = set()
    deduped: list[Lead] = []
    for lead in leads:
        fingerprint = lead.fingerprint_text
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        deduped.append(lead)
    return deduped


def _lead_fingerprints(leads: list[Lead]) -> set[str]:
    return {lead.fingerprint_text for lead in leads}


def _page_overlap_ratio(leads: list[Lead], seen_fingerprints: set[str]) -> float:
    page_fingerprints = _lead_fingerprints(leads)
    if not page_fingerprints or not seen_fingerprints:
        return 0.0
    return len(page_fingerprints & seen_fingerprints) / len(page_fingerprints)


def _find_named_source(config: dict | None, source_name: str) -> dict | None:
    if not config or not source_name:
        return None
    for item in config.get("sources", []):
        if str(item.get("name") or "").strip() == source_name:
            return dict(item)
    return None


def _normalize_video_url(url: str) -> str:
    value = str(url or "").strip()
    if not value:
        return ""
    split_result = urlsplit(value)
    query = [(key, item) for key, item in parse_qsl(split_result.query, keep_blank_values=True) if key != "spm_id_from"]
    return urlunsplit((split_result.scheme, split_result.netloc, split_result.path, urlencode(query), split_result.fragment))


def _extract_xiaohongshu_note_id(url: str) -> str:
    value = str(url or "").strip()
    if not value:
        return ""
    path = urlsplit(value).path.strip("/")
    if not path:
        return ""
    return path.rsplit("/", 1)[-1]


def _normalize_xiaohongshu_note_url(url: str) -> str:
    note_id = _extract_xiaohongshu_note_id(url)
    if note_id:
        return f"https://www.xiaohongshu.com/explore/{note_id}"
    return str(url or "").strip()


def _prepare_xiaohongshu_probe_url(url: str) -> str:
    value = str(url or "").strip()
    if not value:
        return ""
    if value.startswith("/"):
        return f"https://www.xiaohongshu.com{value}"
    split_result = urlsplit(value)
    if split_result.scheme and split_result.netloc:
        return value
    return _normalize_xiaohongshu_note_url(value)


def _format_unix_time_millis(value: object) -> str:
    try:
        timestamp = int(value or 0)
    except (TypeError, ValueError):
        return ""
    if timestamp <= 0:
        return ""
    if timestamp > 10_000_000_000:
        timestamp //= 1000
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))


def _build_xiaohongshu_note_detail_url(note_id: str, xsec_token: str = "") -> str:
    normalized_url = f"https://www.xiaohongshu.com/search_result/{note_id}"
    if not xsec_token:
        return normalized_url
    query = urlencode(
        {
            "xsec_token": xsec_token,
            "xsec_source": "",
        }
    )
    return f"{normalized_url}?{query}"


def _parse_xiaohongshu_search_note_metadata(network_log_path: Path) -> dict[str, dict[str, object]]:
    if not network_log_path.exists():
        return {}
    try:
        payload = json.loads(network_log_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, list):
        return {}

    metadata: dict[str, dict[str, object]] = {}
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("url") or "")
        if "/api/sns/web/v1/search/notes" not in url:
            continue
        body = str(entry.get("body_preview") or "")
        if not body:
            continue
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            continue
        for item in ((data.get("data") or {}).get("items") or []):
            if not isinstance(item, dict):
                continue
            note_id = str(item.get("id") or "").strip()
            if not note_id:
                continue
            xsec_token = str(item.get("xsec_token") or "").strip()
            interact_info = ((item.get("note_card") or {}).get("interact_info") or {})
            try:
                comment_count = int(str(interact_info.get("comment_count") or "0").strip() or 0)
            except ValueError:
                comment_count = 0
            normalized_url = _normalize_xiaohongshu_note_url(f"https://www.xiaohongshu.com/explore/{note_id}")
            current = metadata.get(normalized_url, {})
            current_count = int(current.get("comment_count") or 0)
            if comment_count >= current_count:
                metadata[normalized_url] = {
                    "comment_count": comment_count,
                    "detail_url": _build_xiaohongshu_note_detail_url(note_id, xsec_token),
                }
    return metadata


def _parse_xiaohongshu_search_note_comment_counts(network_log_path: Path) -> dict[str, int]:
    comment_counts: dict[str, int] = {}
    metadata = _parse_xiaohongshu_search_note_metadata(network_log_path)
    for normalized_url, item in metadata.items():
        comment_counts[normalized_url] = int(item.get("comment_count") or 0)
    return comment_counts


def _build_bilibili_comment_probe_source(source: dict, video_lead: Lead, index: int) -> dict:
    network_log_template = str(
        source.get("comment_network_log_template")
        or f"logs/{source['name']}.video_{{video_index}}.network.json"
    )
    probe_source = {
        "name": source["name"],
        "kind": "cloak_cdp_page",
        "parse_kind": "html_links",
        "site_kind": "bilibili_hot_comments_probe",
        "category": source["category"],
        "location": _normalize_video_url(video_lead.url),
        "default_url": _normalize_video_url(video_lead.url),
        "wait_selector": str(source.get("comment_wait_selector") or "body"),
        "wait_until": str(source.get("comment_wait_until") or "domcontentloaded"),
        "post_load_wait_ms": int(source.get("comment_post_load_wait_ms", 7000) or 7000),
        "scroll_steps": int(source.get("comment_scroll_steps", 6) or 6),
        "scroll_pause_ms": int(source.get("comment_scroll_pause_ms", 1500) or 1500),
        "capture_network": True,
        "capture_response_body": True,
        "response_body_max_chars": int(source.get("comment_response_body_max_chars", 120000) or 120000),
        "capture_resource_types": ["fetch", "xhr"],
        "capture_url_patterns": ["/x/v2/reply/wbi/main", "/x/v2/reply/main"],
        "network_log_path": network_log_template.format(video_index=index),
        "page_actions": [
            {
                "type": "evaluate",
                "script": "() => { const closeButton = document.querySelector('.login-tip .close'); if (closeButton instanceof HTMLElement) { closeButton.click(); } return true; }",
                "wait_after_ms": 1200,
            }
        ],
        "cloak": dict(source.get("cloak", {})),
    }
    if source.get("comment_rendered_html_template"):
        probe_source["rendered_html_path"] = str(source["comment_rendered_html_template"]).format(video_index=index)
    if source.get("comment_screenshot_template"):
        probe_source["screenshot_path"] = str(source["comment_screenshot_template"]).format(video_index=index)
    return probe_source


def _build_douyin_video_probe_source(source: dict) -> dict:
    network_log_path = str(
        source.get("video_network_log_path")
        or f"logs/{source['name']}.network.json"
    )
    probe_source = {
        "name": source["name"],
        "kind": "cloak_cdp_page",
        "parse_kind": "json_state",
        "site_kind": "douyin_hot_videos_probe",
        "category": source["category"],
        "location": str(source.get("location") or ""),
        "default_url": str(source.get("default_url") or source.get("location") or ""),
        "json_items_path": "items",
        "field_map": {
            "title": "title",
            "content": "content",
            "url": "url",
            "published_at": "published_at",
        },
        "wait_selector": str(source.get("wait_selector") or "body"),
        "wait_until": str(source.get("wait_until") or "commit"),
        "timeout_seconds": int(source.get("timeout_seconds", 60) or 60),
        "post_load_wait_ms": int(source.get("post_load_wait_ms", 7000) or 7000),
        "scroll_steps": int(source.get("scroll_steps", 3) or 3),
        "scroll_pause_ms": int(source.get("scroll_pause_ms", 1600) or 1600),
        "retry_on_empty_attempts": int(source.get("retry_on_empty_attempts", 1) or 1),
        "retry_on_empty_wait_ms": int(source.get("retry_on_empty_wait_ms", 2500) or 2500),
        "retry_on_empty_reset_to_blank": bool(source.get("retry_on_empty_reset_to_blank", True)),
        "capture_network": True,
        "capture_response_body": True,
        "response_body_max_chars": int(source.get("video_response_body_max_chars", 400000) or 400000),
        "capture_resource_types": ["xhr", "fetch", "document"],
        "capture_url_patterns": ["/aweme/v1/web/search/item/"],
        "network_log_path": network_log_path,
        "page_actions": list(source.get("page_actions", []) or []),
        "extract_js": "() => ({ items: [] })",
        "cloak": dict(source.get("cloak", {})),
    }
    if source.get("rendered_html_path"):
        probe_source["rendered_html_path"] = str(source["rendered_html_path"])
    if source.get("screenshot_path"):
        probe_source["screenshot_path"] = str(source["screenshot_path"])
    if source.get("extracted_json_path"):
        probe_source["extracted_json_path"] = str(source["extracted_json_path"])
    return probe_source


def _build_douyin_comment_probe_source(source: dict, video_lead: Lead, index: int) -> dict:
    network_log_template = str(
        source.get("comment_network_log_template")
        or f"logs/{source['name']}.video_{{video_index}}.network.json"
    )
    probe_source = {
        "name": source["name"],
        "kind": "cloak_cdp_page",
        "parse_kind": "json_state",
        "site_kind": "douyin_hot_comments_probe",
        "category": source["category"],
        "location": str(video_lead.url or "").strip(),
        "default_url": str(video_lead.url or "").strip(),
        "json_items_path": "items",
        "field_map": {
            "title": "title",
            "content": "content",
            "url": "url",
            "published_at": "published_at",
        },
        "wait_selector": str(source.get("comment_wait_selector") or "body"),
        "wait_until": str(source.get("comment_wait_until") or "commit"),
        "timeout_seconds": int(source.get("comment_timeout_seconds", 60) or 60),
        "post_load_wait_ms": int(source.get("comment_post_load_wait_ms", 9000) or 9000),
        "scroll_steps": int(source.get("comment_scroll_steps", 5) or 5),
        "scroll_pause_ms": int(source.get("comment_scroll_pause_ms", 1800) or 1800),
        "capture_network": True,
        "capture_response_body": True,
        "response_body_max_chars": int(source.get("comment_response_body_max_chars", 200000) or 200000),
        "capture_resource_types": ["xhr", "fetch", "document"],
        "capture_url_patterns": ["/aweme/v1/web/comment/list/"],
        "network_log_path": network_log_template.format(video_index=index),
        "extract_js": "() => ({ items: [] })",
        "cloak": dict(source.get("cloak", {})),
    }
    if source.get("comment_rendered_html_template"):
        probe_source["rendered_html_path"] = str(source["comment_rendered_html_template"]).format(video_index=index)
    if source.get("comment_screenshot_template"):
        probe_source["screenshot_path"] = str(source["comment_screenshot_template"]).format(video_index=index)
    return probe_source


def _build_xiaohongshu_comment_probe_source(source: dict, note_lead: Lead, index: int) -> dict:
    note_id = _extract_xiaohongshu_note_id(note_lead.url)
    title_hint = str(note_lead.title or "").strip()
    note_url = _prepare_xiaohongshu_probe_url(note_lead.url) or _normalize_xiaohongshu_note_url(note_lead.url)
    comments_per_note = int(source.get("comments_per_note", 5) or 5)
    rendered_template = str(source.get("comment_rendered_html_template") or "").strip()
    screenshot_template = str(source.get("comment_screenshot_template") or "").strip()
    extracted_template = str(source.get("comment_extracted_json_template") or "").strip()
    network_template = str(source.get("comment_network_log_template") or "").strip()
    mark_target_script = f"""
    () => {{
      const noteId = {json.dumps(note_id, ensure_ascii=False)};
      const titleHint = {json.dumps(title_hint, ensure_ascii=False)};
      const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
      const anchors = Array.from(document.querySelectorAll('a[href*="/explore/"], a[href*="/discovery/item/"]'));
      const candidates = anchors.map((anchor) => {{
        const card = anchor.closest('section, [class*="note-item"], [class*="item"]') || anchor.parentElement || anchor;
        const href = normalize(anchor.getAttribute('href') || anchor.href || '');
        const titleNode = card instanceof HTMLElement ? card.querySelector('.title, [class*="title"]') : null;
        const text = normalize(titleNode instanceof HTMLElement ? titleNode.textContent : (card.textContent || ''));
        return {{ anchor, card, href, text }};
      }});
      let target = candidates.find((item) => noteId && item.href.includes(noteId));
      if (!target && titleHint) {{
        target = candidates.find((item) => item.text && item.text.includes(titleHint.slice(0, 12)));
      }}
      if (!target && candidates.length) {{
        target = candidates[0];
      }}
      for (const item of candidates) {{
        if (item.anchor instanceof HTMLElement) {{
          item.anchor.removeAttribute('data-radar-note-target');
        }}
        if (item.card instanceof HTMLElement) {{
          item.card.removeAttribute('data-radar-note-target');
        }}
      }}
      if (!target) {{
        return {{ marked: false, candidates: candidates.length }};
      }}
      if (target.anchor instanceof HTMLElement) {{
        target.anchor.setAttribute('data-radar-note-target', '1');
      }}
      if (target.card instanceof HTMLElement) {{
        target.card.setAttribute('data-radar-note-target', '1');
        target.card.scrollIntoView({{ block: 'center', inline: 'center', behavior: 'instant' }});
      }}
      return {{
        marked: true,
        targetHref: target.href,
        targetText: target.text.slice(0, 80),
        candidates: candidates.length,
      }};
    }}
    """
    extract_script = f"""
    () => {{
      const noteTitle = {json.dumps(title_hint, ensure_ascii=False)};
      const noteUrl = {json.dumps(note_url, ensure_ascii=False)};
      const limit = {comments_per_note};
      const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
      const detailTitleNode = document.querySelector('.note-content .title, .title');
      const activeTitle = normalize(detailTitleNode ? detailTitleNode.textContent : document.title) || noteTitle;
      const activeUrl = normalize(location.href) || noteUrl;
      const commentSelectors = [
        '.comment-item',
        '[class*="comment-item"]',
        '.parent-comment',
        '[class*="parent-comment"]',
      ];
      const commentNodes = commentSelectors.flatMap((selector) => Array.from(document.querySelectorAll(selector)));
      const seen = new Set();
      const items = [];
      for (const node of commentNodes) {{
        const author = normalize((node.querySelector('.author, [class*="author"], .name, [class*="name"]') || {{}}).textContent || '');
        const contentNode = node.querySelector('.content, [class*="content"], .comment-text, [class*="comment-text"]');
        const content = normalize((contentNode || node).textContent || '');
        const timeText = normalize((node.querySelector('.date, [class*="date"], .time, [class*="time"]') || {{}}).textContent || '');
        if (!content || content.length < 2) {{
          continue;
        }}
        const key = `${{author}}|${{content}}`;
        if (seen.has(key)) {{
          continue;
        }}
        seen.add(key);
        items.push({{
          title: content,
          content: normalize([noteTitle && `笔记:${{noteTitle}}`, author && `评论用户:${{author}}`, timeText && `时间:${{timeText}}`].filter(Boolean).join(' | ')),
          url: `${{noteUrl}}#comment-${{items.length + 1}}`,
          published_at: timeText,
        }});
        items[items.length - 1].content = normalize([
          activeTitle && `绗旇:${{activeTitle}}`,
          author && `璇勮鐢ㄦ埛:${{author}}`,
          timeText && `鏃堕棿:${{timeText}}`,
        ].filter(Boolean).join(' | '));
        items[items.length - 1].url = `${{activeUrl}}#comment-${{items.length}}`;
        if (items.length >= limit) {{
          break;
        }}
      }}
      return {{
        items,
        href: activeUrl,
        title: activeTitle,
        hasCommentPanel: !!document.querySelector('.main-comment, [class*="comment"]'),
        commentNodeCount: commentNodes.length,
      }};
    }}
    """
    probe_source = {
        "name": source["name"],
        "kind": "cloak_cdp_page",
        "parse_kind": "json_state",
        "site_kind": "xiaohongshu_hot_comments",
        "category": source["category"],
        "location": note_url,
        "default_url": note_url,
        "json_items_path": "items",
        "field_map": {
            "title": "title",
            "content": "content",
            "url": "url",
            "published_at": "published_at",
        },
        "wait_selector": str(source.get("comment_wait_selector") or "body"),
        "wait_until": str(source.get("comment_wait_until") or "domcontentloaded"),
        "timeout_seconds": int(source.get("comment_timeout_seconds", 60) or 60),
        "post_load_wait_ms": int(source.get("comment_post_load_wait_ms", 6500) or 6500),
        "scroll_steps": int(source.get("comment_scroll_steps", 1) or 1),
        "scroll_pause_ms": int(source.get("comment_scroll_pause_ms", 1200) or 1200),
        "page_actions": [
            {
                "type": "evaluate",
                "script": "() => { const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim(); const texts = ['同意', '知道了', '稍后再说', '下次再说']; for (const text of texts) { const node = Array.from(document.querySelectorAll('button, div, span')).find((item) => normalize(item.textContent) === text); if (node instanceof HTMLElement) { node.click(); } } return document.body ? document.body.innerText.slice(0, 200) : ''; }",
                "wait_after_ms": 1200,
            },
            {
                "type": "evaluate",
                "script": mark_target_script.strip(),
                "wait_after_ms": 500,
            },
            {
                "type": "click",
                "selector": str(source.get("comment_click_selector") or "[data-radar-note-target='1'] .cover, [data-radar-note-target='1'] [class*='cover'], [data-radar-note-target='1']"),
                "wait_after_ms": int(source.get("comment_click_wait_ms", 3500) or 3500),
            },
            {
                "type": "evaluate",
                "script": "() => { const scrollers = ['.main-comment', '[class*=\"comment-container\"]', '[class*=\"comment-list\"]', '[class*=\"comment\"]']; for (const selector of scrollers) { const node = document.querySelector(selector); if (node instanceof HTMLElement) { node.scrollTop = node.scrollHeight; } } window.scrollTo(0, document.body.scrollHeight); return location.href; }",
                "wait_after_ms": int(source.get("comment_scroll_wait_ms", 2200) or 2200),
            },
        ],
        "extract_js": extract_script.strip(),
        "capture_network": bool(network_template),
        "capture_response_body": bool(network_template),
        "response_body_max_chars": int(source.get("comment_response_body_max_chars", 150000) or 150000),
        "capture_resource_types": ["xhr", "fetch", "document"],
        "capture_url_patterns": ["/api/sns/web/v1/search/notes", "/api/sns/web/v1/note"],
        "max_items": comments_per_note,
        "cloak": dict(source.get("cloak", {})),
    }
    probe_source["page_actions"] = [
        {
            "type": "evaluate",
            "script": "() => { const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim(); const texts = ['同意', '知道了', '稍后再说', '下次再说']; for (const text of texts) { const node = Array.from(document.querySelectorAll('button, div, span')).find((item) => normalize(item.textContent) === text); if (node instanceof HTMLElement) { node.click(); } } const closeSelectors = ['[class*=\"close\"]', '[aria-label=\"关闭\"]', '.close']; for (const selector of closeSelectors) { const node = document.querySelector(selector); if (node instanceof HTMLElement) { node.click(); } } return document.body ? document.body.innerText.slice(0, 200) : ''; }",
            "wait_after_ms": 1200,
        },
        {
            "type": "evaluate",
            "script": "() => true",
            "wait_after_ms": 200,
        },
        {
            "type": "click",
            "selector": "body",
            "wait_after_ms": 0,
        },
        {
            "type": "evaluate",
            "script": "() => { const detail = document.querySelector('#noteContainer, .note-content, .main-comment, [class*=\"comment-container\"]'); if (detail instanceof HTMLElement) { detail.scrollIntoView({ block: 'center', inline: 'center', behavior: 'instant' }); } const scrollers = ['.main-comment', '[class*=\"comment-container\"]', '[class*=\"comment-list\"]', '[class*=\"comment\"]']; for (const selector of scrollers) { const node = document.querySelector(selector); if (node instanceof HTMLElement) { node.scrollTop = node.scrollHeight; } } window.scrollTo(0, document.body.scrollHeight); return location.href; }",
            "wait_after_ms": int(source.get("comment_scroll_wait_ms", 2200) or 2200),
        },
    ]
    probe_source["extract_js"] = f"""
    () => {{
      const noteTitle = {json.dumps(title_hint, ensure_ascii=False)};
      const noteUrl = {json.dumps(note_url, ensure_ascii=False)};
      const limit = {comments_per_note};
      const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
      const detailTitleNode = document.querySelector('.note-content .title, .title');
      const activeTitle = normalize(detailTitleNode ? detailTitleNode.textContent : document.title) || noteTitle;
      const activeUrl = normalize(location.href) || noteUrl;
      const commentSelectors = ['.comment-item', '[class*="comment-item"]', '.parent-comment', '[class*="parent-comment"]'];
      const commentNodes = commentSelectors.flatMap((selector) => Array.from(document.querySelectorAll(selector)));
      const seen = new Set();
      const items = [];
      for (const node of commentNodes) {{
        const author = normalize((node.querySelector('.author, [class*="author"], .name, [class*="name"]') || {{}}).textContent || '');
        const contentNode = node.querySelector('.content, [class*="content"], .comment-text, [class*="comment-text"]');
        const content = normalize((contentNode || node).textContent || '');
        const timeText = normalize((node.querySelector('.date, [class*="date"], .time, [class*="time"]') || {{}}).textContent || '');
        if (!content || content.length < 2) {{
          continue;
        }}
        const key = `${{author}}|${{content}}`;
        if (seen.has(key)) {{
          continue;
        }}
        seen.add(key);
        items.push({{
          title: content,
          content: normalize([activeTitle && `note=${{activeTitle}}`, author && `author=${{author}}`, timeText && `time=${{timeText}}`].filter(Boolean).join(' | ')),
          url: `${{activeUrl}}#comment-${{items.length + 1}}`,
          published_at: timeText,
        }});
        if (items.length >= limit) {{
          break;
        }}
      }}
      return {{
        items,
        href: activeUrl,
        title: activeTitle,
        hasCommentPanel: !!document.querySelector('.main-comment, [class*="comment"]'),
        commentNodeCount: commentNodes.length,
      }};
    }}
    """.strip()
    probe_source["capture_url_patterns"] = ["/api/sns/web/v2/comment/page", "/api/sns/web/v1/note"]
    if rendered_template:
        probe_source["rendered_html_path"] = rendered_template.format(note_index=index)
    if screenshot_template:
        probe_source["screenshot_path"] = screenshot_template.format(note_index=index)
    if extracted_template:
        probe_source["extracted_json_path"] = extracted_template.format(note_index=index)
    if network_template:
        probe_source["network_log_path"] = network_template.format(note_index=index)
    return probe_source


def _extract_bilibili_comment_message(reply: dict) -> str:
    content = reply.get("content")
    if isinstance(content, dict):
        for key in ("message", "content"):
            value = str(content.get(key) or "").strip()
            if value:
                return value
    for key in ("message", "content"):
        value = str(reply.get(key) or "").strip()
        if value:
            return value
    return ""


def _format_unix_time(value: object) -> str:
    try:
        timestamp = int(value or 0)
    except (TypeError, ValueError):
        return ""
    if timestamp <= 0:
        return ""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))


def _format_bilibili_comment_time(value: object) -> str:
    try:
        timestamp = int(value or 0)
    except (TypeError, ValueError):
        return ""
    if timestamp <= 0:
        return ""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))


def _message_matches_keywords(message: str, *, include_keywords: list[str], exclude_keywords: list[str]) -> bool:
    lowered = message.lower()
    if include_keywords and not any(keyword.lower() in lowered for keyword in include_keywords):
        return False
    if exclude_keywords and any(keyword.lower() in lowered for keyword in exclude_keywords):
        return False
    return True


def _text_matches_any_keywords(text: str, keywords: list[str]) -> bool:
    lowered = str(text or "").lower()
    return any(str(keyword).strip().lower() in lowered for keyword in keywords if str(keyword).strip())


def _text_matches_keyword_filters(text: str, *, include_keywords: list[str], exclude_keywords: list[str]) -> bool:
    lowered = str(text or "").lower()
    if include_keywords and not _text_matches_any_keywords(lowered, include_keywords):
        return False
    if exclude_keywords and _text_matches_any_keywords(lowered, exclude_keywords):
        return False
    return True


def _lead_matches_keywords(lead: Lead, *, include_keywords: list[str], exclude_keywords: list[str]) -> bool:
    text = f"{lead.title}\n{lead.content}".lower()
    if include_keywords and not any(keyword.lower() in text for keyword in include_keywords):
        return False
    if exclude_keywords and any(keyword.lower() in text for keyword in exclude_keywords):
        return False
    return True


def _lead_title_matches_keywords(lead: Lead, *, include_keywords: list[str], exclude_keywords: list[str]) -> bool:
    return _text_matches_keyword_filters(
        lead.title,
        include_keywords=include_keywords,
        exclude_keywords=exclude_keywords,
    )


def _parse_bilibili_comment_network(path: Path, *, video_lead: Lead, source: dict, limit: int) -> list[Lead]:
    if not path.exists():
        return []
    try:
        entries = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(entries, list):
        return []

    leads: list[Lead] = []
    seen_reply_ids: set[str] = set()
    include_keywords = [str(item).strip() for item in source.get("comment_text_include_keywords", []) if str(item).strip()]
    exclude_keywords = [str(item).strip() for item in source.get("comment_text_exclude_keywords", []) if str(item).strip()]
    question_keywords = [str(item).strip() for item in source.get("comment_question_keywords", []) if str(item).strip()]
    business_keywords = [str(item).strip() for item in source.get("comment_business_keywords", []) if str(item).strip()]
    author_exclude_keywords = [str(item).strip().lower() for item in source.get("comment_author_exclude_keywords", []) if str(item).strip()]
    max_message_length = int(source.get("comment_max_message_length", 280) or 280)
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("url") or "")
        body_preview = str(entry.get("body_preview") or "")
        if "/x/v2/reply/" not in url or not body_preview:
            continue
        try:
            payload = json.loads(body_preview)
        except json.JSONDecodeError:
            continue
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            continue
        replies = data.get("replies")
        if not isinstance(replies, list):
            continue
        for reply in replies:
            if not isinstance(reply, dict):
                continue
            reply_id = str(reply.get("rpid_str") or reply.get("rpid") or "").strip()
            if reply_id and reply_id in seen_reply_ids:
                continue
            message = _extract_bilibili_comment_message(reply)
            if not message:
                continue
            if not _message_matches_keywords(
                message,
                include_keywords=include_keywords,
                exclude_keywords=exclude_keywords,
            ):
                continue
            if question_keywords and not _text_matches_any_keywords(message, question_keywords):
                continue
            if business_keywords and not _text_matches_any_keywords(message, business_keywords):
                continue
            member = reply.get("member") if isinstance(reply.get("member"), dict) else {}
            uname = str(member.get("uname") or "").strip()
            if author_exclude_keywords and any(keyword in uname.lower() for keyword in author_exclude_keywords):
                continue
            if max_message_length > 0 and len(message) > max_message_length:
                continue
            like_count = int(reply.get("like") or 0)
            content_parts = [message]
            if uname:
                content_parts.append(f"评论者 {uname}")
            if like_count > 0:
                content_parts.append(f"点赞 {like_count}")
            reply_url = _normalize_video_url(video_lead.url)
            if reply_id:
                reply_url = f"{reply_url}#reply{reply_id}"
                seen_reply_ids.add(reply_id)
            leads.append(
                Lead(
                    source_name=source["name"],
                    source_kind=source["kind"],
                    category=source["category"],
                    title=message,
                    content=" | ".join(part for part in content_parts if part),
                    url=reply_url,
                    published_at=_format_bilibili_comment_time(reply.get("ctime")),
                )
            )
            if len(leads) >= limit:
                return leads
    return leads


def _parse_douyin_video_network(path: Path, *, source: dict, limit: int) -> list[Lead]:
    if not path.exists():
        return []
    try:
        entries = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(entries, list):
        return []

    leads: list[Lead] = []
    seen_ids: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("url") or "")
        body_preview = str(entry.get("body_preview") or "")
        if "/aweme/v1/web/search/item/" not in url or not body_preview:
            continue
        try:
            payload = json.loads(body_preview)
        except json.JSONDecodeError:
            continue
        data = payload.get("data")
        if not isinstance(data, list):
            continue
        for item in data:
            if not isinstance(item, dict):
                continue
            aweme_info = item.get("aweme_info")
            if not isinstance(aweme_info, dict):
                continue
            aweme_id = str(aweme_info.get("aweme_id") or "").strip()
            if not aweme_id or aweme_id in seen_ids:
                continue
            title = str(aweme_info.get("desc") or "").strip()
            if not title:
                continue
            author = aweme_info.get("author") if isinstance(aweme_info.get("author"), dict) else {}
            statistics = aweme_info.get("statistics") if isinstance(aweme_info.get("statistics"), dict) else {}
            content_parts: list[str] = []
            nickname = str(author.get("nickname") or "").strip()
            if nickname:
                content_parts.append(nickname)
            play_count = int(statistics.get("play_count") or 0)
            if play_count > 0:
                content_parts.append(f"播放 {play_count}")
            comment_count = int(statistics.get("comment_count") or 0)
            if comment_count > 0:
                content_parts.append(f"评论 {comment_count}")
            digg_count = int(statistics.get("digg_count") or 0)
            if digg_count > 0:
                content_parts.append(f"点赞 {digg_count}")
            leads.append(
                Lead(
                    source_name=source["name"],
                    source_kind=source["kind"],
                    category=source["category"],
                    title=title,
                    content=" | ".join(content_parts),
                    url=f"https://www.douyin.com/video/{aweme_id}",
                    published_at=_format_unix_time(aweme_info.get("create_time")),
                    comment_count=comment_count,
                )
            )
            seen_ids.add(aweme_id)
            if len(leads) >= limit:
                return leads
    return leads


def _parse_douyin_comment_network(path: Path, *, video_lead: Lead, source: dict, limit: int) -> list[Lead]:
    if not path.exists():
        return []
    try:
        entries = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(entries, list):
        return []

    leads: list[Lead] = []
    seen_comment_ids: set[str] = set()
    include_keywords = [str(item).strip() for item in source.get("comment_text_include_keywords", []) if str(item).strip()]
    exclude_keywords = [str(item).strip() for item in source.get("comment_text_exclude_keywords", []) if str(item).strip()]
    question_keywords = [str(item).strip() for item in source.get("comment_question_keywords", []) if str(item).strip()]
    business_keywords = [str(item).strip() for item in source.get("comment_business_keywords", []) if str(item).strip()]
    author_exclude_keywords = [str(item).strip().lower() for item in source.get("comment_author_exclude_keywords", []) if str(item).strip()]
    max_message_length = int(source.get("comment_max_message_length", 280) or 280)
    min_message_length = int(source.get("comment_min_message_length", 4) or 4)
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("url") or "")
        body_preview = str(entry.get("body_preview") or "")
        if "/aweme/v1/web/comment/list/" not in url or not body_preview:
            continue
        try:
            payload = json.loads(body_preview)
        except json.JSONDecodeError:
            continue
        comments = payload.get("comments")
        if not isinstance(comments, list):
            continue
        for comment in comments:
            if not isinstance(comment, dict):
                continue
            comment_id = str(comment.get("cid") or "").strip()
            if comment_id and comment_id in seen_comment_ids:
                continue
            message = str(comment.get("text") or "").strip()
            if not message:
                continue
            if not _message_matches_keywords(
                message,
                include_keywords=include_keywords,
                exclude_keywords=exclude_keywords,
            ):
                continue
            question_matched = _text_matches_any_keywords(message, question_keywords) if question_keywords else False
            business_matched = _text_matches_any_keywords(message, business_keywords) if business_keywords else False
            if question_keywords or business_keywords:
                if not question_matched and not business_matched:
                    continue
            normalized_message = re.sub(r"\[[^\]]+\]", "", message)
            normalized_message = re.sub(r"[\s!！?？,.，。:：;；()（）【】\[\]<>《》]+", "", normalized_message)
            if len(normalized_message) < min_message_length:
                continue
            user = comment.get("user") if isinstance(comment.get("user"), dict) else {}
            nickname = str(user.get("nickname") or "").strip()
            if author_exclude_keywords and any(keyword in nickname.lower() for keyword in author_exclude_keywords):
                continue
            if max_message_length > 0 and len(message) > max_message_length:
                continue
            like_count = int(comment.get("digg_count") or 0)
            content_parts = [message]
            if nickname:
                content_parts.append(f"评论者 {nickname}")
            if like_count > 0:
                content_parts.append(f"点赞 {like_count}")
            comment_url = str(video_lead.url or "").strip()
            if comment_id:
                comment_url = f"{comment_url}?comment_id={comment_id}"
                seen_comment_ids.add(comment_id)
            leads.append(
                Lead(
                    source_name=source["name"],
                    source_kind=source["kind"],
                    category=source["category"],
                    title=message,
                    content=" | ".join(part for part in content_parts if part),
                    url=comment_url,
                    published_at=_format_unix_time(comment.get("create_time")),
                )
            )
            if len(leads) >= limit:
                return leads
    return leads


def _normalize_text_signal(text: str) -> str:
    normalized = re.sub(r"\[[^\]]+\]", "", str(text or ""))
    normalized = re.sub(r"[\W_]+", "", normalized, flags=re.UNICODE)
    return normalized.lower()


def _extract_xiaohongshu_comment_message(comment: dict) -> str:
    content = comment.get("content")
    if isinstance(content, dict):
        for key in ("text", "content", "message"):
            value = str(content.get(key) or "").strip()
            if value:
                return value
    return str(content or "").strip()


def _parse_xiaohongshu_comment_network(path: Path, *, note_lead: Lead, source: dict, limit: int) -> list[Lead]:
    if not path.exists():
        return []
    try:
        entries = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(entries, list):
        return []

    leads: list[Lead] = []
    seen_comment_ids: set[str] = set()
    include_keywords = [str(item).strip() for item in source.get("comment_text_include_keywords", []) if str(item).strip()]
    exclude_keywords = [str(item).strip() for item in source.get("comment_text_exclude_keywords", []) if str(item).strip()]
    question_keywords = [str(item).strip() for item in source.get("comment_question_keywords", []) if str(item).strip()]
    business_keywords = [str(item).strip() for item in source.get("comment_business_keywords", []) if str(item).strip()]
    author_exclude_keywords = [str(item).strip().lower() for item in source.get("comment_author_exclude_keywords", []) if str(item).strip()]
    low_signal_exact = {
        str(item).strip().lower()
        for item in source.get(
            "comment_low_signal_exact_keywords",
            ["求", "同求", "互助", "蹲", "dd", "滴滴", "打卡", "路过", "来了", "已关注", "求链接", "求资料", "可以", "已发"],
        )
        if str(item).strip()
    }
    low_signal_contains = [
        str(item).strip().lower()
        for item in source.get(
            "comment_low_signal_contains_keywords",
            ["互助就回", "带图互助", "已三连", "求踢", "求分享"],
        )
        if str(item).strip()
    ]
    max_message_length = int(source.get("comment_max_message_length", 120) or 120)
    min_message_length = int(source.get("comment_min_message_length", 2) or 2)
    note_title = str(note_lead.title or "").strip()
    note_url = _normalize_xiaohongshu_note_url(note_lead.url) or str(note_lead.url or "").strip()

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("url") or "")
        body_preview = str(entry.get("body_preview") or "")
        if "/api/sns/web/v2/comment/page" not in url or not body_preview:
            continue
        try:
            payload = json.loads(body_preview)
        except json.JSONDecodeError:
            continue
        comments = ((payload.get("data") or {}).get("comments") or [])
        if not isinstance(comments, list):
            continue
        for comment in comments:
            if not isinstance(comment, dict):
                continue
            comment_items = [comment]
            sub_comments = comment.get("sub_comments") or []
            if isinstance(sub_comments, list):
                comment_items.extend(item for item in sub_comments if isinstance(item, dict))
            for item in comment_items:
                comment_id = str(item.get("id") or "").strip()
                if comment_id and comment_id in seen_comment_ids:
                    continue
                message = _extract_xiaohongshu_comment_message(item)
                if not message:
                    continue
                if not _message_matches_keywords(
                    message,
                    include_keywords=include_keywords,
                    exclude_keywords=exclude_keywords,
                ):
                    continue
                normalized_message = _normalize_text_signal(message)
                if len(normalized_message) < min_message_length:
                    continue
                if normalized_message in low_signal_exact:
                    continue
                lowered_message = message.lower()
                if any(keyword in lowered_message for keyword in low_signal_contains):
                    continue
                combined_text = "\n".join(part for part in (message, note_title) if part)
                question_matched = _text_matches_any_keywords(combined_text, question_keywords) if question_keywords else False
                business_matched = _text_matches_any_keywords(combined_text, business_keywords) if business_keywords else False
                if question_keywords or business_keywords:
                    if not question_matched and not business_matched:
                        continue
                user_info = item.get("user_info") if isinstance(item.get("user_info"), dict) else {}
                nickname = str(user_info.get("nickname") or "").strip()
                if author_exclude_keywords and any(keyword in nickname.lower() for keyword in author_exclude_keywords):
                    continue
                if max_message_length > 0 and len(message) > max_message_length:
                    continue
                like_count = int(item.get("like_count") or 0)
                ip_location = str(item.get("ip_location") or "").strip()
                content_parts: list[str] = []
                if note_title:
                    content_parts.append(f"note={note_title}")
                if nickname:
                    content_parts.append(f"author={nickname}")
                if like_count > 0:
                    content_parts.append(f"likes={like_count}")
                if ip_location:
                    content_parts.append(f"ip={ip_location}")
                comment_url = note_url
                if comment_id:
                    comment_url = f"{note_url}#comment-{comment_id}"
                    seen_comment_ids.add(comment_id)
                leads.append(
                    Lead(
                        source_name=source["name"],
                        source_kind=source["kind"],
                        category=source["category"],
                        title=message,
                        content=" | ".join(content_parts),
                        url=comment_url,
                        published_at=_format_unix_time_millis(item.get("create_time")),
                    )
                )
                if len(leads) >= limit:
                    return leads
    return leads


def fetch_douyin_hot_videos(base_dir: str | Path, source: dict, config: dict | None = None) -> list[Lead]:
    probe_source = _build_douyin_video_probe_source(source)
    fetch_cloak_cdp_page(base_dir, probe_source, config)
    network_log = Path(base_dir) / str(probe_source["network_log_path"])
    max_items = int(source.get("max_items", 20) or 20)
    leads = _parse_douyin_video_network(network_log, source=source, limit=max_items)
    return _apply_source_filters(_dedupe_leads(leads), source)


def fetch_bilibili_hot_comments(base_dir: str | Path, source: dict, config: dict | None = None) -> list[Lead]:
    seed_source = None
    inline_seed_source = source.get("seed_source")
    if isinstance(inline_seed_source, dict) and inline_seed_source:
        seed_source = dict(inline_seed_source)
        seed_source.setdefault("name", f"{source['name']}__seed")
        seed_source.setdefault("category", str(source.get("seed_category") or "social_hot_posts"))
    else:
        seed_source_name = str(source.get("seed_source_name") or "").strip()
        seed_source = _find_named_source(config, seed_source_name)
        if not seed_source:
            raise ValueError(f"Bilibili comment source seed_source_name not found: {seed_source_name}")

    seed_limit = int(source.get("seed_limit", 2) or 2)
    comments_per_video = int(source.get("comments_per_video", 3) or 3)
    seed_include_keywords = [str(item).strip() for item in source.get("seed_title_include_keywords", []) if str(item).strip()]
    seed_exclude_keywords = [str(item).strip() for item in source.get("seed_title_exclude_keywords", []) if str(item).strip()]
    raw_seed_leads = fetch_source(base_dir, seed_source, config)
    seed_leads = [
        lead
        for lead in raw_seed_leads
        if _lead_title_matches_keywords(
            lead,
            include_keywords=seed_include_keywords,
            exclude_keywords=seed_exclude_keywords,
        )
    ][:seed_limit]
    aggregated: list[Lead] = []

    for index, video_lead in enumerate(seed_leads, start=1):
        probe_source = _build_bilibili_comment_probe_source(source, video_lead, index)
        fetch_cloak_cdp_page(base_dir, probe_source, config)
        network_log = Path(base_dir) / str(probe_source["network_log_path"])
        aggregated.extend(
            _parse_bilibili_comment_network(
                network_log,
                video_lead=video_lead,
                source=source,
                limit=comments_per_video,
            )
        )

    return _apply_source_filters(_dedupe_leads(aggregated), source)


def fetch_douyin_hot_comments(base_dir: str | Path, source: dict, config: dict | None = None) -> list[Lead]:
    seed_source = None
    inline_seed_source = source.get("seed_source")
    if isinstance(inline_seed_source, dict) and inline_seed_source:
        seed_source = dict(inline_seed_source)
        seed_source.setdefault("name", f"{source['name']}__seed")
        seed_source.setdefault("category", str(source.get("seed_category") or "social_hot_posts"))
    else:
        seed_source_name = str(source.get("seed_source_name") or "").strip()
        seed_source = _find_named_source(config, seed_source_name)
        if not seed_source:
            raise ValueError(f"Douyin comment source seed_source_name not found: {seed_source_name}")

    seed_limit = int(source.get("seed_limit", 2) or 2)
    comments_per_video = int(source.get("comments_per_video", 3) or 3)
    seed_include_keywords = [str(item).strip() for item in source.get("seed_title_include_keywords", []) if str(item).strip()]
    seed_exclude_keywords = [str(item).strip() for item in source.get("seed_title_exclude_keywords", []) if str(item).strip()]
    raw_seed_leads = fetch_source(base_dir, seed_source, config)
    seed_leads = [
        lead
        for lead in raw_seed_leads
        if _lead_title_matches_keywords(
            lead,
            include_keywords=seed_include_keywords,
            exclude_keywords=seed_exclude_keywords,
        )
    ]
    seed_leads = sorted(
        seed_leads,
        key=lambda item: (-int(item.comment_count or 0), _normalize_video_url(item.url)),
    )[:seed_limit]
    aggregated: list[Lead] = []

    for index, video_lead in enumerate(seed_leads, start=1):
        probe_source = _build_douyin_comment_probe_source(source, video_lead, index)
        fetch_cloak_cdp_page(base_dir, probe_source, config)
        network_log = Path(base_dir) / str(probe_source["network_log_path"])
        aggregated.extend(
            _parse_douyin_comment_network(
                network_log,
                video_lead=video_lead,
                source=source,
                limit=comments_per_video,
            )
        )

    return _apply_source_filters(_dedupe_leads(aggregated), source)


def fetch_xiaohongshu_hot_comments(base_dir: str | Path, source: dict, config: dict | None = None) -> list[Lead]:
    seed_source = None
    inline_seed_source = source.get("seed_source")
    if isinstance(inline_seed_source, dict) and inline_seed_source:
        seed_source = dict(inline_seed_source)
        seed_source.setdefault("name", f"{source['name']}__seed")
        seed_source.setdefault("category", str(source.get("seed_category") or "social_hot_posts"))
    else:
        seed_source_name = str(source.get("seed_source_name") or "").strip()
        seed_source = _find_named_source(config, seed_source_name)
        if not seed_source:
            raise ValueError(f"Xiaohongshu comment source seed_source_name not found: {seed_source_name}")

    seed_limit = int(source.get("seed_limit", 2) or 2)
    seed_include_keywords = [str(item).strip() for item in source.get("seed_title_include_keywords", []) if str(item).strip()]
    seed_exclude_keywords = [str(item).strip() for item in source.get("seed_title_exclude_keywords", []) if str(item).strip()]
    raw_seed_leads = fetch_source(base_dir, seed_source, config)
    search_note_metadata: dict[str, dict[str, object]] = {}
    seed_network_log_path = str(seed_source.get("network_log_path") or "").strip()
    if seed_network_log_path:
        search_note_metadata = _parse_xiaohongshu_search_note_metadata(Path(base_dir) / seed_network_log_path)
    indexed_seed_leads = list(enumerate(raw_seed_leads))
    for _, lead in indexed_seed_leads:
        normalized_url = _normalize_xiaohongshu_note_url(lead.url)
        metadata = search_note_metadata.get(normalized_url, {})
        lead.comment_count = int(metadata.get("comment_count") or 0)
        detail_url = str(metadata.get("detail_url") or "").strip()
        if detail_url:
            lead.url = detail_url

    seed_leads = [
        lead
        for _, lead in indexed_seed_leads
        if _lead_title_matches_keywords(
            lead,
            include_keywords=seed_include_keywords,
            exclude_keywords=seed_exclude_keywords,
        )
    ]
    seed_leads = sorted(
        seed_leads,
        key=lambda item: (-int(item.comment_count or 0), _normalize_xiaohongshu_note_url(item.url)),
    )[:seed_limit]
    aggregated: list[Lead] = []

    for index, note_lead in enumerate(seed_leads, start=1):
        probe_source = _build_xiaohongshu_comment_probe_source(source, note_lead, index)
        dom_leads = fetch_cloak_cdp_page(base_dir, probe_source, config)
        network_leads: list[Lead] = []
        network_log_path = str(probe_source.get("network_log_path") or "").strip()
        if network_log_path:
            network_leads = _parse_xiaohongshu_comment_network(
                Path(base_dir) / network_log_path,
                note_lead=note_lead,
                source=source,
                limit=int(source.get("comments_per_note", 5) or 5),
            )
        aggregated.extend(network_leads or dom_leads)

    filter_source = dict(source)
    filter_source["title_include_keywords"] = []
    filter_source["content_include_keywords"] = []
    return _apply_source_filters(_dedupe_leads(aggregated), filter_source)


def _fetch_page_with_fallbacks(
    base_dir: str | Path,
    page_source: dict,
    config: dict | None = None,
    seen_fingerprints: set[str] | None = None,
) -> tuple[list[Lead], bool]:
    pagination = page_source.get("pagination") or {}
    overlap_threshold = float(pagination.get("duplicate_overlap_ratio", 1.0) or 1.0)
    stop_on_duplicate = bool(pagination.get("stop_on_duplicate_page", False))
    try_fallback_on_duplicate = bool(pagination.get("try_fallback_on_duplicate_page", False))
    fallback_on_empty = bool(page_source.get("fallback_on_empty", False))

    attempts = _source_attempts(page_source)
    last_error: Exception | None = None
    last_non_empty: list[Lead] = []
    known_fingerprints = seen_fingerprints or set()

    for index, candidate in enumerate(attempts):
        try:
            leads = _fetch_source_once(base_dir, candidate, config)
        except Exception as exc:
            last_error = exc
            continue

        if leads and known_fingerprints:
            overlap_ratio = _page_overlap_ratio(leads, known_fingerprints)
            if overlap_ratio >= overlap_threshold:
                has_more_attempts = index + 1 < len(attempts)
                if try_fallback_on_duplicate and has_more_attempts:
                    continue
                if stop_on_duplicate:
                    return [], True
                leads = []

        if leads:
            return leads, False

        last_non_empty = leads
        if not fallback_on_empty:
            return leads, False

    if last_error:
        raise last_error
    return last_non_empty, False


def _parse_html_links_raw(raw: str, source: dict) -> list[Lead]:
    if source.get("site_kind") == "v2ex_topics_html":
        return _apply_source_filters(fetch_v2ex_topics_html(raw, source), source)
    if source.get("site_kind") == "zbj_demand_hall":
        return _apply_source_filters(fetch_zbj_demand_hall(raw, source), source)
    if source.get("site_kind") == "zbj_list":
        return _apply_source_filters(fetch_zbj_list(raw, source), source)
    if source.get("site_kind") == "zbj_content_hub":
        return _apply_source_filters(fetch_zbj_content_hub(raw, source), source)
    if source.get("site_kind") == "zbj_search_list":
        return _apply_source_filters(fetch_zbj_search_list(raw, source), source)
    parser = _AnchorExtractor()
    parser.feed(raw)
    base_url = source.get("base_url", "")
    leads: list[Lead] = []
    for text, href in parser.links:
        leads.append(
            Lead(
                source_name=source["name"],
                source_kind=source["kind"],
                category=source["category"],
                title=text,
                content="",
                url=urljoin(base_url, href),
                published_at="",
            )
        )
    return _apply_source_filters(leads, source)


def _parse_html_text_regex_raw(raw: str, source: dict) -> list[Lead]:
    if source.get("site_kind") == "sourceforge_reviews":
        return _apply_source_filters(fetch_sourceforge_reviews(raw, source), source)
    if source.get("site_kind") == "ccgp_procurement_list":
        return _apply_source_filters(fetch_ccgp_procurement_list(raw, source), source)
    if source.get("site_kind") == "zbj_demand_hall":
        return _apply_source_filters(fetch_zbj_demand_hall(raw, source), source)
    if source.get("site_kind") == "zbj_demand_detail":
        return _apply_source_filters(fetch_zbj_demand_detail(raw, source), source)
    if source.get("site_kind") == "zbj_service_detail":
        return _apply_source_filters(fetch_zbj_service_detail(raw, source), source)
    parser = _TextExtractor()
    parser.feed(raw)
    text = parser.get_text()
    pattern = re.compile(source["item_pattern"], re.DOTALL | re.MULTILINE)
    default_url = source.get("default_url", source["location"])
    leads: list[Lead] = []
    for match in pattern.finditer(text):
        groups = {key: (value or "").strip() for key, value in match.groupdict().items()}
        title = groups.pop("title", "").strip()
        url = groups.pop("url", "").strip() or default_url
        content_parts = [value for value in groups.values() if value]
        content = "\n".join(content_parts).strip()
        if title:
            leads.append(
                Lead(
                    source_name=source["name"],
                    source_kind=source["kind"],
                    category=source["category"],
                    title=title,
                    content=content,
                    url=urljoin(source.get("base_url", ""), url),
                    published_at="",
                )
            )
    return _apply_source_filters(leads, source)


def _apply_source_filters(leads: list[Lead], source: dict) -> list[Lead]:
    include_keywords = [keyword.lower() for keyword in source.get("title_include_keywords", [])]
    exclude_keywords = [keyword.lower() for keyword in source.get("title_exclude_keywords", [])]
    content_include_keywords = [keyword.lower() for keyword in source.get("content_include_keywords", [])]
    content_exclude_keywords = [keyword.lower() for keyword in source.get("content_exclude_keywords", [])]
    max_items = int(source.get("max_items", 0) or 0)

    filtered: list[Lead] = []
    for lead in leads:
        title = lead.title.lower()
        content = lead.content.lower()
        if include_keywords and not any(keyword in title for keyword in include_keywords):
            continue
        if exclude_keywords and any(keyword in title for keyword in exclude_keywords):
            continue
        if content_include_keywords and not any(keyword in content for keyword in content_include_keywords):
            continue
        if content_exclude_keywords and any(keyword in content for keyword in content_exclude_keywords):
            continue
        filtered.append(lead)

    if max_items > 0:
        return filtered[:max_items]
    return filtered


class _AnchorExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self._current_href = ""
        self._capture = False
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        attr_map = dict(attrs)
        self._current_href = attr_map.get("href") or ""
        self._capture = True
        self._chunks = []

    def handle_data(self, data: str) -> None:
        if self._capture:
            self._chunks.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or not self._capture:
            return
        text = " ".join(chunk.strip() for chunk in self._chunks if chunk.strip()).strip()
        if text:
            self.links.append((text, self._current_href))
        self._current_href = ""
        self._capture = False
        self._chunks = []


class _TextExtractor(HTMLParser):
    BLOCK_TAGS = {"p", "div", "li", "br", "section", "article", "h1", "h2", "h3", "h4", "h5", "h6"}

    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"script", "style", "noscript"}:
            self._skip_depth += 1
            return
        if self._skip_depth == 0 and tag in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style", "noscript"} and self._skip_depth > 0:
            self._skip_depth -= 1
            return
        if self._skip_depth == 0 and tag in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth == 0 and data.strip():
            self.parts.append(data.strip())
            self.parts.append(" ")

    def get_text(self) -> str:
        text = "".join(self.parts)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{2,}", "\n", text)
        return text.strip()


def fetch_rss(base_dir: str | Path, source: dict, config: dict | None = None) -> list[Lead]:
    raw = _read_text(base_dir, source["location"], _runtime_config(config, source))
    if source.get("site_kind") == "v2ex_rss":
        return _apply_source_filters(fetch_v2ex_rss(raw, source), source)
    root = ET.fromstring(raw)
    leads: list[Lead] = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        content = (item.findtext("description") or "").strip()
        url = (item.findtext("link") or "").strip()
        published_at = (item.findtext("pubDate") or "").strip()
        if title:
            leads.append(
                Lead(
                    source_name=source["name"],
                    source_kind=source["kind"],
                    category=source["category"],
                    title=title,
                    content=content,
                    url=url,
                    published_at=published_at,
                )
            )
    return _apply_source_filters(leads, source)


def _extract_path(data: dict, path: str) -> list[dict]:
    value: object = data
    for segment in path.split("."):
        if not isinstance(value, dict):
            return []
        value = value.get(segment)
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _parse_json_raw(raw: str, source: dict) -> list[Lead]:
    if source.get("site_kind") == "v2ex_json_feed":
        return _apply_source_filters(fetch_v2ex_json_feed(raw, source), source)
    if source.get("site_kind") == "remoteok_jobs":
        return _apply_source_filters(fetch_remoteok_jobs(raw, source), source)
    if source.get("site_kind") == "cnode_topics":
        return _apply_source_filters(fetch_cnode_topics(raw, source), source)
    if source.get("site_kind") == "xianyu_service_list":
        return _apply_source_filters(fetch_xianyu_service_list(raw, source), source)
    if source.get("site_kind") == "zbj_search_state":
        return _apply_source_filters(fetch_zbj_search_state(raw, source), source)
    if source.get("site_kind") == "zbj_demand_hall":
        return _apply_source_filters(fetch_zbj_demand_hall(raw, source), source)
    if source.get("site_kind"):
        return _apply_source_filters(fetch_json_items(raw, source), source)

    data = json.loads(raw)
    items = _extract_path(data, source["json_items_path"])
    field_map = source.get("field_map", {})
    leads: list[Lead] = []
    for item in items:
        title = str(item.get(field_map.get("title", "title"), "")).strip()
        content = str(item.get(field_map.get("content", "content"), "")).strip()
        url = str(item.get(field_map.get("url", "url"), "")).strip()
        published_at = str(item.get(field_map.get("published_at", "published_at"), "")).strip()
        if title:
            leads.append(
                Lead(
                    source_name=source["name"],
                    source_kind=source["kind"],
                    category=source["category"],
                    title=title,
                    content=content,
                    url=url,
                    published_at=published_at,
                )
            )
    return _apply_source_filters(leads, source)


def fetch_json(base_dir: str | Path, source: dict, config: dict | None = None) -> list[Lead]:
    raw = _read_text(base_dir, source["location"], _runtime_config(config, source))
    return _parse_json_raw(raw, source)


def fetch_html_links(base_dir: str | Path, source: dict, config: dict | None = None) -> list[Lead]:
    raw = _read_text(base_dir, source["location"], _runtime_config(config, source))
    return _parse_html_links_raw(raw, source)


def fetch_html_text_regex(base_dir: str | Path, source: dict, config: dict | None = None) -> list[Lead]:
    raw = _read_text(base_dir, source["location"], _runtime_config(config, source))
    return _parse_html_text_regex_raw(raw, source)


def fetch_cloak_cdp_page(base_dir: str | Path, source: dict, config: dict | None = None) -> list[Lead]:
    cloak_config = _cloak_runtime_config(config, source)
    client = build_cloak_client(cloak_config)

    profile_id = str(cloak_config.get("profile_id") or "").strip()
    profile_name = str(cloak_config.get("profile_name") or "").strip()
    if not profile_id and not profile_name and cloak_config.get("create_payload"):
        created = client.create_browser(dict(cloak_config["create_payload"]))
        profile_id = str(created.get("id") or "").strip()
    resolved_profile_id = client.resolve_profile_id(profile_id=profile_id or None, profile_name=profile_name or None)
    close_after_fetch = bool(cloak_config.get("close_after_fetch", True))
    open_args = list(cloak_config.get("open_args", []) or [])
    queue = bool(cloak_config.get("queue", True))

    open_data = client.open_browser(resolved_profile_id, args=open_args, queue=queue)
    should_close_browser = close_after_fetch and not bool(open_data.get("already_running"))
    try:
        rendered = fetch_page_via_cdp(open_data, source, base_dir)
        parse_kind = str(source.get("parse_kind") or "").strip()
        render_source = dict(source)
        render_source["default_url"] = rendered.get("url") or source.get("default_url", source.get("location", ""))
        if parse_kind == "html_text_regex":
            return _parse_html_text_regex_raw(rendered["html"], render_source)
        if parse_kind == "json_state":
            payload = rendered.get("data")
            if payload is None:
                return []
            return _parse_json_raw(json.dumps(payload, ensure_ascii=False), render_source)
        return _parse_html_links_raw(rendered["html"], render_source)
    finally:
        if should_close_browser:
            client.close_browser(resolved_profile_id)


def _fetch_source_once(base_dir: str | Path, source: dict, config: dict | None = None) -> list[Lead]:
    kind = source["kind"]
    if kind == "rss":
        return fetch_rss(base_dir, source, config)
    if kind == "json":
        return fetch_json(base_dir, source, config)
    if kind == "html_links":
        return fetch_html_links(base_dir, source, config)
    if kind == "html_text_regex":
        return fetch_html_text_regex(base_dir, source, config)
    if kind == "cloak_cdp_page":
        return fetch_cloak_cdp_page(base_dir, source, config)
    if kind == "bilibili_hot_comments":
        return fetch_bilibili_hot_comments(base_dir, source, config)
    if kind == "douyin_hot_videos":
        return fetch_douyin_hot_videos(base_dir, source, config)
    if kind == "douyin_hot_comments":
        return fetch_douyin_hot_comments(base_dir, source, config)
    if kind == "xiaohongshu_hot_comments":
        return fetch_xiaohongshu_hot_comments(base_dir, source, config)
    raise ValueError(f"Unsupported source kind: {kind}")


def fetch_source(base_dir: str | Path, source: dict, config: dict | None = None) -> list[Lead]:
    aggregated: list[Lead] = []
    seen_fingerprints: set[str] = set()

    for paged_source in _paginated_attempts(source):
        page_leads, should_stop = _fetch_page_with_fallbacks(
            base_dir,
            paged_source,
            config,
            seen_fingerprints=seen_fingerprints,
        )
        if page_leads:
            aggregated.extend(page_leads)
            seen_fingerprints.update(_lead_fingerprints(page_leads))
        if should_stop:
            break

    enriched = _enrich_with_detail(aggregated, base_dir, source, config)
    return _dedupe_leads(enriched)
