from __future__ import annotations

import html
import json
import re
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin

from radar.models import Lead


class _ZbjAnchorExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self._href = ""
        self._capture = False
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        attr_map = dict(attrs)
        href = attr_map.get("href") or ""
        cls = attr_map.get("class") or ""
        if not href or ("shop" not in href and "service" not in href and "detail" not in href):
            return
        if any(token in cls for token in ["title", "tt", "name"]):
            self._capture = True
        else:
            self._capture = True
        self._href = href
        self._chunks = []

    def handle_data(self, data: str) -> None:
        if self._capture:
            self._chunks.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or not self._capture:
            return
        text = " ".join(chunk.strip() for chunk in self._chunks if chunk.strip()).strip()
        if text and len(text) >= 4:
            self.links.append((text, self._href))
        self._href = ""
        self._capture = False
        self._chunks = []


class _HeadingExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.items: list[str] = []
        self._capture = False
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"h2", "h3", "h4"}:
            self._capture = True
            self._chunks = []

    def handle_data(self, data: str) -> None:
        if self._capture:
            self._chunks.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"h2", "h3", "h4"} and self._capture:
            text = re.sub(r"\s+", " ", "".join(self._chunks)).strip()
            if text and len(text) >= 6:
                self.items.append(text)
            self._capture = False
            self._chunks = []


class _V2exTopicExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.topics: list[tuple[str, str]] = []
        self._capture = False
        self._href = ""
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        attr_map = dict(attrs)
        href = attr_map.get("href") or ""
        if not href.startswith("/t/"):
            return
        self._capture = True
        self._href = href
        self._chunks = []

    def handle_data(self, data: str) -> None:
        if self._capture:
            self._chunks.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or not self._capture:
            return
        title = re.sub(r"\s+", " ", "".join(self._chunks)).strip()
        if title and len(title) >= 4:
            self.topics.append((title, self._href))
        self._capture = False
        self._href = ""
        self._chunks = []


class _JsValueParser:
    def __init__(self, raw: str, variables: dict[str, object] | None = None) -> None:
        self.raw = raw
        self.length = len(raw)
        self.index = 0
        self.variables = variables or {}

    def parse_value(self) -> object:
        self._skip_ws()
        if self.index >= self.length:
            raise ValueError("unexpected end of javascript payload")
        char = self.raw[self.index]
        if char == "{":
            return self._parse_object()
        if char == "[":
            return self._parse_array()
        if char in {'"', "'"}:
            return self._parse_string()
        if char == "-" or char.isdigit():
            return self._parse_number()
        return self._parse_identifier_value()

    def parse_arguments(self) -> list[object]:
        values: list[object] = []
        while True:
            self._skip_ws()
            if self.index >= self.length:
                break
            values.append(self.parse_value())
            self._skip_ws()
            if self.index < self.length and self.raw[self.index] == ",":
                self.index += 1
                continue
            break
        return values

    def _skip_ws(self) -> None:
        while self.index < self.length and self.raw[self.index] in " \t\r\n":
            self.index += 1

    def _parse_string(self) -> str:
        quote = self.raw[self.index]
        self.index += 1
        result: list[str] = []
        while self.index < self.length:
            char = self.raw[self.index]
            self.index += 1
            if char == quote:
                return "".join(result)
            if char != "\\":
                result.append(char)
                continue
            if self.index >= self.length:
                break
            escaped = self.raw[self.index]
            self.index += 1
            if escaped == "u" and self.index + 4 <= self.length:
                chunk = self.raw[self.index : self.index + 4]
                self.index += 4
                try:
                    result.append(chr(int(chunk, 16)))
                except ValueError:
                    result.append(chunk)
                continue
            if escaped == "x" and self.index + 2 <= self.length:
                chunk = self.raw[self.index : self.index + 2]
                self.index += 2
                try:
                    result.append(chr(int(chunk, 16)))
                except ValueError:
                    result.append(chunk)
                continue
            escape_map = {
                '"': '"',
                "'": "'",
                "\\": "\\",
                "/": "/",
                "b": "\b",
                "f": "\f",
                "n": "\n",
                "r": "\r",
                "t": "\t",
            }
            result.append(escape_map.get(escaped, escaped))
        raise ValueError("unterminated javascript string")

    def _parse_number(self) -> int | float:
        match = re.match(r"-?\d+(?:\.\d+)?", self.raw[self.index :])
        if not match:
            raise ValueError("invalid javascript number")
        token = match.group(0)
        self.index += len(token)
        if "." in token:
            return float(token)
        return int(token)

    def _parse_identifier(self) -> str:
        match = re.match(r"[A-Za-z_$][A-Za-z0-9_$]*", self.raw[self.index :])
        if not match:
            raise ValueError(f"invalid javascript identifier near {self.raw[self.index:self.index + 20]!r}")
        token = match.group(0)
        self.index += len(token)
        return token

    def _parse_identifier_value(self) -> object:
        token = self._parse_identifier()
        if token == "true":
            return True
        if token == "false":
            return False
        if token == "null":
            return None
        if token == "void":
            self._skip_ws()
            if self.raw[self.index : self.index + 1] == "0":
                self.index += 1
            return None
        if token == "Array":
            self._skip_ws()
            if self.index < self.length and self.raw[self.index] == "(":
                self.index += 1
                self._skip_ws()
                if self.raw[self.index : self.index + 1] == "0":
                    self.index += 1
                self._skip_ws()
                if self.index < self.length and self.raw[self.index] == ")":
                    self.index += 1
                return []
        return self.variables.get(token, token)

    def _parse_object(self) -> dict[str, object]:
        result: dict[str, object] = {}
        self.index += 1
        while True:
            self._skip_ws()
            if self.index >= self.length:
                raise ValueError("unterminated javascript object")
            if self.raw[self.index] == "}":
                self.index += 1
                return result
            if self.raw[self.index] in {'"', "'"}:
                key = self._parse_string()
            else:
                key = self._parse_identifier()
            self._skip_ws()
            if self.index >= self.length or self.raw[self.index] != ":":
                raise ValueError("expected ':' in javascript object")
            self.index += 1
            result[key] = self.parse_value()
            self._skip_ws()
            if self.index < self.length and self.raw[self.index] == ",":
                self.index += 1
                continue
            if self.index < self.length and self.raw[self.index] == "}":
                self.index += 1
                return result
        return result

    def _parse_array(self) -> list[object]:
        result: list[object] = []
        self.index += 1
        while True:
            self._skip_ws()
            if self.index >= self.length:
                raise ValueError("unterminated javascript array")
            if self.raw[self.index] == "]":
                self.index += 1
                return result
            result.append(self.parse_value())
            self._skip_ws()
            if self.index < self.length and self.raw[self.index] == ",":
                self.index += 1
                continue
            if self.index < self.length and self.raw[self.index] == "]":
                self.index += 1
                return result
        return result


def _find_balanced_segment(raw: str, start: int, open_char: str, close_char: str) -> int:
    depth = 0
    index = start
    in_string = False
    quote = ""
    while index < len(raw):
        char = raw[index]
        if in_string:
            if char == "\\":
                index += 2
                continue
            if char == quote:
                in_string = False
            index += 1
            continue
        if char in {'"', "'"}:
            in_string = True
            quote = char
            index += 1
            continue
        if char == open_char:
            depth += 1
        elif char == close_char:
            depth -= 1
            if depth == 0:
                return index
        index += 1
    raise ValueError("unterminated javascript segment")


def _extract_zbj_nuxt_result_list(raw: str) -> list[dict[str, Any]]:
    marker = "window.__NUXT__=(function("
    start = raw.find(marker)
    if start < 0:
        return []

    function_pos = raw.find("function(", start)
    if function_pos < 0:
        return []
    params_open = raw.find("(", function_pos)
    params_close = _find_balanced_segment(raw, params_open, "(", ")")
    params = [item.strip() for item in raw[params_open + 1 : params_close].split(",") if item.strip()]

    body_open = raw.find("{", params_close)
    if body_open < 0:
        return []
    body_close = _find_balanced_segment(raw, body_open, "{", "}")
    body = raw[body_open + 1 : body_close]

    call_open = raw.find("(", body_close + 1)
    if call_open < 0:
        return []
    call_close = _find_balanced_segment(raw, call_open, "(", ")")
    arguments_raw = raw[call_open + 1 : call_close]

    variables = dict(zip(params, _JsValueParser(arguments_raw).parse_arguments()))
    result_list_pos = body.find("resultList:")
    if result_list_pos < 0:
        return []
    array_start = body.find("[", result_list_pos)
    if array_start < 0:
        return []
    payload = _JsValueParser(body[array_start:], variables).parse_value()
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def _is_zbj_tech_demand_legacy(title: str, category: str, content: str, labels: str, source: dict) -> bool:
    text = "\n".join([title, category, content, labels]).lower()
    positive = [
        "网站",
        "小程序",
        "app",
        "软件",
        "系统",
        "脚本",
        "自动",
        "批量",
        "表格",
        "excel",
        "cms",
        "erp",
        "crm",
        "数据",
        "图表",
        "报表",
        "接口",
        "api",
        "识别",
        "ocr",
        "采集",
        "爬虫",
        "开发",
        "管理",
        "仪表盘",
        "dashboard",
    ]
    negative = [
        "logo",
        "vi",
        "海报",
        "包装",
        "宣传",
        "剪辑",
        "视频",
        "修图",
        "抠图",
        "配音",
        "动画",
        "摄影",
        "取名",
        "作图",
        "平面",
        "美工",
        "插画",
        "漫画",
    ]
    positive.extend(str(item).lower() for item in source.get("tech_positive_keywords", []))
    negative.extend(str(item).lower() for item in source.get("tech_negative_keywords", []))
    if any(token in text for token in negative):
        return False
    return any(token in text for token in positive)


_SOURCEFORGE_NEGATIVE_SIGNALS = [
    "too expensive",
    "painful",
    "hard to troubleshoot",
    "learning curve",
    "less intuitive",
    "mobile",
    "slow",
    "scattered",
    "difficult",
    "complex",
    "steep",
    "intricate",
    "error",
    "problem",
]

_SOURCEFORGE_POSITIVE_ONLY_CONS = [
    "no cons",
    "never encountered any problem",
    "can't go wrong",
    "excellent tool. no cons",
    "i will update if i face any",
]


def _is_zbj_tech_demand(title: str, category: str, content: str, labels: str, source: dict) -> bool:
    title_text = title.lower()
    category_text = category.lower()
    content_text = content.lower()
    labels_text = labels.lower()
    text = "\n".join([title_text, category_text, content_text, labels_text])

    strong_positive = [
        "网站",
        "网站开发",
        "网站建设",
        "网站二次开发",
        "小程序",
        "app",
        "软件",
        "系统",
        "脚本",
        "自动",
        "自动化",
        "表格",
        "excel",
        "数据",
        "图表",
        "报表",
        "接口",
        "api",
        "ocr",
        "识别",
        "cms",
        "erp",
        "crm",
        "saas",
        "采集",
        "爬虫",
        "管理后台",
        "后台",
        "对账",
        "同步",
        "监控",
        "提醒",
        "dashboard",
        "工具",
    ]
    soft_positive = [
        "管理",
        "流程",
        "工作流",
        "批量",
        "统计",
        "导出",
        "清洗",
        "转换",
        "可视化",
        "看板",
        "企业ai",
    ]
    hard_negative = [
        "logo",
        "vi",
        "海报",
        "包装",
        "宣传",
        "剪辑",
        "视频",
        "修图",
        "抠图",
        "配音",
        "动画",
        "摄影",
        "取名",
        "作图",
        "平面",
        "美工",
        "插画",
        "漫画",
        "商标",
        "原画",
        "建模",
    ]

    strong_positive.extend(str(item).lower() for item in source.get("tech_positive_keywords", []))
    hard_negative.extend(str(item).lower() for item in source.get("tech_negative_keywords", []))

    strong_hits = _keyword_hit_count(text, strong_positive)
    soft_hits = _keyword_hit_count(text, soft_positive)
    negative_hits = _keyword_hit_count(text, hard_negative)
    title_negative_hits = _keyword_hit_count("\n".join([title_text, category_text]), hard_negative)
    category_positive_keywords = [str(item).lower() for item in source.get("tech_positive_category_keywords", [])]
    category_negative_keywords = [str(item).lower() for item in source.get("tech_negative_category_keywords", [])]
    context_keywords = [str(item).lower() for item in source.get("tech_context_keywords", [])]
    category_positive_hits = _keyword_hit_count(category_text, category_positive_keywords)
    category_negative_hits = _keyword_hit_count("\n".join([title_text, category_text, labels_text]), category_negative_keywords)
    context_hits = _keyword_hit_count(text, context_keywords)

    if (title_negative_hits or category_negative_hits) and strong_hits < 2 and category_positive_hits == 0 and context_hits < 2:
        return False
    if category_positive_hits >= 1:
        return True
    if strong_hits >= 1 and not title_negative_hits and (title_text or category_text):
        return True
    if negative_hits and strong_hits == 0 and soft_hits < 2 and context_hits < 2:
        return False
    if strong_hits >= 2:
        return True
    return strong_hits >= 1 or soft_hits >= 2 or context_hits >= 2


def _strip_html_tags(value: str) -> str:
    value = re.sub(r"<br\s*/?>", "\n", value, flags=re.IGNORECASE)
    value = re.sub(r"<[^>]+>", " ", value)
    value = html.unescape(value)
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r"\n{2,}", "\n", value)
    return value.strip()


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _nested_value(data: object, *segments: str) -> object:
    value = data
    for segment in segments:
        if not isinstance(value, dict):
            return ""
        value = value.get(segment)
    return value


def _first_non_empty(data: object, *candidates: object) -> object:
    for candidate in candidates:
        value: object = ""
        if isinstance(candidate, tuple):
            value = _nested_value(data, *candidate)
        elif isinstance(candidate, str):
            value = _nested_value(data, candidate)
        if value not in (None, "", [], {}):
            return value
    return ""


def _coerce_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        for key in ("format", "text", "name", "title", "value", "label"):
            candidate = _coerce_text(value.get(key))
            if candidate:
                return candidate
        return ""
    if isinstance(value, list):
        parts = [_coerce_text(item) for item in value]
        return " / ".join(part for part in parts if part)
    return _normalize_text(_strip_html_tags(str(value)))


def _extract_int(value: object) -> int:
    text = _coerce_text(value)
    match = re.search(r"(\d+)", text.replace(",", ""))
    return int(match.group(1)) if match else 0


def _extract_price_value(value: object) -> float:
    if isinstance(value, dict):
        for key in ("origin", "value", "price", "amount"):
            candidate = value.get(key)
            if candidate not in (None, ""):
                try:
                    return float(str(candidate).replace(",", "").strip())
                except ValueError:
                    continue
    text = _coerce_text(value)
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)", text.replace(",", ""))
    return float(match.group(1)) if match else 0.0


def _extract_first_int(raw: str, patterns: list[str]) -> int:
    for pattern in patterns:
        match = re.search(pattern, raw, re.IGNORECASE)
        if match:
            value = match.group("value") if "value" in match.groupdict() else match.group(1)
            digits = re.sub(r"[^\d]", "", value or "")
            if digits:
                return int(digits)
    return 0


def _extract_first_text(raw: str, patterns: list[str]) -> str:
    for pattern in patterns:
        match = re.search(pattern, raw, re.IGNORECASE | re.DOTALL)
        if match:
            value = match.group("value") if "value" in match.groupdict() else match.group(1)
            cleaned = _normalize_text(_strip_html_tags(value or ""))
            if cleaned:
                return cleaned
    return ""


def _keyword_hit_count(text: str, keywords: list[str]) -> int:
    return sum(1 for keyword in keywords if keyword and keyword in text)


def _is_v2ex_demand(title: str, content: str) -> bool:
    text = f"{title}\n{content}".lower()
    demand_signals = [
        "求",
        "自动",
        "批量",
        "脚本",
        "工具",
        "workflow",
        "automation",
        "monitor",
        "report",
        "导出",
        "提醒",
        "效率",
        "报表",
        "订单",
        "库存",
        "店铺",
        "有没有",
        "怎么做",
    ]
    noise_signals = ["每日", "分享创造", "show hn", "show v2ex", "二手交易"]
    return not any(noise in title.lower() for noise in noise_signals) and any(signal in text for signal in demand_signals)


def fetch_v2ex_rss(raw: str, source: dict) -> list[Lead]:
    root = ET.fromstring(raw)
    leads: list[Lead] = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        content = (item.findtext("description") or "").strip()
        url = (item.findtext("link") or "").strip()
        published_at = (item.findtext("pubDate") or "").strip()
        normalized_title = title.lower()
        normalized_content = re.sub(r"<[^>]+>", " ", content).strip().lower()
        if title and _is_v2ex_demand(normalized_title, normalized_content):
            leads.append(
                Lead(
                    source_name=source["name"],
                    source_kind=source["kind"],
                    category=source["category"],
                    title=title,
                    content=re.sub(r"<[^>]+>", " ", content).strip(),
                    url=url,
                    published_at=published_at,
                )
            )
    return leads


def fetch_v2ex_json_feed(raw: str, source: dict) -> list[Lead]:
    data = json.loads(raw)
    items = data.get("items", [])
    if not isinstance(items, list):
        return []
    leads: list[Lead] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        content = _strip_html_tags(
            str(item.get("content_text") or item.get("content_html") or item.get("summary") or item.get("content", ""))
        )
        url = str(item.get("url") or item.get("external_url") or "").strip()
        published_at = str(item.get("date_published") or item.get("published_at") or "").strip()
        if title and _is_v2ex_demand(title, content):
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
    return leads


def fetch_v2ex_topics_html(raw: str, source: dict) -> list[Lead]:
    parser = _V2exTopicExtractor()
    parser.feed(raw)
    base_url = source.get("base_url", source.get("default_url", "https://www.v2ex.com"))
    leads: list[Lead] = []
    seen: set[str] = set()
    for title, href in parser.topics:
        normalized = re.sub(r"\s+", " ", title).strip()
        if normalized in seen:
            continue
        seen.add(normalized)
        if not _is_v2ex_demand(normalized, ""):
            continue
        leads.append(
            Lead(
                source_name=source["name"],
                source_kind=source["kind"],
                category=source["category"],
                title=normalized,
                content="",
                url=urljoin(base_url, href),
                published_at="",
            )
        )
    return leads


def fetch_sourceforge_reviews(raw: str, source: dict) -> list[Lead]:
    normalized = re.sub(r"\s+", " ", raw)
    patterns = [
        re.compile(
            r'<h3[^>]*class="review-title"[^>]*>(?P<title>.*?)</h[34]>.*?Posted\s+\d{4}-\d{2}-\d{2}.*?<p><b>Pros:</b>\s*(?P<pros>.*?)</p>.*?<p><b>Cons:</b>\s*(?P<cons>.*?)</p>.*?<p><b>Overall:</b>\s*(?P<overall>.*?)</p>',
            re.IGNORECASE,
        ),
        re.compile(
            r'"review_title":"(?P<title>[^"]+)".*?"pros":"(?P<pros>.*?)".*?"cons":"(?P<cons>.*?)".*?"overall":"(?P<overall>.*?)"',
            re.IGNORECASE,
        ),
        re.compile(
            r'"(?P<title>[^"]+)"\s+Posted\s+\d{4}-\d{2}-\d{2}\s+Pros:\s*(?P<pros>.*?)\s+Cons:\s*(?P<cons>.*?)\s+Overall:\s*(?P<overall>.*?)(?:Read More|$)',
            re.IGNORECASE,
        ),
    ]

    leads: list[Lead] = []
    seen: set[str] = set()
    for pattern in patterns:
        for match in pattern.finditer(normalized):
            title = _strip_html_tags((match.groupdict().get("title") or "").strip()).strip('" ')
            if not title or title in seen:
                continue
            pros = _strip_html_tags((match.groupdict().get("pros") or "").strip())
            cons = _strip_html_tags((match.groupdict().get("cons") or "").strip())
            overall = _strip_html_tags((match.groupdict().get("overall") or "").strip())
            normalized_cons = cons.lower()
            if any(token in normalized_cons for token in _SOURCEFORGE_POSITIVE_ONLY_CONS):
                continue
            if not any(token in normalized_cons or token in overall.lower() for token in _SOURCEFORGE_NEGATIVE_SIGNALS):
                continue
            seen.add(title)
            parts = []
            if cons:
                parts.append(f"Cons: {cons}")
            if overall:
                parts.append(f"Overall: {overall}")
            if pros and any(token in pros.lower() for token in ["automation", "workflow", "lead", "report", "integration"]):
                parts.append(f"Pros: {pros}")
            leads.append(
                Lead(
                    source_name=source["name"],
                    source_kind=source["kind"],
                    category=source["category"],
                    title=title,
                    content="\n".join(parts),
                    url=source.get("default_url", source["location"]),
                    published_at="",
                )
            )
    if leads:
        return leads

    # Fallback: alternatives / compare blurbs can still reveal real workflow pains even when review blocks are absent.
    fallback_pattern = re.compile(
        r"<strong>(?P<title>[^<]+)</strong>\s*</a>\s*<span>(?P<summary>.*?)</span>",
        re.IGNORECASE,
    )
    for match in fallback_pattern.finditer(normalized):
        title = _strip_html_tags(match.group("title")).strip()
        summary = _strip_html_tags(match.group("summary")).strip()
        text = f"{title}\n{summary}".lower()
        if not any(token in text for token in ["workflow", "automate", "integration", "data", "efficiency", "scale", "siloed"]):
            continue
        if title in seen:
            continue
        seen.add(title)
        leads.append(
            Lead(
                source_name=source["name"],
                source_kind=source["kind"],
                category=source["category"],
                title=title,
                content=summary,
                url=source.get("default_url", source["location"]),
                published_at="",
            )
        )
    return leads


def fetch_zbj_list(raw: str, source: dict) -> list[Lead]:
    parser = _ZbjAnchorExtractor()
    parser.feed(raw)
    base_url = source.get("base_url", "")
    leads: list[Lead] = []
    seen: set[str] = set()
    for text, href in parser.links:
        normalized = re.sub(r"\s+", " ", text).strip()
        if normalized in seen:
            continue
        seen.add(normalized)
        if len(normalized) < 4:
            continue
        leads.append(
            Lead(
                source_name=source["name"],
                source_kind=source["kind"],
                category=source["category"],
                title=normalized,
                content="",
                url=urljoin(base_url, href),
                published_at="",
            )
        )
    return leads


def fetch_zbj_content_hub(raw: str, source: dict) -> list[Lead]:
    parser = _HeadingExtractor()
    parser.feed(raw)
    leads: list[Lead] = []
    seen: set[str] = set()
    titles = list(parser.items)
    if not titles:
        anchor_parser = _ZbjAnchorExtractor()
        anchor_parser.feed(raw)
        titles = [text for text, _href in anchor_parser.links]
    for title in titles:
        normalized = re.sub(r"\s+", " ", title).strip()
        normalized_lower = normalized.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        # 过滤过泛或明显无关的内容
        if any(noise in normalized_lower for noise in ["logo", "包装", "海报", "配音", "动画", "vi设计", "商标"]):
            continue
        if not any(
            token in normalized_lower
            for token in [
                "app",
                "小程序",
                "软件",
                "开发",
                "数据",
                "python",
                "java",
                "客户端",
                "电商",
                "商城",
                "工具",
                "系统",
                "报表",
                "对账",
                "自动",
            ]
        ):
            continue
        leads.append(
            Lead(
                source_name=source["name"],
                source_kind=source["kind"],
                category=source["category"],
                title=normalized,
                content="",
                url=source.get("default_url", source["location"]),
                published_at="",
            )
        )
    return leads


def fetch_zbj_search_list(raw: str, source: dict) -> list[Lead]:
    parser = _ZbjAnchorExtractor()
    parser.feed(raw)
    base_url = source.get("base_url", source.get("default_url", "https://fuwu.zbj.com"))
    leads: list[Lead] = []
    seen: set[str] = set()
    for text, href in parser.links:
        normalized = re.sub(r"\s+", " ", text).strip()
        normalized_lower = normalized.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        if len(normalized) < 6:
            continue
        if not any(
            token in normalized_lower
            for token in [
                "脚本",
                "自动",
                "软件",
                "开发",
                "数据",
                "爬虫",
                "采集",
                "报表",
                "对账",
                "工具",
                "系统",
                "小程序",
                "商城",
                "erp",
                "crm",
                "python",
                "c#",
            ]
        ):
            continue
        leads.append(
            Lead(
                source_name=source["name"],
                source_kind=source["kind"],
                category=source["category"],
                title=normalized,
                content="",
                url=urljoin(base_url, href),
                published_at="",
            )
        )

    if leads:
        return leads

    json_title_pattern = re.compile(r'"(?:title|serviceName|name|headline)"\s*:\s*"(?P<title>[^"]{6,80})"', re.IGNORECASE)
    for match in json_title_pattern.finditer(raw):
        normalized = _strip_html_tags(match.group("title")).strip()
        normalized_lower = normalized.lower()
        if normalized in seen:
            continue
        if not any(token in normalized_lower for token in ["脚本", "自动", "软件", "开发", "数据", "小程序", "商城", "系统", "报表", "工具"]):
            continue
        seen.add(normalized)
        leads.append(
            Lead(
                source_name=source["name"],
                source_kind=source["kind"],
                category=source["category"],
                title=normalized,
                content="",
                url=source.get("default_url", source.get("location", "")),
                published_at="",
            )
        )
    return leads


def fetch_zbj_search_state(raw: str, source: dict) -> list[Lead]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []

    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        candidate = data.get("resultList") or data.get("items") or data.get("data") or []
        items = candidate if isinstance(candidate, list) else []
    else:
        items = []

    detail_url_template = source.get("detail_url_template", "https://www.zbj.com/fw/{id}.html")
    search_keyword = _normalize_text(str(source.get("search_keyword") or ""))
    leads: list[Lead] = []
    seen: set[str] = set()

    for item in items:
        if not isinstance(item, dict):
            continue
        service_id = str(item.get("id") or item.get("serviceId") or "").strip()
        title = _normalize_text(
            _strip_html_tags(str(item.get("name") or item.get("serviceName") or item.get("title") or ""))
        )
        if not title or len(title) < 4 or title in seen:
            continue
        seen.add(title)

        price = _coerce_text(item.get("price"))
        price_value = _extract_price_value(item.get("price"))
        cumulative_sale_count = _coerce_text(item.get("cumulativeSaleCount"))
        sale_count = _coerce_text(item.get("saleCount"))
        comment_count = _coerce_text(item.get("commentCount"))
        good_comment_count = _coerce_text(item.get("goodCommentCount"))
        city = _coerce_text(_nested_value(item, "shop", "city"))
        user_type = _coerce_text(_nested_value(item, "shop", "userType"))
        standard_time = _coerce_text(item.get("standardTime"))
        shop_name = _coerce_text(item.get("shopName") or _nested_value(item, "shop", "shopName"))
        comment = _normalize_text(_strip_html_tags(str(item.get("comment") or "")))
        catalog_tree = _coerce_text(_nested_value(item, "extFields", "catalogTree"))

        parts: list[str] = []
        if search_keyword:
            parts.append(f"关键词: {search_keyword}")
        if shop_name:
            parts.append(f"店铺: {shop_name}")
        if price:
            parts.append(f"价格: {price}")
        if cumulative_sale_count or sale_count:
            parts.append(f"销量: {cumulative_sale_count or sale_count}")
        if comment_count:
            parts.append(f"评价数: {comment_count}")
        if good_comment_count:
            parts.append(f"好评: {good_comment_count}")
        if city:
            parts.append(f"城市: {city}")
        if user_type:
            parts.append(f"商家类型: {user_type}")
        if standard_time:
            parts.append(f"交付: {standard_time}")
        if catalog_tree:
            parts.append(f"类目: {catalog_tree}")
        if comment:
            parts.append(f"说明: {comment}")

        url = source.get("default_url", source.get("location", ""))
        if service_id:
            url = detail_url_template.format(id=service_id)

        leads.append(
            Lead(
                source_name=source["name"],
                source_kind=source["kind"],
                category=source["category"],
                title=title,
                content="\n".join(parts).strip(),
                url=url,
                published_at="",
                price_text=price,
                price_value=price_value,
                sale_count=_extract_int(item.get("saleCount")),
                cumulative_sale_count=_extract_int(item.get("cumulativeSaleCount")),
                comment_count=_extract_int(item.get("commentCount")),
                good_comment_count=_extract_int(item.get("goodCommentCount")),
                delivery_text=standard_time,
            )
        )
    return leads


def fetch_zbj_demand_hall(raw: str, source: dict) -> list[Lead]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = None

    items: list[dict] = []
    if isinstance(data, list):
        items = [item for item in data if isinstance(item, dict)]
    elif isinstance(data, dict):
        candidates = [
            data.get("items"),
            data.get("list"),
            data.get("rows"),
            data.get("resultList"),
            data.get("records"),
            data.get("data"),
            _nested_value(data, "data", "items"),
            _nested_value(data, "data", "list"),
            _nested_value(data, "data", "rows"),
            _nested_value(data, "data", "records"),
            _nested_value(data, "result", "items"),
            _nested_value(data, "result", "list"),
        ]
        for candidate in candidates:
            if isinstance(candidate, list):
                items = [item for item in candidate if isinstance(item, dict)]
                if items:
                    break
    elif "window.__NUXT__=" in raw:
        items = _extract_zbj_nuxt_result_list(raw)

    if not items:
        return []

    base_url = source.get("base_url", source.get("default_url", source.get("location", "")))
    detail_url_template = str(source.get("detail_url_template") or "").strip()
    leads: list[Lead] = []
    seen: set[str] = set()

    for item in items:
        title = _normalize_text(
            _strip_html_tags(
                str(
                    _first_non_empty(
                        item,
                        "title",
                        "taskTitle",
                        "demandTitle",
                        "name",
                        "taskName",
                        "requireTitle",
                    )
                    or ""
                )
            )
        )
        if not title or len(title) < 4 or title in seen:
            continue
        seen.add(title)

        detail_url = _coerce_text(
            _first_non_empty(
                item,
                "url",
                "detailUrl",
                "taskUrl",
                "jumpUrl",
                "link",
                ("ext", "url"),
            )
        )
        demand_id = _coerce_text(
            _first_non_empty(
                item,
                "id",
                "taskId",
                "demandId",
                "requireId",
                "bizId",
                "uuid",
            )
        )
        if detail_url:
            final_url = urljoin(base_url, detail_url)
        elif demand_id and detail_url_template:
            final_url = detail_url_template.format(id=demand_id)
        elif demand_id:
            final_url = f"{source.get('default_url', source.get('location', ''))}#demand-{demand_id}"
        else:
            final_url = source.get("default_url", source.get("location", ""))

        budget_raw = _first_non_empty(
            item,
            "budget",
            "price",
            "amount",
            "offerAmount",
            "priceRange",
            "priceDesc",
            "expectedPrice",
            "expectPrice",
            ("budgetInfo", "budget"),
            ("budgetInfo", "price"),
        )
        budget_text = _coerce_text(budget_raw)
        budget_value = _extract_price_value(budget_raw)
        if not budget_text:
            min_budget = _coerce_text(_first_non_empty(item, "minBudget", ("budgetInfo", "min")))
            max_budget = _coerce_text(_first_non_empty(item, "maxBudget", ("budgetInfo", "max")))
            if min_budget and max_budget:
                budget_text = f"{min_budget} - {max_budget}"
                budget_value = _extract_price_value(min_budget)
            elif min_budget:
                budget_text = min_budget
                budget_value = _extract_price_value(min_budget)

        employer_name = _coerce_text(
            _first_non_empty(
                item,
                "employerName",
                "companyName",
                "nickName",
                "userName",
                "publisherName",
                "shopName",
                "uuid",
            )
        )
        city = _coerce_text(
            _first_non_empty(
                item,
                "city",
                "cityName",
                "region",
                "areaName",
                ("area", "name"),
                "provinceName",
            )
        )
        category_text = _coerce_text(
            _first_non_empty(
                item,
                "categoryName",
                "cateName",
                "industryName",
                "category3Name",
                "category2Name",
                "category1Name",
                ("category", "name"),
                ("industry", "name"),
            )
        )
        status_text = _coerce_text(_first_non_empty(item, "statusDesc", "status"))
        labels_text = _coerce_text(_first_non_empty(item, "attrLabels"))
        deadline_text = _coerce_text(
            _first_non_empty(
                item,
                "deadline",
                "deliveryCycle",
                "deliveryDays",
                "deliveryTime",
                "endTime",
                "expireTime",
            )
        )
        requirement_text = _normalize_text(
            _strip_html_tags(
                str(
                    _first_non_empty(
                        item,
                        "content",
                        "desc",
                        "description",
                        "summary",
                        "brief",
                        "taskDesc",
                        "demandDesc",
                        "requirement",
                    )
                    or ""
                )
            )
        )
        published_at = _coerce_text(
            _first_non_empty(
                item,
                "published_at",
                "publishTime",
                "publishedTime",
                "createTime",
                "updateTime",
                "gmtCreate",
                "originPublishTime",
            )
        )

        parts: list[str] = []
        if employer_name:
            parts.append(f"雇主: {employer_name}")
        if status_text:
            parts.append(f"状态: {status_text}")
        if budget_text:
            parts.append(f"预算: {budget_text}")
        if city:
            parts.append(f"地区: {city}")
        if category_text:
            parts.append(f"类目: {category_text}")
        if labels_text:
            parts.append(f"标签: {labels_text}")
        if deadline_text:
            parts.append(f"周期: {deadline_text}")
        if requirement_text:
            parts.append(requirement_text)

        exclude_status_keywords = [str(item).strip().lower() for item in source.get("exclude_status_keywords", []) if str(item).strip()]
        if exclude_status_keywords and any(keyword in status_text.lower() for keyword in exclude_status_keywords):
            continue

        if source.get("require_tech_demand", False) and not _is_zbj_tech_demand(
            title,
            category_text,
            requirement_text,
            labels_text,
            source,
        ):
            continue

        leads.append(
            Lead(
                source_name=source["name"],
                source_kind=source["kind"],
                category=source["category"],
                title=title,
                content="\n".join(parts).strip(),
                url=final_url,
                published_at=published_at,
                price_text=budget_text,
                price_value=budget_value,
                delivery_text=deadline_text,
            )
        )
    return leads


def fetch_zbj_demand_detail_legacy(raw: str, source: dict) -> list[Lead]:
    title = _extract_first_text(
        raw,
        [
            r'<meta[^>]+property="og:title"[^>]+content="(?P<value>[^"]+)"',
            r"<title>(?P<value>[^<]+)</title>",
            r"<h1[^>]*>(?P<value>.*?)</h1>",
            r"需求标题[:：]\s*(?P<value>[^\n<]{4,120})",
        ],
    )
    if title:
        title = re.sub(r"\s*[-|_].*$", "", title).strip()

    budget_text = _extract_first_text(
        raw,
        [
            r"预算[:：]\s*(?P<value>[0-9,\-~至元万千百 ]+)",
            r"报价[:：]\s*(?P<value>[0-9,\-~至元万千百 ]+)",
            r"priceDesc[:=]\s*[\"'](?P<value>[^\"']+)[\"']",
        ],
    )
    published_at = _extract_first_text(
        raw,
        [
            r"(?P<value>\d{4}-\d{2}-\d{2}发布)",
            r"发布时间[:：]\s*(?P<value>\d{4}-\d{2}-\d{2})",
        ],
    )
    deadline_text = _extract_first_text(
        raw,
        [
            r"周期[:：]\s*(?P<value>[^\n<]{1,40})",
            r"交付(?:周期|时间)?[:：]\s*(?P<value>[^\n<]{1,40})",
        ],
    )
    city = _extract_first_text(
        raw,
        [
            r"地区[:：]\s*(?P<value>[^\n<]{1,40})",
            r"城市[:：]\s*(?P<value>[^\n<]{1,40})",
        ],
    )
    status_text = _extract_first_text(
        raw,
        [
            r"状态[:：]\s*(?P<value>[^\n<]{1,20})",
            r"(?P<value>进行中|待服务商报价|已完结)",
        ],
    )
    requirement_text = _extract_first_text(
        raw,
        [
            r"需求描述[:：]\s*(?P<value>.*?)(?:联系雇主|预算[:：]|发布时间[:：]|$)",
            r"任务描述[:：]\s*(?P<value>.*?)(?:联系雇主|预算[:：]|发布时间[:：]|$)",
            r"content[:=]\s*[\"'](?P<value>.*?)[\"']",
        ],
    )

    if title == "猪八戒网" and not any([budget_text, published_at, deadline_text, city, status_text, requirement_text]):
        return []
    if not title and not requirement_text:
        return []

    content_parts: list[str] = []
    if status_text:
        content_parts.append(f"状态: {status_text}")
    if budget_text:
        content_parts.append(f"预算: {budget_text}")
    if city:
        content_parts.append(f"地区: {city}")
    if deadline_text:
        content_parts.append(f"周期: {deadline_text}")
    if requirement_text:
        content_parts.append(requirement_text)

    return [
        Lead(
            source_name=source["name"],
            source_kind=source["kind"],
            category=source["category"],
            title=title or source.get("default_title", "猪八戒需求详情"),
            content="\n".join(content_parts).strip(),
            url=source.get("default_url", source.get("location", "")),
            published_at=published_at,
            price_text=budget_text,
            price_value=_extract_price_value(budget_text),
            delivery_text=deadline_text,
        )
    ]


def fetch_zbj_demand_detail(raw: str, source: dict) -> list[Lead]:
    title = _extract_first_text(
        raw,
        [
            r'<meta[^>]+property="og:title"[^>]+content="(?P<value>[^"]+)"',
            r"<title>(?P<value>[^<]+)</title>",
            r"<h1[^>]*>(?P<value>.*?)</h1>",
            r'"title"\s*:\s*"(?P<value>[^"]{4,120})"',
            r'"taskTitle"\s*:\s*"(?P<value>[^"]{4,120})"',
            r'"demandTitle"\s*:\s*"(?P<value>[^"]{4,120})"',
            r"需求标题[:：]?\s*(?P<value>[^\n<]{4,120})",
        ],
    )
    if title:
        title = re.sub(r"\s*[-|_].*$", "", title).strip()

    budget_text = _extract_first_text(
        raw,
        [
            r"预算[:：]?\s*(?P<value>[0-9,\-~至元万千百]+)",
            r"报价[:：]?\s*(?P<value>[0-9,\-~至元万千百]+)",
            r'priceDesc[:=]\s*["\'](?P<value>[^"\']+)["\']',
            r'"priceDesc"\s*:\s*"(?P<value>[^"]+)"',
            r'"budget"\s*:\s*"(?P<value>[^"]+)"',
        ],
    )
    published_at = _extract_first_text(
        raw,
        [
            r"(?P<value>\d{4}-\d{2}-\d{2}发布)",
            r"发布时间[:：]?\s*(?P<value>\d{4}-\d{2}-\d{2})",
            r'"publishTime"\s*:\s*"(?P<value>[^"]+)"',
            r'"publishedAt"\s*:\s*"(?P<value>[^"]+)"',
            r'"createTime"\s*:\s*"(?P<value>[^"]+)"',
        ],
    )
    deadline_text = _extract_first_text(
        raw,
        [
            r"周期[:：]?\s*(?P<value>[^\n<]{1,40})",
            r"交付(?:周期|时间)?[:：]?\s*(?P<value>[^\n<]{1,40})",
            r'"delivery(?:Cycle|Time|Days)?"\s*:\s*"(?P<value>[^"]+)"',
        ],
    )
    city = _extract_first_text(
        raw,
        [
            r"地区[:：]?\s*(?P<value>[^\n<]{1,40})",
            r"城市[:：]?\s*(?P<value>[^\n<]{1,40})",
            r'"cityName"\s*:\s*"(?P<value>[^"]+)"',
            r'"provinceName"\s*:\s*"(?P<value>[^"]+)"',
        ],
    )
    status_text = _extract_first_text(
        raw,
        [
            r"状态[:：]?\s*(?P<value>[^\n<]{1,20})",
            r"(?P<value>进行中|待服务商报价|已完结)",
            r'"statusDesc"\s*:\s*"(?P<value>[^"]+)"',
        ],
    )
    requirement_text = _extract_first_text(
        raw,
        [
            r"需求描述[:：]?\s*(?P<value>.*?)(?:联系雇主|预算[:：]?|发布时间[:：]?|$)",
            r"任务描述[:：]?\s*(?P<value>.*?)(?:联系雇主|预算[:：]?|发布时间[:：]?|$)",
            r'"content"\s*:\s*"(?P<value>.*?)"',
            r'"desc"\s*:\s*"(?P<value>.*?)"',
            r'"description"\s*:\s*"(?P<value>.*?)"',
            r'"taskDesc"\s*:\s*"(?P<value>.*?)"',
            r'"demandDesc"\s*:\s*"(?P<value>.*?)"',
        ],
    )

    lower_raw = raw.lower()
    if any(marker in lower_raw for marker in ["geetest", "拖动滑块继续访问", "_verify=1", "verify.zbj.com"]):
        return []
    if title == "猪八戒网" and not any([budget_text, published_at, deadline_text, city, status_text, requirement_text]):
        return []
    if not title and not requirement_text:
        return []

    content_parts: list[str] = []
    if status_text:
        content_parts.append(f"状态: {status_text}")
    if budget_text:
        content_parts.append(f"预算: {budget_text}")
    if city:
        content_parts.append(f"地区: {city}")
    if deadline_text:
        content_parts.append(f"周期: {deadline_text}")
    if requirement_text:
        content_parts.append(requirement_text)

    return [
        Lead(
            source_name=source["name"],
            source_kind=source["kind"],
            category=source["category"],
            title=title or source.get("default_title", "猪八戒需求详情"),
            content="\n".join(content_parts).strip(),
            url=source.get("default_url", source.get("location", "")),
            published_at=published_at,
            price_text=budget_text,
            price_value=_extract_price_value(budget_text),
            delivery_text=deadline_text,
        )
    ]


def fetch_zbj_service_detail(raw: str, source: dict) -> list[Lead]:
    title_patterns = [
        re.compile(r'<meta[^>]+property="og:title"[^>]+content="(?P<title>[^"]+)"', re.IGNORECASE),
        re.compile(r"<title>(?P<title>[^<]+)</title>", re.IGNORECASE),
        re.compile(r"<h1[^>]*>(?P<title>.*?)</h1>", re.IGNORECASE),
    ]
    title = ""
    for pattern in title_patterns:
        match = pattern.search(raw)
        if match:
            title = _strip_html_tags(match.group("title"))
            break
    if not title:
        return []

    summary_patterns = [
        re.compile(r'<meta[^>]+name="description"[^>]+content="(?P<desc>[^"]+)"', re.IGNORECASE),
        re.compile(r'服务卖点\s*</[^>]+>\s*<[^>]+>(?P<desc>.*?)</', re.IGNORECASE),
    ]
    content_parts: list[str] = []
    sale_count = _extract_first_int(
        raw,
        [
            r"近半年销量\s*(?P<value>\d+)",
            r"销量\s*(?P<value>\d+)",
            r"成交\s*(?P<value>\d+)",
        ],
    )
    cumulative_sale_count = _extract_first_int(
        raw,
        [
            r"累计销量\s*(?P<value>\d+)",
            r"累计成交\s*(?P<value>\d+)",
        ],
    )
    comment_count = _extract_first_int(
        raw,
        [
            r"评价(?:数)?\s*(?P<value>\d+)",
            r"评论(?:数)?\s*(?P<value>\d+)",
        ],
    )
    good_comment_count = _extract_first_int(
        raw,
        [
            r"好评(?:数)?\s*(?P<value>\d+)",
        ],
    )
    delivery_text = _extract_first_text(
        raw,
        [
            r"(?P<value>\d+\s*天交付)",
            r"交付(?:时间)?[:：]?\s*(?P<value>[^\s<]{1,20})",
        ],
    )
    for pattern in summary_patterns:
        match = pattern.search(raw)
        if match:
            snippet = _strip_html_tags(match.groupdict().get("desc", "")).strip()
            if snippet:
                content_parts.append(snippet)

    price_matches = re.findall(r"¥\s*([0-9][0-9,\.]*)", raw, re.IGNORECASE)
    price_text = ""
    price_value = 0.0
    if price_matches:
        unique_prices = []
        for value in price_matches:
            cleaned = value.replace(",", "")
            if cleaned not in unique_prices:
                unique_prices.append(cleaned)
        price_text = " / ".join(f"¥{item}" for item in unique_prices[:3])
        price_value = float(unique_prices[0]) if unique_prices else 0.0
        content_parts.append(f"Price: {' / '.join(unique_prices[:3])}")
    if cumulative_sale_count or sale_count:
        content_parts.append(f"销量: {cumulative_sale_count or sale_count}")
    if comment_count:
        content_parts.append(f"评价数: {comment_count}")
    if good_comment_count:
        content_parts.append(f"好评: {good_comment_count}")
    if delivery_text:
        content_parts.append(f"交付: {delivery_text}")

    return [
        Lead(
            source_name=source["name"],
            source_kind=source["kind"],
            category=source["category"],
            title=re.sub(r"\s+", " ", title).strip(),
            content="\n".join(content_parts).strip(),
            url=source.get("default_url", source.get("location", "")),
            published_at="",
            price_text=price_text,
            price_value=price_value,
            sale_count=sale_count,
            cumulative_sale_count=cumulative_sale_count,
            comment_count=comment_count,
            good_comment_count=good_comment_count,
            delivery_text=delivery_text,
        )
    ]


def fetch_xianyu_service_list(raw: str, source: dict) -> list[Lead]:
    data = json.loads(raw)
    items: list[dict[str, Any]] = []
    if isinstance(data, dict):
        candidates = [
            data.get("items"),
            _nested_value(data, "data", "items"),
            _nested_value(data, "data", "list"),
            _nested_value(data, "data", "result", "items"),
            _nested_value(data, "data", "resultList"),
            _nested_value(data, "result", "items"),
            _nested_value(data, "result", "list"),
            _nested_value(data, "list"),
        ]
        for candidate in candidates:
            if isinstance(candidate, list):
                items = [item for item in candidate if isinstance(item, dict)]
                if items:
                    break
    elif isinstance(data, list):
        items = [item for item in data if isinstance(item, dict)]

    if not items:
        return []

    base_url = str(source.get("base_url") or source.get("default_url") or source.get("location") or "").strip()
    detail_url_template = str(source.get("detail_url_template") or "").strip()
    leads: list[Lead] = []
    seen: set[str] = set()

    for item in items:
        title = _coerce_text(
            _first_non_empty(
                item,
                "title",
                "itemTitle",
                "name",
                "subject",
                "serviceTitle",
                ("item", "title"),
            )
        )
        if not title or len(title) < 4 or title in seen:
            continue
        seen.add(title)

        detail_url = _coerce_text(
            _first_non_empty(
                item,
                "url",
                "itemUrl",
                "detailUrl",
                "jumpUrl",
                "targetUrl",
                ("item", "url"),
            )
        )
        item_id = _coerce_text(_first_non_empty(item, "id", "itemId", "bizId", ("item", "id")))
        final_url = urljoin(base_url, detail_url) if detail_url else ""
        if not final_url and item_id and detail_url_template:
            final_url = detail_url_template.format(id=item_id)
        if not final_url:
            final_url = base_url

        price_raw = _first_non_empty(
            item,
            "price",
            "priceText",
            "displayPrice",
            "amount",
            ("priceInfo", "price"),
            ("priceInfo", "priceText"),
            ("priceInfo", "displayPrice"),
        )
        price_text = _coerce_text(price_raw)
        price_value = _extract_price_value(price_raw)

        seller_text = _coerce_text(
            _first_non_empty(
                item,
                "sellerName",
                "userName",
                "nickName",
                "accountName",
                ("seller", "name"),
                ("seller", "nick"),
                ("user", "nick"),
            )
        )
        city_text = _coerce_text(
            _first_non_empty(
                item,
                "cityName",
                "city",
                "location",
                "areaName",
                ("location", "city"),
                ("location", "name"),
            )
        )
        tags_text = _coerce_text(_first_non_empty(item, "tags", "serviceTags", "labels", ("item", "tags")))
        summary_text = _coerce_text(
            _first_non_empty(
                item,
                "content",
                "summary",
                "desc",
                "description",
                "subtitle",
                "subTitle",
                ("item", "summary"),
            )
        )
        published_at = _coerce_text(_first_non_empty(item, "publishTime", "published_at", "createTime", "gmtCreate"))

        content_parts: list[str] = []
        if price_text:
            content_parts.append(f"Price: {price_text}")
        if seller_text:
            content_parts.append(f"Seller: {seller_text}")
        if city_text:
            content_parts.append(f"City: {city_text}")
        if tags_text:
            content_parts.append(f"Tags: {tags_text}")
        if summary_text:
            content_parts.append(summary_text)

        leads.append(
            Lead(
                source_name=source["name"],
                source_kind=source["kind"],
                category=source["category"],
                title=title,
                content="\n".join(content_parts).strip(),
                url=final_url,
                published_at=published_at,
                price_text=price_text,
                price_value=price_value,
            )
        )
    return leads


def fetch_remoteok_jobs(raw: str, source: dict) -> list[Lead]:
    data = json.loads(raw)
    if not isinstance(data, list):
        return []

    leads: list[Lead] = []
    seen: set[str] = set()
    for item in data:
        if not isinstance(item, dict):
            continue
        position = _coerce_text(_first_non_empty(item, "position", "title", "role"))
        company = _coerce_text(_first_non_empty(item, "company", "company_name"))
        job_id = _coerce_text(_first_non_empty(item, "id", "slug"))
        if not position or not job_id:
            continue

        url = _coerce_text(_first_non_empty(item, "url", "apply_url"))
        if not url:
            slug = _coerce_text(_first_non_empty(item, "slug"))
            if slug:
                url = urljoin(str(source.get("base_url") or "https://remoteok.com/"), f"/remote-jobs/{slug}")
        if not url:
            continue

        title = f"{company} - {position}" if company else position
        if title in seen:
            continue
        seen.add(title)

        tags = _coerce_text(_first_non_empty(item, "tags"))
        location = _coerce_text(_first_non_empty(item, "location"))
        description = _coerce_text(_first_non_empty(item, "description"))
        salary_min = int(item.get("salary_min") or 0) if isinstance(item.get("salary_min"), (int, float, str)) else 0
        salary_max = int(item.get("salary_max") or 0) if isinstance(item.get("salary_max"), (int, float, str)) else 0
        salary_parts = [str(value) for value in (salary_min, salary_max) if value > 0]
        price_text = ""
        if len(salary_parts) == 2:
            price_text = f"${salary_parts[0]}-${salary_parts[1]}/year"
        elif len(salary_parts) == 1:
            price_text = f"${salary_parts[0]}/year"

        content_parts: list[str] = []
        if company:
            content_parts.append(f"Company: {company}")
        if location:
            content_parts.append(f"Location: {location}")
        if tags:
            content_parts.append(f"Tags: {tags}")
        if price_text:
            content_parts.append(f"Salary: {price_text}")
        if description:
            content_parts.append(description)

        leads.append(
            Lead(
                source_name=source["name"],
                source_kind=source["kind"],
                category=source["category"],
                title=title,
                content="\n".join(content_parts).strip(),
                url=url,
                published_at=_coerce_text(_first_non_empty(item, "date")),
                price_text=price_text,
                price_value=float(salary_max or salary_min or 0),
            )
        )
    return leads


def fetch_ccgp_procurement_list(raw: str, source: dict) -> list[Lead]:
    pattern = re.compile(
        r"<li>\s*<a[^>]+href=\"(?P<href>[^\"]+)\"[^>]+title=\"(?P<title>[^\"]+)\"[^>]*>.*?</a>\s*"
        r"发布时间：<em>(?P<published_at>[^<]+)</em>\s*"
        r"地域：<em>(?P<region>[^<]+)</em>\s*"
        r"采购人：<em>(?P<purchaser>[^<]+)</em>",
        re.IGNORECASE | re.DOTALL,
    )
    base_url = str(source.get("base_url") or source.get("default_url") or source.get("location") or "").strip()
    leads: list[Lead] = []
    seen: set[str] = set()

    for match in pattern.finditer(raw):
        title = _coerce_text(match.group("title"))
        if not title or title in seen:
            continue
        seen.add(title)
        href = _coerce_text(match.group("href"))
        region = _coerce_text(match.group("region"))
        purchaser = _coerce_text(match.group("purchaser"))
        published_at = _coerce_text(match.group("published_at"))

        content_parts: list[str] = []
        if region:
            content_parts.append(f"Region: {region}")
        if purchaser:
            content_parts.append(f"Purchaser: {purchaser}")

        leads.append(
            Lead(
                source_name=source["name"],
                source_kind=source["kind"],
                category=source["category"],
                title=title,
                content="\n".join(content_parts).strip(),
                url=urljoin(base_url, href),
                published_at=published_at,
            )
        )
    return leads


def fetch_cnode_topics(raw: str, source: dict) -> list[Lead]:
    data = json.loads(raw)
    items = data.get("data") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return []

    base_url = str(source.get("base_url") or "https://cnodejs.org").strip()
    leads: list[Lead] = []
    seen: set[str] = set()

    for item in items:
        if not isinstance(item, dict):
            continue
        title = _coerce_text(_first_non_empty(item, "title"))
        topic_id = _coerce_text(_first_non_empty(item, "id"))
        if not title or not topic_id or title in seen:
            continue
        seen.add(title)

        author_name = _coerce_text(_first_non_empty(item, ("author", "loginname")))
        reply_count = _extract_int(item.get("reply_count"))
        visit_count = _extract_int(item.get("visit_count"))
        tab = _coerce_text(_first_non_empty(item, "tab"))
        content_text = _coerce_text(_first_non_empty(item, "content"))

        content_parts: list[str] = []
        if author_name:
            content_parts.append(f"Author: {author_name}")
        if tab:
            content_parts.append(f"Tab: {tab}")
        if reply_count:
            content_parts.append(f"Replies: {reply_count}")
        if visit_count:
            content_parts.append(f"Visits: {visit_count}")
        if content_text:
            content_parts.append(content_text)

        leads.append(
            Lead(
                source_name=source["name"],
                source_kind=source["kind"],
                category=source["category"],
                title=title,
                content="\n".join(content_parts).strip(),
                url=urljoin(base_url, f"/topic/{topic_id}"),
                published_at=_coerce_text(_first_non_empty(item, "create_at", "last_reply_at")),
                comment_count=reply_count,
            )
        )
    return leads


def fetch_json_items(raw: str, source: dict) -> list[Lead]:
    data = json.loads(raw)
    items_path = source["json_items_path"]
    value: object = data
    for segment in items_path.split("."):
        if not isinstance(value, dict):
            return []
        value = value.get(segment)
    if not isinstance(value, list):
        return []

    field_map = source.get("field_map", {})
    detail_url_template = str(source.get("detail_url_template") or "").strip()
    default_url = str(source.get("default_url") or source.get("location") or "").strip()
    leads: list[Lead] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        title = str(item.get(field_map.get("title", "title"), "")).strip()
        content = str(item.get(field_map.get("content", "content"), "")).strip()
        url = str(item.get(field_map.get("url", "url"), "")).strip()
        item_id = str(item.get(field_map.get("id", "id"), "")).strip()
        published_at = str(item.get(field_map.get("published_at", "published_at"), "")).strip()
        if not url and detail_url_template and item_id:
            url = detail_url_template.format(id=item_id)
        if not url:
            url = default_url
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
    return leads
