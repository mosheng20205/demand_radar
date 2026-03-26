from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


def _ensure_playwright() -> None:
    try:
        import playwright.sync_api  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "Playwright is required for Cloak CDP fetching. Install with: pip install playwright && python -m playwright install chromium"
        ) from exc


def _normalize_cdp_endpoint(open_data: dict[str, Any]) -> str:
    http_endpoint = str(open_data.get("http") or "").strip()
    if http_endpoint:
        if http_endpoint.startswith(("http://", "https://", "ws://", "wss://")):
            return http_endpoint
        return f"http://{http_endpoint}"
    ws_endpoint = str(open_data.get("ws") or "").strip()
    if ws_endpoint:
        return ws_endpoint
    raise RuntimeError("Cloak browser open response does not contain a CDP endpoint.")


class _SafeFormatDict(dict[str, Any]):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _format_template(value: str, context: dict[str, Any]) -> str:
    if not value or "{" not in value:
        return value
    try:
        return value.format_map(_SafeFormatDict(context))
    except Exception:
        return value


def _build_format_context(source: dict[str, Any]) -> dict[str, Any]:
    page_number = int(source.get("page_number", 1) or 1)
    return {
        "page_number": page_number,
        "target_page": page_number,
        "location": str(source.get("location") or ""),
        "default_url": str(source.get("default_url") or source.get("location") or ""),
        "source_name": str(source.get("name") or ""),
    }


def _matches_any_pattern(value: str, patterns: list[str]) -> bool:
    if not patterns:
        return True
    lowered = value.lower()
    return any(pattern.lower() in lowered for pattern in patterns if pattern)


def _normalize_action(action: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(action)
    for key, value in list(normalized.items()):
        if not isinstance(value, str):
            continue
        if key in {"script", "extract_js"}:
            continue
        normalized[key] = _format_template(value, context)
    return normalized


def _click_pagination(page: Any, target_page: int, timeout_ms: int) -> None:
    href_candidates = [
        f'a[href*="/xq/?p={target_page}"]',
        f'a[href*="?p={target_page}"]',
        f'a[href*="&p={target_page}"]',
    ]
    for selector in href_candidates:
        locator = page.locator(selector)
        if locator.count():
            locator.first.click(timeout=timeout_ms)
            page.wait_for_timeout(min(timeout_ms, 4000))
            return

    click_script = """
    ({ targetPage }) => {
      const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
      const elements = Array.from(document.querySelectorAll('a, button, li, span, div'));
      const isVisible = (el) => {
        if (!el || !(el instanceof HTMLElement)) return false;
        const style = window.getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style && style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
      };
      const clickable = elements.filter(isVisible);
      const hrefExact = clickable.find((el) => {
        const href = normalize(el.getAttribute('href'));
        return href.includes(`?p=${targetPage}`) || href.includes(`&p=${targetPage}`);
      });
      if (hrefExact) {
        hrefExact.click();
        return { strategy: 'href', text: normalize(hrefExact.textContent) };
      }
      const exact = clickable.find((el) => {
        const text = normalize(el.textContent);
        if (text !== String(targetPage)) return false;
        const cls = normalize(el.className || '');
        return !/disabled|active|current/.test(cls.toLowerCase()) || el.tagName.toLowerCase() === 'a';
      });
      if (exact) {
        exact.click();
        return { strategy: 'exact', text: normalize(exact.textContent) };
      }
      const datasetExact = clickable.find((el) => {
        const attrs = [el.getAttribute('data-page'), el.getAttribute('data-pagenum'), el.getAttribute('page'), el.getAttribute('aria-label')];
        return attrs.some((item) => normalize(item) === String(targetPage));
      });
      if (datasetExact) {
        datasetExact.click();
        return { strategy: 'attribute', text: normalize(datasetExact.textContent) };
      }
      const nextCandidates = clickable.filter((el) => /下一页|next/i.test(normalize(el.textContent)) || /next/.test(normalize(el.className || '').toLowerCase()));
      if (nextCandidates.length && targetPage > 1) {
        for (let index = 1; index < targetPage; index += 1) {
          const candidate = nextCandidates[0];
          if (!candidate) break;
          candidate.click();
        }
        return { strategy: 'next', text: normalize(nextCandidates[0].textContent) };
      }
      return null;
    }
    """
    result = page.evaluate(click_script, {"targetPage": target_page})
    if not result:
        raise RuntimeError(f"Could not locate pagination control for target page {target_page}.")
    page.wait_for_timeout(min(timeout_ms, 4000))


def _run_actions(page: Any, actions: list[dict[str, Any]], timeout_ms: int, context: dict[str, Any]) -> None:
    if not actions:
        return
    for raw_action in actions:
        action = _normalize_action(raw_action, context)
        action_type = str(action.get("type") or "").strip().lower()
        if action_type in {"", "wait"}:
            page.wait_for_timeout(int(action.get("timeout_ms", action.get("wait_ms", 1000)) or 1000))
            continue
        if action_type == "wait_for_selector":
            selector = str(action.get("selector") or "").strip()
            if not selector:
                raise RuntimeError("wait_for_selector action requires selector.")
            page.wait_for_selector(selector, timeout=int(action.get("timeout_ms", timeout_ms) or timeout_ms))
            continue
        if action_type == "click":
            selector = str(action.get("selector") or "").strip()
            if not selector:
                raise RuntimeError("click action requires selector.")
            page.locator(selector).first.click(timeout=int(action.get("timeout_ms", timeout_ms) or timeout_ms))
            page.wait_for_timeout(int(action.get("wait_after_ms", 1200) or 1200))
            continue
        if action_type == "click_text":
            text = str(action.get("text") or "").strip()
            if not text:
                raise RuntimeError("click_text action requires text.")
            page.get_by_text(text, exact=bool(action.get("exact", True))).first.click(
                timeout=int(action.get("timeout_ms", timeout_ms) or timeout_ms)
            )
            page.wait_for_timeout(int(action.get("wait_after_ms", 1200) or 1200))
            continue
        if action_type == "evaluate":
            script = str(action.get("script") or "").strip()
            if not script:
                raise RuntimeError("evaluate action requires script.")
            argument = action.get("argument")
            if argument is None:
                page.evaluate(script)
            else:
                page.evaluate(script, argument)
            page.wait_for_timeout(int(action.get("wait_after_ms", 1200) or 1200))
            continue
        if action_type == "click_pagination":
            target_page_raw = action.get("target_page", context.get("target_page", 1))
            target_page = int(target_page_raw or 1)
            clicked = False
            if target_page > 1:
                try:
                    _click_pagination(page, target_page, int(action.get("timeout_ms", timeout_ms) or timeout_ms))
                    clicked = True
                except RuntimeError:
                    if not bool(action.get("optional", False) or action.get("allow_missing", False)):
                        raise
            wait_selector = str(action.get("wait_for_selector") or "").strip()
            if wait_selector and clicked:
                page.wait_for_selector(wait_selector, timeout=int(action.get("timeout_ms", timeout_ms) or timeout_ms))
            if clicked:
                page.wait_for_timeout(int(action.get("wait_after_ms", 2500) or 2500))
            continue
        raise RuntimeError(f"Unsupported page action type: {action_type}")


def _page_has_target_content(page: Any, target_url: str) -> bool:
    if page.is_closed():
        return False
    current_url = str(page.url or "").strip()
    if not current_url or current_url == "about:blank":
        return False

    target_host = urlsplit(target_url).netloc.lower()
    current_host = urlsplit(current_url).netloc.lower()
    if target_host and current_host and target_host != current_host:
        return False

    try:
        title = str(page.title() or "").strip()
    except Exception:
        title = ""
    try:
        html_length = len(page.content())
    except Exception:
        html_length = 0
    return bool(title) or html_length >= 1000


def _goto_target_page(page: Any, url: str, wait_until: str, timeout_ms: int, *, owns_page: bool) -> None:
    from playwright.sync_api import Error

    for attempt in range(2):
        try:
            page.goto(url, wait_until=wait_until, timeout=timeout_ms)
            return
        except Error as exc:
            if "ERR_ABORTED" not in str(exc) or attempt > 0:
                raise
            page.wait_for_timeout(1500)
            try:
                page.wait_for_load_state("domcontentloaded", timeout=min(timeout_ms, 5000))
            except Error:
                pass
            if _page_has_target_content(page, url):
                return
            try:
                page.goto("about:blank", wait_until="load", timeout=min(timeout_ms, 5000))
            except Error:
                if owns_page:
                    raise


def _prepare_page_capture(
    page: Any,
    *,
    wait_selector: str,
    timeout_ms: int,
    post_load_wait_ms: int,
    scroll_steps: int,
    scroll_pause_ms: int,
    page_actions: list[dict[str, Any]],
    context: dict[str, Any],
) -> None:
    if wait_selector:
        page.wait_for_selector(wait_selector, timeout=timeout_ms)
    if post_load_wait_ms > 0:
        page.wait_for_timeout(post_load_wait_ms)
    for _ in range(scroll_steps):
        page.mouse.wheel(0, 2400)
        page.wait_for_timeout(scroll_pause_ms)
    _run_actions(page, page_actions, timeout_ms, context)


def _is_empty_extracted_data(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (str, bytes, list, tuple, set, dict)):
        return len(value) == 0
    return False


def _matches_html_markers(html: str, markers: list[str]) -> bool:
    if not markers:
        return True
    lowered = html.lower()
    return any(marker.lower() in lowered for marker in markers if marker)


def _page_match_score(page: Any, target_url: str) -> int:
    if page.is_closed():
        return -1
    current_url = str(page.url or "").strip()
    if not current_url:
        return 0
    if current_url == "about:blank":
        return 1
    score = 10
    target_host = urlsplit(target_url).netloc.lower()
    current_host = urlsplit(current_url).netloc.lower()
    if target_host and current_host == target_host:
        score += 100
    return score


def _acquire_cdp_page(
    browser: Any,
    *,
    target_url: str,
    attached_to_running_browser: bool,
    timeout_ms: int,
    allow_create_page_if_missing: bool,
) -> tuple[Any, bool]:
    wait_deadline = time.time() + max(min(timeout_ms / 1000, 10), 3)
    while time.time() < wait_deadline:
        best_page = None
        best_score = -1
        for context in browser.contexts:
            for item in context.pages:
                score = _page_match_score(item, target_url)
                if score > best_score:
                    best_page = item
                    best_score = score
        if best_page is not None:
            return best_page, False
        time.sleep(0.5)

    if attached_to_running_browser and not allow_create_page_if_missing:
        raise RuntimeError("CDP connected to a running Cloak browser but no reusable page was found.")

    contexts = list(browser.contexts)
    if not contexts:
        raise RuntimeError("CDP connected to Cloak browser but no browser context became available.")
    page = contexts[0].new_page()
    return page, True


def fetch_page_via_cdp(open_data: dict[str, Any], source: dict[str, Any], base_dir: str | Path) -> dict[str, Any]:
    _ensure_playwright()
    from playwright.sync_api import Error, sync_playwright

    endpoint = _normalize_cdp_endpoint(open_data)
    url = str(source.get("location") or source.get("url") or "").strip()
    if not url:
        raise ValueError("Cloak CDP source requires location or url.")
    format_context = _build_format_context(source)

    wait_until = str(source.get("wait_until", "domcontentloaded"))
    timeout_ms = int(source.get("timeout_ms", source.get("timeout_seconds", 30) * 1000))
    connect_retries = int(source.get("connect_retries", 6) or 6)
    connect_backoff_ms = int(source.get("connect_backoff_ms", 1000) or 1000)
    wait_selector = str(source.get("wait_selector") or "").strip()
    post_load_wait_ms = int(source.get("post_load_wait_ms", 0) or 0)
    scroll_steps = int(source.get("scroll_steps", 0) or 0)
    scroll_pause_ms = int(source.get("scroll_pause_ms", 800) or 800)
    screenshot_path = _format_template(str(source.get("screenshot_path") or "").strip(), format_context)
    rendered_html_path = _format_template(str(source.get("rendered_html_path") or "").strip(), format_context)
    extract_js = str(source.get("extract_js") or "").strip()
    extracted_json_path = _format_template(str(source.get("extracted_json_path") or "").strip(), format_context)
    network_log_path = _format_template(str(source.get("network_log_path") or "").strip(), format_context)
    capture_network = bool(source.get("capture_network", False) or network_log_path)
    capture_response_body = bool(source.get("capture_response_body", False))
    response_body_max_chars = int(source.get("response_body_max_chars", 4000) or 4000)
    capture_resource_types = [str(item).strip().lower() for item in source.get("capture_resource_types", ["xhr", "fetch", "document"]) if str(item).strip()]
    capture_url_patterns = [str(item).strip() for item in source.get("capture_url_patterns", []) if str(item).strip()]
    page_actions = list(source.get("page_actions", []) or [])
    retry_on_empty_attempts = int(source.get("retry_on_empty_attempts", 0) or 0)
    retry_on_empty_wait_ms = int(source.get("retry_on_empty_wait_ms", 1500) or 1500)
    retry_on_empty_reset_to_blank = bool(source.get("retry_on_empty_reset_to_blank", False))
    retry_on_empty_html_markers = [
        str(item).strip()
        for item in source.get("retry_on_empty_html_markers", [])
        if str(item).strip()
    ]
    retry_wait_selector = str(source.get("retry_wait_selector") or wait_selector).strip()
    retry_post_load_wait_ms = int(source.get("retry_post_load_wait_ms", post_load_wait_ms) or 0)
    retry_scroll_steps = int(source.get("retry_scroll_steps", scroll_steps) or 0)
    retry_scroll_pause_ms = int(source.get("retry_scroll_pause_ms", scroll_pause_ms) or scroll_pause_ms)
    retry_page_actions = list(source.get("retry_page_actions", page_actions) or page_actions)
    allow_create_page_if_missing = bool(source.get("allow_create_page_if_missing", True))
    captured_network: list[dict[str, Any]] = []

    with sync_playwright() as playwright:
        last_error: Exception | None = None
        browser = None
        for attempt in range(connect_retries + 1):
            try:
                browser = playwright.chromium.connect_over_cdp(endpoint)
                break
            except Exception as exc:
                last_error = exc
                if attempt >= connect_retries:
                    raise RuntimeError(f"CDP connect failed for endpoint {endpoint}: {exc}") from exc
                time.sleep(connect_backoff_ms / 1000)

        if browser is None:
            raise RuntimeError(f"CDP connect failed for endpoint {endpoint}: {last_error}")
        attached_to_running_browser = bool(open_data.get("already_running"))
        page, owns_page = _acquire_cdp_page(
            browser,
            target_url=url,
            attached_to_running_browser=attached_to_running_browser,
            timeout_ms=timeout_ms,
            allow_create_page_if_missing=allow_create_page_if_missing,
        )
        try:
            if capture_network:
                def _record_response(response: Any) -> None:
                    try:
                        request = response.request
                        resource_type = str(request.resource_type or "").lower()
                        url_value = str(response.url or "")
                        if capture_resource_types and resource_type not in capture_resource_types:
                            return
                        if not _matches_any_pattern(url_value, capture_url_patterns):
                            return
                        entry: dict[str, Any] = {
                            "url": url_value,
                            "method": str(request.method or ""),
                            "resource_type": resource_type,
                            "status": int(response.status or 0),
                            "content_type": str(response.headers.get("content-type") or ""),
                            "post_data": request.post_data or "",
                        }
                        if capture_response_body and ("json" in entry["content_type"].lower() or resource_type in {"xhr", "fetch"}):
                            try:
                                body_bytes = response.body()
                                body_text = body_bytes.decode("utf-8", errors="ignore")
                            except Exception:
                                try:
                                    body_text = response.text()
                                except Exception:
                                    body_text = ""
                            if len(body_text) > response_body_max_chars:
                                body_text = body_text[:response_body_max_chars]
                            entry["body_preview"] = body_text
                        captured_network.append(entry)
                    except Exception:
                        return

                page.on("response", _record_response)
            html = ""
            title = ""
            final_url = ""
            extracted_data: Any = None
            total_attempts = max(retry_on_empty_attempts, 0) + 1
            for attempt in range(total_attempts):
                if attempt == 0:
                    _goto_target_page(page, url, wait_until, timeout_ms, owns_page=owns_page)
                    _prepare_page_capture(
                        page,
                        wait_selector=wait_selector,
                        timeout_ms=timeout_ms,
                        post_load_wait_ms=post_load_wait_ms,
                        scroll_steps=scroll_steps,
                        scroll_pause_ms=scroll_pause_ms,
                        page_actions=page_actions,
                        context=format_context,
                    )
                else:
                    page.wait_for_timeout(retry_on_empty_wait_ms)
                    if retry_on_empty_reset_to_blank:
                        try:
                            page.goto("about:blank", wait_until="load", timeout=min(timeout_ms, 5000))
                        except Error:
                            if owns_page:
                                raise
                    _goto_target_page(page, url, wait_until, timeout_ms, owns_page=owns_page)
                    _prepare_page_capture(
                        page,
                        wait_selector=retry_wait_selector,
                        timeout_ms=timeout_ms,
                        post_load_wait_ms=retry_post_load_wait_ms,
                        scroll_steps=retry_scroll_steps,
                        scroll_pause_ms=retry_scroll_pause_ms,
                        page_actions=retry_page_actions,
                        context=format_context,
                    )

                html = page.content()
                title = page.title()
                final_url = page.url
                extracted_data = page.evaluate(extract_js) if extract_js else None

                if attempt + 1 >= total_attempts:
                    break
                if not extract_js or not _is_empty_extracted_data(extracted_data):
                    break
                if not _matches_html_markers(html, retry_on_empty_html_markers):
                    break
            if screenshot_path:
                resolved_screenshot = Path(base_dir) / screenshot_path
                resolved_screenshot.parent.mkdir(parents=True, exist_ok=True)
                page.screenshot(path=str(resolved_screenshot), full_page=True)
            if rendered_html_path:
                resolved_html = Path(base_dir) / rendered_html_path
                resolved_html.parent.mkdir(parents=True, exist_ok=True)
                resolved_html.write_text(html, encoding="utf-8")
            if extracted_json_path:
                resolved_json = Path(base_dir) / extracted_json_path
                resolved_json.parent.mkdir(parents=True, exist_ok=True)
                resolved_json.write_text(
                    json.dumps(extracted_data, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            if network_log_path:
                resolved_network = Path(base_dir) / network_log_path
                resolved_network.parent.mkdir(parents=True, exist_ok=True)
                resolved_network.write_text(
                    json.dumps(captured_network, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            return {"html": html, "title": title, "url": final_url, "data": extracted_data}
        except Error as exc:
            raise RuntimeError(f"CDP page fetch failed for {url}: {exc}") from exc
        finally:
            if owns_page and not page.is_closed():
                page.close()
