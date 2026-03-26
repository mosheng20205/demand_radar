from __future__ import annotations

import json
import socket
import time
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urlsplit


class CloakApiError(RuntimeError):
    def __init__(
        self,
        path: str,
        *,
        status_code: int | None = None,
        payload: dict[str, Any] | None = None,
        body: str = "",
    ) -> None:
        self.path = path
        self.status_code = status_code
        self.payload = payload or {}
        self.body = body

        message = str(self.payload.get("msg") or body or f"Cloak API {path} failed")
        if status_code is not None:
            message = f"Cloak API {path} failed: HTTP {status_code} {message}"
        else:
            message = f"Cloak API {path} failed: {message}"
        super().__init__(message)


class CloakClient:
    def __init__(self, base_url: str, timeout_seconds: int = 20) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def _post(self, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        data = json.dumps(payload or {}).encode("utf-8")
        request = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                return self._parse_json_response(response.read())
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            payload_data = self._parse_json_body(body)
            raise CloakApiError(path, status_code=exc.code, payload=payload_data, body=body) from exc

    @staticmethod
    def _parse_json_body(body: str) -> dict[str, Any]:
        if not body:
            return {}
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _parse_json_response(self, raw: bytes) -> dict[str, Any]:
        body = raw.decode("utf-8", errors="ignore")
        payload = self._parse_json_body(body)
        if payload:
            return payload
        raise RuntimeError("Cloak API returned a non-JSON response.")

    def _debug_host(self) -> str:
        return urlsplit(self.base_url).hostname or "127.0.0.1"

    def _is_debug_port_reachable(self, port: int, *, host: str | None = None, timeout_seconds: float = 1.0) -> bool:
        if port <= 0:
            return False
        target_host = (host or self._debug_host()).strip() or "127.0.0.1"
        try:
            with socket.create_connection((target_host, port), timeout=timeout_seconds):
                return True
        except OSError:
            return False

    def _build_open_data(self, profile_id: str, port: int, *, already_running: bool) -> dict[str, Any]:
        detail = self.browser_detail(profile_id)
        host = self._debug_host()
        return {
            "debugPort": port,
            "http": f"{host}:{port}",
            "ws": f"ws://{host}:{port}/devtools/browser",
            "profileId": str(detail.get("id") or detail.get("profileId") or profile_id),
            "name": str(detail.get("name") or profile_id),
            "pid": detail.get("pid"),
            "already_running": already_running,
        }

    def _resolve_open_data_from_ports(
        self,
        profile_id: str,
        *,
        already_running: bool,
        retries: int = 3,
        backoff_seconds: float = 1.0,
    ) -> dict[str, Any]:
        last_port = 0
        for attempt in range(retries + 1):
            port = self.browser_ports().get(profile_id, 0)
            last_port = port
            if port and self._is_debug_port_reachable(port):
                return self._build_open_data(profile_id, port, already_running=already_running)
            if attempt >= retries:
                break
            time.sleep(backoff_seconds * (attempt + 1))
        if last_port:
            raise RuntimeError(f"Cloak profile debug port is not reachable yet: {profile_id}:{last_port}")
        raise RuntimeError(f"Cloak profile has no debug port in /browser/ports: {profile_id}")

    @staticmethod
    def _is_running_status(value: object) -> bool:
        status = str(value or "").strip().lower()
        return bool(status) and ("running" in status or "运行" in status)

    def _resolve_running_open_data(self, profile_id: str) -> dict[str, Any]:
        detail = self.browser_detail(profile_id)
        if not self._is_running_status(detail.get("status")):
            raise RuntimeError(f"Cloak profile is not running: {profile_id}")
        return self._resolve_open_data_from_ports(profile_id, already_running=True, retries=1, backoff_seconds=0.5)

    @staticmethod
    def _looks_like_already_running_error(exc: Exception) -> bool:
        message = str(exc)
        if isinstance(exc, CloakApiError) and exc.path == "/browser/open" and exc.status_code == 400:
            return True
        lowered = message.lower()
        return (
            "already running" in lowered
            or "running" in lowered
            or "运行中" in message
            or "已经在运行中" in message
        )

    def health(self) -> dict[str, Any]:
        return self._post("/health")

    def list_browsers(
        self,
        *,
        page: int = 0,
        page_size: int = 100,
        group_id: str | None = None,
        name: str | None = None,
    ) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {"page": page, "pageSize": page_size}
        if group_id:
            payload["groupId"] = group_id
        if name:
            payload["name"] = name
        response = self._post("/browser/list", payload)
        data = response.get("data") or {}
        return list(data.get("list") or [])

    def browser_detail(self, profile_id: str) -> dict[str, Any]:
        response = self._post("/browser/detail", {"id": profile_id})
        data = response.get("data") or {}
        if not data:
            raise RuntimeError(f"Cloak browser detail returned empty data for profile: {profile_id}")
        return dict(data)

    def browser_ports(self) -> dict[str, int]:
        response = self._post("/browser/ports", {})
        data = response.get("data") or {}
        return {str(key): int(value) for key, value in data.items() if str(key).strip() and str(value).strip()}

    def resolve_profile_id(self, *, profile_id: str | None = None, profile_name: str | None = None) -> str:
        if profile_id:
            return profile_id
        if not profile_name:
            raise ValueError("Cloak profile_id or profile_name is required.")

        browsers = self.list_browsers(name=profile_name)
        for item in browsers:
            if str(item.get("name", "")).strip() == profile_name:
                return str(item.get("id") or item.get("profileId") or "").strip()
        for item in browsers:
            name = str(item.get("name", "")).strip()
            if profile_name in name:
                return str(item.get("id") or item.get("profileId") or "").strip()
        raise ValueError(f"Cloak profile not found: {profile_name}")

    def create_browser(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post("/browser/update", payload)

    def open_browser(
        self,
        profile_id: str,
        *,
        args: list[str] | None = None,
        queue: bool = True,
        retries: int = 3,
        backoff_seconds: float = 2.0,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"id": profile_id, "queue": queue}
        if args:
            payload["args"] = args
        last_error: Exception | None = None
        for attempt in range(retries + 1):
            try:
                response = self._post("/browser/open", payload)
                if not response.get("success"):
                    raise RuntimeError(str(response.get("msg") or f"Failed to open Cloak browser: {profile_id}"))
                return self._resolve_open_data_from_ports(
                    profile_id,
                    already_running=False,
                    retries=max(retries, 3),
                    backoff_seconds=max(backoff_seconds, 1.0),
                )
            except Exception as exc:
                last_error = exc
                if self._looks_like_already_running_error(exc):
                    try:
                        return self._resolve_running_open_data(profile_id)
                    except Exception:
                        pass
                if attempt >= retries:
                    raise
                time.sleep(backoff_seconds * (attempt + 1))
        if last_error:
            raise last_error
        raise RuntimeError(f"Failed to open Cloak browser: {profile_id}")

    def close_browser(self, profile_id: str) -> dict[str, Any]:
        response = self._post("/browser/close", {"id": profile_id})
        if not response.get("success"):
            raise RuntimeError(str(response.get("msg") or f"Failed to close Cloak browser: {profile_id}"))
        return response


def build_cloak_client(cloak_config: dict[str, Any]) -> CloakClient:
    base_url = str(cloak_config.get("api_base_url") or "").strip()
    if not base_url:
        raise ValueError("Cloak api_base_url is required.")
    timeout_seconds = int(cloak_config.get("timeout_seconds", 20))
    return CloakClient(base_url=base_url, timeout_seconds=timeout_seconds)
