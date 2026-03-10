from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib import error, parse, request


class MeshCentralClientError(RuntimeError):
    pass


@dataclass(frozen=True)
class MeshCentralConfig:
    base_url: str
    api_key: str
    timeout_seconds: float


class MeshCentralClient:
    def __init__(self, config: MeshCentralConfig | None = None):
        self._config = config or self._load_from_env()

    @staticmethod
    def _load_from_env() -> MeshCentralConfig:
        base_url = os.getenv("MESHCENTRAL_URL", "").strip().rstrip("/")
        if not base_url:
            raise MeshCentralClientError("MESHCENTRAL_URL must be set")

        api_key = (
            os.getenv("MESHCENTRAL_API_KEY")
            or os.getenv("MESHCENTRAL_TOKEN")
            or ""
        ).strip()
        if not api_key:
            raise MeshCentralClientError("MESHCENTRAL_API_KEY (or MESHCENTRAL_TOKEN) must be set")

        timeout_raw = os.getenv("MESHCENTRAL_TIMEOUT_SECONDS", "10")
        try:
            timeout_seconds = float(timeout_raw)
        except ValueError as exc:
            raise MeshCentralClientError("MESHCENTRAL_TIMEOUT_SECONDS must be numeric") from exc

        return MeshCentralConfig(base_url=base_url, api_key=api_key, timeout_seconds=timeout_seconds)

    def _request(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        url = f"{self._config.base_url}{path}"
        data = None
        headers = {
            "Accept": "application/json",
            "x-api-key": self._config.api_key,
        }
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = request.Request(url=url, method=method.upper(), headers=headers, data=data)
        try:
            with request.urlopen(req, timeout=self._config.timeout_seconds) as resp:
                payload = resp.read()
                content_type = resp.headers.get("Content-Type", "")
                if "application/json" in content_type and payload:
                    return json.loads(payload.decode("utf-8"))
                if not payload:
                    return {}
                return {"raw": payload.decode("utf-8", errors="replace")}
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
            raise MeshCentralClientError(
                f"MeshCentral request failed status={exc.code} method={method} path={path} detail={detail}"
            ) from exc
        except error.URLError as exc:
            raise MeshCentralClientError(
                f"MeshCentral request failed method={method} path={path} reason={exc.reason}"
            ) from exc

    def auth_check(self) -> dict[str, Any]:
        return self._request("GET", "/api/authcheck")

    def open_session(self, mesh_node_id: str) -> dict[str, Any]:
        node = parse.quote(mesh_node_id, safe="")
        return self._request("POST", f"/api/nodes/{node}/session/open")

    def fetch_session_metadata(self, mesh_node_id: str) -> dict[str, Any]:
        node = parse.quote(mesh_node_id, safe="")
        metadata = self._request("GET", f"/api/nodes/{node}/metadata")
        if not isinstance(metadata, dict):
            raise MeshCentralClientError("MeshCentral metadata response must be an object")
        return metadata

    def capture_screenshot(self, mesh_node_id: str) -> dict[str, Any]:
        node = parse.quote(mesh_node_id, safe="")
        return self._request("GET", f"/api/nodes/{node}/screenshot")

    def close_session(self, mesh_node_id: str) -> dict[str, Any]:
        node = parse.quote(mesh_node_id, safe="")
        return self._request("POST", f"/api/nodes/{node}/session/close")
