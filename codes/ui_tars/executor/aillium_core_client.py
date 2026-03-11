from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib import error, parse, request


class AilliumCoreClientError(RuntimeError):
    pass


class AilliumCoreForbiddenError(AilliumCoreClientError):
    pass


class AilliumCoreDeviceNotFoundError(AilliumCoreClientError):
    pass


class AilliumCoreRetryableError(AilliumCoreClientError):
    pass


@dataclass(frozen=True)
class AilliumCoreConfig:
    base_url: str
    token: str
    timeout_seconds: float


class AilliumCoreClient:
    def __init__(self, config: AilliumCoreConfig | None = None):
        self._config = config or self._load_from_env()

    @staticmethod
    def _load_from_env() -> AilliumCoreConfig:
        base_url = os.getenv("AILLIUM_CORE_BASE_URL", "").strip().rstrip("/")
        if not base_url:
            raise AilliumCoreClientError("AILLIUM_CORE_BASE_URL must be set")

        token = os.getenv("AILLIUM_CORE_TOKEN", "").strip()
        if not token:
            raise AilliumCoreClientError("AILLIUM_CORE_TOKEN must be set")

        timeout_raw = os.getenv("AILLIUM_CORE_TIMEOUT_SECONDS", "10")
        try:
            timeout_seconds = float(timeout_raw)
        except ValueError as exc:
            raise AilliumCoreClientError("AILLIUM_CORE_TIMEOUT_SECONDS must be numeric") from exc

        return AilliumCoreConfig(base_url=base_url, token=token, timeout_seconds=timeout_seconds)

    def _request(self, method: str, path: str) -> Any:
        req = request.Request(
            url=f"{self._config.base_url}{path}",
            method=method.upper(),
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self._config.token}",
            },
        )

        try:
            with request.urlopen(req, timeout=self._config.timeout_seconds) as resp:
                payload = resp.read()
                if not payload:
                    return {}
                return json.loads(payload.decode("utf-8"))
        except error.HTTPError as exc:
            if exc.code in {401, 403}:
                raise AilliumCoreForbiddenError("aillium-core rejected executor credentials") from exc
            if exc.code == 404:
                raise AilliumCoreDeviceNotFoundError("device not found in aillium-core") from exc
            detail = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
            raise AilliumCoreClientError(
                f"aillium-core request failed status={exc.code} method={method} path={path} detail={detail}"
            ) from exc
        except TimeoutError as exc:
            raise AilliumCoreRetryableError("aillium-core request timed out") from exc
        except error.URLError as exc:
            raise AilliumCoreRetryableError(
                f"aillium-core network failure reason={exc.reason}"
            ) from exc

    def resolve_meshcentral_node_id(self, tenant_id: str, device_id: str) -> str:
        tenant = parse.quote(tenant_id, safe="")
        device = parse.quote(device_id, safe="")
        payload = self._request("GET", f"/api/v1/tenants/{tenant}/devices/{device}/meshcentral-node")
        if not isinstance(payload, dict):
            raise AilliumCoreClientError("aillium-core response must be a JSON object")

        mesh_node_id = payload.get("meshcentral_node_id")
        if not mesh_node_id and isinstance(payload.get("data"), dict):
            mesh_node_id = payload["data"].get("meshcentral_node_id")

        if not isinstance(mesh_node_id, str) or not mesh_node_id.strip():
            raise AilliumCoreClientError("aillium-core response missing meshcentral_node_id")
        return mesh_node_id.strip()
