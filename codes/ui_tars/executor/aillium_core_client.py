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
            raise AilliumCoreClientError(
                "AILLIUM_CORE_TIMEOUT_SECONDS must be numeric"
            ) from exc

        return AilliumCoreConfig(
            base_url=base_url,
            token=token,
            timeout_seconds=timeout_seconds,
        )

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        query: dict[str, str] | None = None,
    ) -> Any:
        body = None
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._config.token}",
        }

        url = f"{self._config.base_url}{path}"
        if query:
            url = f"{url}?{parse.urlencode(query)}"

        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = request.Request(
            url=url,
            method=method.upper(),
            headers=headers,
            data=body,
        )

        try:
            with request.urlopen(req, timeout=self._config.timeout_seconds) as resp:
                if resp.status == 204:
                    return None
                response_payload = resp.read()
                if not response_payload:
                    return {}
                return json.loads(response_payload.decode("utf-8"))
        except error.HTTPError as exc:
            if exc.code in {401, 403}:
                raise AilliumCoreForbiddenError(
                    "aillium-core rejected executor credentials"
                ) from exc
            if exc.code == 404:
                raise AilliumCoreDeviceNotFoundError(
                    "device not found in aillium-core"
                ) from exc

            detail = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
            raise AilliumCoreClientError(
                f"aillium-core request failed status={exc.code} "
                f"method={method} path={path} detail={detail}"
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

        payload = self._request("GET", f"/tenants/{tenant}/devices/{device}")

        if not isinstance(payload, dict):
            raise AilliumCoreClientError("aillium-core response must be a JSON object")

        mesh_node_id = payload.get("meshcentralNodeId")

        if not mesh_node_id and isinstance(payload.get("data"), dict):
            data = payload["data"]
            mesh_node_id = data.get("meshcentralNodeId") or data.get("meshcentral_node_id")

        if not mesh_node_id:
            mesh_node_id = payload.get("meshcentral_node_id")

        if not isinstance(mesh_node_id, str) or not mesh_node_id.strip():
            raise AilliumCoreClientError(
                "aillium-core response missing meshcentralNodeId"
            )

        return mesh_node_id.strip()

    def poll_executor_task(self, task_type: str) -> dict[str, Any] | None:
        response = self._request(
            "GET",
            "/api/v1/workers/tasks/poll",
            query={"task_type": task_type},
        )
        if response is None:
            return None
        if not isinstance(response, dict):
            raise AilliumCoreClientError("poll response must be a JSON object")
        return response

    def submit_executor_result(self, task_id: str, result_payload: dict[str, Any]) -> dict[str, Any]:
        task = parse.quote(task_id, safe="")
        response = self._request(
            "POST",
            f"/api/v1/workers/tasks/{task}/result",
            payload=result_payload,
        )
        if response is None:
            return {}
        if not isinstance(response, dict):
            raise AilliumCoreClientError("result callback response must be a JSON object")
        return response
