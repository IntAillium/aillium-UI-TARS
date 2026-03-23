"""Aillium Core task-bus client for the TARS worker.

Communicates with the aillium-core control plane via the task-bus API
(POST /v1/task-bus/*).  Requires AILLIUM_TENANT_ID so that the worker
can identify which tenant's tasks to poll and claim.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
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
    tenant_id: str
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

        tenant_id = os.getenv("AILLIUM_TENANT_ID", "").strip()
        if not tenant_id:
            raise AilliumCoreClientError("AILLIUM_TENANT_ID must be set")

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
            tenant_id=tenant_id,
            timeout_seconds=timeout_seconds,
        )

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        query: dict[str, str] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> Any:
        body = None
        headers: dict[str, str] = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._config.token}",
            "X-Tenant-Id": self._config.tenant_id,
        }
        if extra_headers:
            headers.update(extra_headers)

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

    def poll_executor_task(self, executor_type: str) -> dict[str, Any] | None:
        """Poll the task-bus for available tasks.

        Aligns with Core endpoint: POST /v1/task-bus/tasks:poll
        Returns the first eligible task dict or None if no tasks available.
        """
        trace_id = str(uuid.uuid4())
        response = self._request(
            "POST",
            "/v1/task-bus/tasks:poll",
            payload={"executorType": executor_type},
            extra_headers={"X-Trace-Id": trace_id},
        )
        if response is None:
            return None
        if not isinstance(response, dict):
            raise AilliumCoreClientError("poll response must be a JSON object")
        tasks = response.get("tasks", [])
        if not tasks:
            return None
        # Return the first eligible task.
        # tenantId is now returned by core; traceId is per-poll correlation.
        task = tasks[0]
        task["traceId"] = trace_id
        if "tenantId" not in task:
            task["tenantId"] = self._config.tenant_id
        return task

    def claim_task_lease(
        self, task_id: str, executor_type: str, visibility_timeout_seconds: int = 60
    ) -> dict[str, Any]:
        """Claim a lease on a polled task.

        Aligns with Core endpoint: POST /v1/task-bus/leases:claim
        Returns lease details including leaseToken and leaseExpiresAt.
        """
        idempotency_key = str(uuid.uuid4())
        response = self._request(
            "POST",
            "/v1/task-bus/leases:claim",
            payload={
                "taskId": task_id,
                "executorType": executor_type,
                "visibilityTimeoutSeconds": visibility_timeout_seconds,
            },
            extra_headers={"Idempotency-Key": idempotency_key},
        )
        if response is None:
            return {}
        if not isinstance(response, dict):
            raise AilliumCoreClientError("claim response must be a JSON object")
        return response

    def report_executor_status(
        self,
        *,
        execution_ref: str,
        executor_type: str,
        status: str,
        trace_id: str,
        artifacts: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        """Report executor status back to the task-bus.

        Aligns with Core endpoint: POST /v1/task-bus/executor-status
        Status values: executor_contract_accepted, executor_contract_rejected,
        executor_started, executor_succeeded, executor_failed, executor_timed_out
        """
        idempotency_key = str(uuid.uuid4())
        payload: dict[str, Any] = {
            "executionRef": execution_ref,
            "executorType": executor_type,
            "status": status,
            "eventTimestamp": datetime.now(timezone.utc).isoformat(),
        }
        if artifacts:
            payload["artifacts"] = artifacts

        response = self._request(
            "POST",
            "/v1/task-bus/executor-status",
            payload=payload,
            extra_headers={
                "Idempotency-Key": idempotency_key,
                "X-Trace-Id": trace_id,
            },
        )
        if response is None:
            return {}
        if not isinstance(response, dict):
            raise AilliumCoreClientError("status report response must be a JSON object")
        return response

    def submit_executor_result(self, task_id: str, result_payload: dict[str, Any]) -> dict[str, Any]:
        """Legacy compatibility shim — delegates to report_executor_status.

        Maps the old result payload shape to the new executor-status API.
        """
        execution_ref = result_payload.get("executionRef", f"exec-{task_id}")
        executor_type = result_payload.get("executorType", "ui-tars")
        trace_id = result_payload.get("traceId", str(uuid.uuid4()))

        raw_status = result_payload.get("status", "failed")
        status_map = {
            "succeeded": "executor_succeeded",
            "failed": "executor_failed",
            "cancelled": "executor_cancelled",
            "timed_out": "executor_timed_out",
        }
        status = status_map.get(raw_status, "executor_failed")

        artifacts = None
        result = result_payload.get("result")
        if isinstance(result, dict):
            raw_artifacts = result.get("artifacts", [])
            if raw_artifacts:
                artifacts = [{"uri": a["uri"]} for a in raw_artifacts if isinstance(a, dict) and "uri" in a]

        return self.report_executor_status(
            execution_ref=execution_ref,
            executor_type=executor_type,
            status=status,
            trace_id=trace_id,
            artifacts=artifacts,
        )
