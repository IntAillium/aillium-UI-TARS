from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from jsonschema import Draft202012Validator

from .meshcentral_client import MeshCentralClient


class RemoteHandshakeValidationError(ValueError):
    pass


class RemoteHandshakeExecutionError(RuntimeError):
    """
    Used to shape server responses for remote-handshake execution failures.

    Option A expects:
    - Validation errors -> 400 (handled by server)
    - Execution errors -> 200 with body.status="failed" (tests expect this)
    """

    def __init__(
        self,
        status_code: int,
        error: str,
        message: str,
        reason_code: str,
        retryable: bool = False,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.error = error
        self.reason_code = reason_code
        self.retryable = retryable


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(ts: datetime) -> str:
    return ts.isoformat().replace("+00:00", "Z")


def _extract_trace_id(request_payload: dict[str, Any], headers: dict[str, str]) -> str | None:
    meta = request_payload.get("meta") if isinstance(request_payload.get("meta"), dict) else {}
    return headers.get("x-trace-id") or meta.get("traceId") or request_payload.get("traceId")


def _artifact_key(prefix: str, tenant_id: str, request_id: str, kind: str) -> str:
    stable = f"{prefix}:{tenant_id}:{request_id}:{kind}".encode("utf-8")
    digest = hashlib.sha256(stable).hexdigest()[:24]
    return f"{prefix}/{tenant_id}/{request_id}/{kind}/{digest}.json"


def execute_remote_handshake(
    request_payload: dict[str, Any],
    request_validator: Draft202012Validator,
    response_schema: dict[str, Any],
    headers: dict[str, str],
    client: MeshCentralClient | None = None,
) -> dict[str, Any]:
    # Validate against canonical executor.request schema
    request_validator.validate(request_payload)

    tenant_id = request_payload.get("tenantId")
    request_id = request_payload.get("requestId")
    meta = request_payload.get("meta") if isinstance(request_payload.get("meta"), dict) else {}

    # Option A: require meshcentral_node_id directly (no core lookup)
    mesh_node_id = meta.get("meshcentral_node_id")

    # Device id still required for audit correlation even if we don’t resolve via core
    device_id = request_payload.get("deviceId") or meta.get("deviceId") or meta.get("device_id")
    trace_id = _extract_trace_id(request_payload, headers)

    if not tenant_id:
        raise RemoteHandshakeValidationError("tenantId is required")
    if not request_id:
        raise RemoteHandshakeValidationError("requestId is required")
    if not device_id:
        raise RemoteHandshakeValidationError("deviceId is required in executor.request")
    if not isinstance(mesh_node_id, str) or not mesh_node_id.strip():
        raise RemoteHandshakeValidationError("meta.meshcentral_node_id is required (Option A)")

    mesh_node_id = mesh_node_id.strip()
    handshake_client = client or MeshCentralClient()

    started_at = _now()
    status = "succeeded"
    message = "Remote handshake completed"

    metadata_payload: dict[str, Any] = {}
    screenshot_payload: dict[str, Any] | None = None

    logs: list[dict[str, Any]] = [
        {
            "step": "handshake_open",
            "level": "INFO",
            "message": "Opening MeshCentral remote session",
            "timestamp": _iso(started_at),
            "tenantId": tenant_id,
            "requestId": request_id,
            "traceId": trace_id,
            "deviceId": device_id,
            "mesh_node_id": mesh_node_id,
        }
    ]

    try:
        handshake_client.open_session(mesh_node_id)

        metadata_payload = handshake_client.fetch_session_metadata(mesh_node_id)
        logs.append(
            {
                "step": "handshake_metadata",
                "level": "INFO",
                "message": "Fetched MeshCentral session metadata",
                "timestamp": _iso(_now()),
                "tenantId": tenant_id,
                "requestId": request_id,
                "traceId": trace_id,
                "deviceId": device_id,
                "mesh_node_id": mesh_node_id,
            }
        )

        # Best-effort screenshot; do not fail handshake if it errors
        try:
            screenshot_payload = handshake_client.capture_screenshot(mesh_node_id)
            logs.append(
                {
                    "step": "handshake_screenshot",
                    "level": "INFO",
                    "message": "Captured MeshCentral screenshot artifact",
                    "timestamp": _iso(_now()),
                    "tenantId": tenant_id,
                    "requestId": request_id,
                    "traceId": trace_id,
                    "deviceId": device_id,
                    "mesh_node_id": mesh_node_id,
                }
            )
        except Exception as screenshot_exc:
            logs.append(
                {
                    "step": "handshake_screenshot",
                    "level": "WARN",
                    "message": f"Screenshot capture skipped: {screenshot_exc}",
                    "timestamp": _iso(_now()),
                    "tenantId": tenant_id,
                    "requestId": request_id,
                    "traceId": trace_id,
                    "deviceId": device_id,
                    "mesh_node_id": mesh_node_id,
                }
            )

    except Exception as exc:
        # Option A semantics: return 200 with a response payload whose status="failed"
        status = "failed"
        message = "Remote handshake failed"
        logs.append(
            {
                "step": "handshake_error",
                "level": "ERROR",
                "message": str(exc),
                "timestamp": _iso(_now()),
                "tenantId": tenant_id,
                "requestId": request_id,
                "traceId": trace_id,
                "deviceId": device_id,
                "mesh_node_id": mesh_node_id,
            }
        )

        # Still attempt close_session in finally (below), then return a failed response.
        # (Do NOT raise here; the tests expect HTTP 200 and body.status="failed".)

    finally:
        try:
            handshake_client.close_session(mesh_node_id)
            logs.append(
                {
                    "step": "handshake_close",
                    "level": "INFO",
                    "message": "Closed MeshCentral remote session",
                    "timestamp": _iso(_now()),
                    "tenantId": tenant_id,
                    "requestId": request_id,
                    "traceId": trace_id,
                    "deviceId": device_id,
                    "mesh_node_id": mesh_node_id,
                }
            )
        except Exception as close_exc:
            logs.append(
                {
                    "step": "handshake_close",
                    "level": "WARN",
                    "message": f"Failed to close session: {close_exc}",
                    "timestamp": _iso(_now()),
                    "tenantId": tenant_id,
                    "requestId": request_id,
                    "traceId": trace_id,
                    "deviceId": device_id,
                    "mesh_node_id": mesh_node_id,
                }
            )

    finished_at = _now()
    duration_ms = int((finished_at - started_at).total_seconds() * 1000)

    artifacts: list[dict[str, Any]] = [
        {
            "kind": "meshcentral.session.metadata",
            "uri": f"s3://aillium-evidence/{_artifact_key('meshcentral', tenant_id, request_id, 'metadata')}",
        }
    ]
    if screenshot_payload is not None:
        artifacts.append(
            {
                "kind": "meshcentral.session.screenshot",
                "uri": f"s3://aillium-evidence/{_artifact_key('meshcentral', tenant_id, request_id, 'screenshot')}",
            }
        )

    response_payload: dict[str, Any] = {
        "tenantId": tenant_id,
        "requestId": request_id,
        "traceId": trace_id,
        "status": status,
        "timing": {
            "startedAt": _iso(started_at),
            "finishedAt": _iso(finished_at),
            "durationMs": duration_ms,
        },
        "result": {
            "message": message,
            "meshcentral_node_id": mesh_node_id,
            "deviceId": device_id,
            "metadata": metadata_payload,
            "screenshot": screenshot_payload,
            "artifacts": artifacts,
            "warnings": [],
        },
        "error": None if status == "succeeded" else {"error": "handshake_failed", "message": message},
        "meta": {
            "tenantId": tenant_id,
            "logs": logs,
        },
    }

    _ = response_schema
    return response_payload