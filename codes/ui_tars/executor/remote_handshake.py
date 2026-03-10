from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from jsonschema import Draft202012Validator

from .meshcentral_client import MeshCentralClient


class RemoteHandshakeValidationError(ValueError):
    pass


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(ts: datetime) -> str:
    return ts.isoformat().replace("+00:00", "Z")


def _extract_trace_id(request_payload: dict[str, Any], headers: dict[str, str]) -> str | None:
    meta = request_payload.get("meta") if isinstance(request_payload.get("meta"), dict) else {}
    return headers.get("x-trace-id") or meta.get("traceId") or request_payload.get("traceId")


def execute_remote_handshake(
    request_payload: dict[str, Any],
    request_validator: Draft202012Validator,
    response_schema: dict[str, Any],
    headers: dict[str, str],
    client: MeshCentralClient | None = None,
) -> dict[str, Any]:
    request_validator.validate(request_payload)

    tenant_id = request_payload.get("tenantId")
    request_id = request_payload.get("requestId")
    meta = request_payload.get("meta") if isinstance(request_payload.get("meta"), dict) else {}
    mesh_node_id = meta.get("meshcentral_node_id")
    device_id = meta.get("deviceId") or meta.get("device_id") or mesh_node_id
    trace_id = _extract_trace_id(request_payload, headers)

    if not tenant_id:
        raise RemoteHandshakeValidationError("tenantId is required")
    if not request_id:
        raise RemoteHandshakeValidationError("requestId is required")
    if not mesh_node_id:
        raise RemoteHandshakeValidationError("meta.meshcentral_node_id is required")

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

    error_payload: dict[str, str] | None = None
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
        status = "failed"
        message = "Remote handshake failed"
        error_payload = {"code": "REMOTE_HANDSHAKE_FAILED", "message": str(exc)}
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
                    "level": "ERROR",
                    "message": f"Failed to close MeshCentral session: {close_exc}",
                    "timestamp": _iso(_now()),
                    "tenantId": tenant_id,
                    "requestId": request_id,
                    "traceId": trace_id,
                    "deviceId": device_id,
                    "mesh_node_id": mesh_node_id,
                }
            )

    finished_at = _now()
    duration_ms = max(0, int((finished_at - started_at).total_seconds() * 1000))
    artifact_seed = hashlib.sha256(f"{tenant_id}:{request_id}:{mesh_node_id}".encode("utf-8")).hexdigest()[:12]
    artifact_key = f"evidence/{tenant_id}/{request_id}/{mesh_node_id}/{artifact_seed}/remote-handshake.json"

    response_payload: dict[str, Any] = {
        "tenantId": tenant_id,
        "requestId": request_id,
        "traceId": trace_id,
        "status": status,
        "startedAt": _iso(started_at),
        "finishedAt": _iso(finished_at),
        "durationMs": duration_ms,
        "artifacts": [
            {
                "type": "session_metadata",
                "uri": f"s3://aillium-artifacts/{artifact_key}",
                "key": artifact_key,
                "contentType": "application/json",
                "metadata": {
                    "mesh_node_id": mesh_node_id,
                    "deviceId": device_id,
                    "metadata": metadata_payload,
                    "screenshot": screenshot_payload,
                },
            }
        ],
        "evidence": [
            {
                "kind": "EvidencePointer",
                "uri": f"s3://aillium-artifacts/{artifact_key}",
                "key": artifact_key,
            }
        ],
        "logs": logs,
        "message": message,
        "error": error_payload,
    }

    Draft202012Validator(response_schema).validate(response_payload)
    return response_payload
