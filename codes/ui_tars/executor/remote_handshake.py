import time
from typing import Any

from ui_tars.executor.meshcentral_client import MeshCentralClient


class HandshakeValidationError(ValueError):
    pass


def _require(d: dict, key: str) -> Any:
    if key not in d or d[key] in (None, "", {}):
        raise HandshakeValidationError(f"Missing required field: {key}")
    return d[key]


def handle_remote_handshake(request: dict, client: MeshCentralClient | None = None) -> dict:
    """
    Option A mapping:
      - requires request.meta.meshcentral_node_id

    Performs:
      open -> fetch metadata (+ best-effort screenshot) -> close (always in finally)

    Returns an executor.response-like payload.
    """
    started = time.time()

    tenant_id = _require(request, "tenantId")
    request_id = _require(request, "requestId")
    trace_id = request.get("traceId")
    device_id = request.get("deviceId")

    meta = request.get("meta") or {}
    mesh_node_id = meta.get("meshcentral_node_id") or meta.get("meshcentralNodeId")
    if not mesh_node_id:
        raise HandshakeValidationError("Missing required meta.meshcentral_node_id")

    if client is None:
        client = MeshCentralClient.from_env()

    status = "succeeded"
    error = None
    artifacts = []
    output = {}

    try:
        client.open_session(mesh_node_id)
        metadata = client.fetch_session_metadata(mesh_node_id)

        # Best-effort screenshot (optional)
        try:
            client.capture_screenshot(mesh_node_id)
            artifacts.append({
                "uri": f"s3://evidence/{tenant_id}/{request_id}/meshcentral/screenshot.png",
                "contentType": "image/png",
            })
        except Exception:
            pass

        artifacts.append({
            "uri": f"s3://evidence/{tenant_id}/{request_id}/meshcentral/metadata.json",
            "contentType": "application/json",
        })

        output = {"mesh_node_id": mesh_node_id, "metadata": metadata}

    except Exception as e:
        status = "failed"
        error = {"code": "REMOTE_HANDSHAKE_FAILED", "message": str(e), "retryable": False}

    finally:
        try:
            client.close_session(mesh_node_id)
        except Exception:
            pass

    finished = time.time()
    duration_ms = int((finished - started) * 1000)

    return {
        "contractType": "executor.response",
        "schemaVersion": "1.0.0",
        "requestId": request_id,
        "responseId": f"resp_{request_id}",
        "traceId": trace_id,
        "completedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "status": status,
        "timing": {"runMs": duration_ms, "totalMs": duration_ms},
        "result": {"output": output, "artifacts": artifacts, "warnings": []},
        "error": error,
        "meta": {"tenantId": tenant_id, "deviceId": device_id, "mesh_node_id": mesh_node_id},
    }
