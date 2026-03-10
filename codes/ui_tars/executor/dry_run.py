from __future__ import annotations

import copy
import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

from jsonschema import Draft202012Validator


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(ts: datetime) -> str:
    return ts.isoformat().replace("+00:00", "Z")


def _resolve_ref(ref: str, root_schema: dict[str, Any]) -> dict[str, Any]:
    if not ref.startswith("#/"):
        return {}
    cursor: Any = root_schema
    for part in ref[2:].split("/"):
        cursor = cursor.get(part)
        if cursor is None:
            return {}
    return cursor if isinstance(cursor, dict) else {}


def _primitive_default(schema: dict[str, Any], seed: str) -> Any:
    if "const" in schema:
        return schema["const"]
    if "enum" in schema and schema["enum"]:
        return schema["enum"][0]
    if "default" in schema:
        return schema["default"]

    schema_type = schema.get("type")
    schema_format = schema.get("format")

    if schema_format == "date-time":
        return _iso(_now())
    if schema_format == "uuid":
        return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))
    if schema_format == "uri":
        return f"s3://aillium-dry-run/{seed}"

    if schema_type == "string":
        return "placeholder"
    if schema_type == "integer":
        minimum = schema.get("minimum", 0)
        return int(minimum)
    if schema_type == "number":
        minimum = schema.get("minimum", 0)
        return float(minimum)
    if schema_type == "boolean":
        return False
    if schema_type == "array":
        return []
    if schema_type == "object":
        return {}
    return None


def _build_required(schema: dict[str, Any], root_schema: dict[str, Any], seed: str) -> Any:
    if "$ref" in schema:
        return _build_required(_resolve_ref(schema["$ref"], root_schema), root_schema, seed)

    if "allOf" in schema and schema["allOf"]:
        merged: dict[str, Any] = {}
        for item in schema["allOf"]:
            value = _build_required(item, root_schema, seed)
            if isinstance(value, dict):
                merged.update(value)
        return merged

    for union_key in ("oneOf", "anyOf"):
        if union_key in schema and schema[union_key]:
            return _build_required(schema[union_key][0], root_schema, seed)

    schema_type = schema.get("type")
    properties = schema.get("properties", {})

    if schema_type == "object" or properties:
        out: dict[str, Any] = {}
        for key in schema.get("required", []):
            child = properties.get(key, {})
            out[key] = _build_required(child, root_schema, f"{seed}.{key}")
        return out

    if schema_type == "array":
        min_items = schema.get("minItems", 0)
        item_schema = schema.get("items", {})
        return [
            _build_required(item_schema, root_schema, f"{seed}[{idx}]")
            for idx in range(min_items)
        ]

    return _primitive_default(schema, seed)


def _set_if_exists(target: dict[str, Any], key: str, value: Any) -> None:
    if key in target:
        target[key] = value


def _walk_mutate(obj: Any, fn) -> None:
    if isinstance(obj, dict):
        fn(obj)
        for val in obj.values():
            _walk_mutate(val, fn)
    elif isinstance(obj, list):
        for item in obj:
            _walk_mutate(item, fn)


def _extract_correlation(request_payload: dict[str, Any], headers: dict[str, str]) -> dict[str, str | None]:
    meta = request_payload.get("meta") if isinstance(request_payload.get("meta"), dict) else {}
    return {
        "tenantId": request_payload.get("tenantId") or meta.get("tenantId") or headers.get("x-tenant-id"),
        "requestId": request_payload.get("requestId") or meta.get("requestId") or headers.get("x-request-id"),
        "traceId": headers.get("x-trace-id") or meta.get("traceId") or request_payload.get("traceId"),
    }


def build_dry_run_response(
    request_payload: dict[str, Any],
    response_schema: dict[str, Any],
    headers: dict[str, str],
) -> dict[str, Any]:
    start = _now()
    meta = request_payload.get("meta") if isinstance(request_payload.get("meta"), dict) else {}
    simulate_failure = bool(meta.get("simulate_failure"))

    corr = _extract_correlation(request_payload, headers)
    request_id = corr.get("requestId") or "unknown"
    seed = hashlib.sha256(str(request_id).encode("utf-8")).hexdigest()[:12]

    response = _build_required(response_schema, response_schema, seed)
    if not isinstance(response, dict):
        response = {}

    end = _now()
    duration_ms = max(0, int((end - start).total_seconds() * 1000))
    status = "failed" if simulate_failure else "succeeded"

    def mut(d: dict[str, Any]) -> None:
        _set_if_exists(d, "tenantId", corr.get("tenantId"))
        _set_if_exists(d, "requestId", corr.get("requestId"))
        _set_if_exists(d, "traceId", corr.get("traceId"))
        _set_if_exists(d, "status", status)
        _set_if_exists(d, "startedAt", _iso(start))
        _set_if_exists(d, "finishedAt", _iso(end))
        _set_if_exists(d, "durationMs", duration_ms)
        _set_if_exists(d, "simulated", True)
        _set_if_exists(d, "mode", "dry-run")
        _set_if_exists(
            d,
            "message",
            "Dry-run simulated failure" if simulate_failure else "Dry-run execution completed",
        )
        _set_if_exists(
            d,
            "error",
            {"code": "DRY_RUN_SIMULATED_FAILURE", "message": "Simulated failure requested"}
            if simulate_failure
            else None,
        )

        if "artifacts" in d and isinstance(d.get("artifacts"), list):
            d["artifacts"] = [
                {
                    "type": "screenshot",
                    "uri": f"s3://aillium-dry-run/{corr.get('tenantId') or 'tenant'}/{request_id}/{seed}/artifact-001.png",
                    "key": f"{corr.get('tenantId') or 'tenant'}/{request_id}/{seed}/artifact-001.png",
                    "contentType": "image/png",
                }
            ]

        if "evidence" in d and isinstance(d.get("evidence"), list):
            d["evidence"] = [
                {
                    "kind": "EvidencePointer",
                    "uri": f"s3://aillium-dry-run/{corr.get('tenantId') or 'tenant'}/{request_id}/{seed}/evidence-001.json",
                    "key": f"{corr.get('tenantId') or 'tenant'}/{request_id}/{seed}/evidence-001.json",
                }
            ]

        if "logs" in d and isinstance(d.get("logs"), list):
            d["logs"] = [
                {
                    "step": "dry_run_validation",
                    "level": "INFO",
                    "message": "executor.request validated",
                    "timestamp": _iso(start),
                },
                {
                    "step": "dry_run_execution",
                    "level": "INFO",
                    "message": "No UI actions executed; dry-run only",
                    "timestamp": _iso(end),
                },
            ]

    _walk_mutate(response, mut)

    # Final pass: validate contract.
    Draft202012Validator(response_schema).validate(response)
    return copy.deepcopy(response)
