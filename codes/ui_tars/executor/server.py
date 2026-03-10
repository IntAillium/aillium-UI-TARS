from __future__ import annotations

import json
import os
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from .audit import Correlation, emit, setup_logging
from .dry_run import build_dry_run_response
from .schema_loader import SchemaLoadError, load_schemas


@dataclass(frozen=True)
class AppContext:
    request_validator: Draft202012Validator
    response_schema: dict[str, Any]


def _json_headers(handler: BaseHTTPRequestHandler, status: int) -> None:
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.end_headers()


def _read_json(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    content_length = int(handler.headers.get("Content-Length", "0"))
    raw = handler.rfile.read(content_length) if content_length > 0 else b"{}"
    parsed = json.loads(raw.decode("utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError("JSON body must be an object")
    return parsed


def _corr_from_payload_and_headers(payload: dict[str, Any], headers) -> Correlation:
    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    return Correlation(
        tenant_id=(payload.get("tenantId") or meta.get("tenantId") or headers.get("x-tenant-id")),
        request_id=(payload.get("requestId") or meta.get("requestId") or headers.get("x-request-id")),
        trace_id=(payload.get("traceId") or meta.get("traceId") or headers.get("x-trace-id")),
    )


def create_handler(context: AppContext):
    class ExecutorHandler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802 - BaseHTTPRequestHandler signature
            if self.path != "/executor/dry-run":
                _json_headers(self, HTTPStatus.NOT_FOUND)
                self.wfile.write(json.dumps({"error": "not_found"}).encode("utf-8"))
                return

            payload: dict[str, Any] = {}
            correlation = Correlation(None, None, self.headers.get("x-trace-id"))

            try:
                payload = _read_json(self)
                correlation = _corr_from_payload_and_headers(payload, self.headers)
                emit("executor.request.received", correlation, path=self.path)

                context.request_validator.validate(payload)
                emit("executor.request.validated", correlation, validation="passed")

                response_payload = build_dry_run_response(
                    request_payload=payload,
                    response_schema=context.response_schema,
                    headers={k.lower(): v for k, v in self.headers.items()},
                )
                emit("executor.response.validated", correlation, validation="passed")
                _json_headers(self, HTTPStatus.OK)
                self.wfile.write(json.dumps(response_payload).encode("utf-8"))
            except ValidationError as exc:
                emit(
                    "executor.request.validation_failed",
                    correlation,
                    message=exc.message,
                    schemaPath=list(exc.schema_path),
                    instancePath=list(exc.path),
                )
                _json_headers(self, HTTPStatus.BAD_REQUEST)
                self.wfile.write(
                    json.dumps(
                        {
                            "error": "request_schema_validation_failed",
                            "message": exc.message,
                            "schemaPath": list(exc.schema_path),
                            "instancePath": list(exc.path),
                        }
                    ).encode("utf-8")
                )
            except Exception as exc:
                emit("executor.request.failed", correlation, error=str(exc), errorType=type(exc).__name__)
                _json_headers(self, HTTPStatus.INTERNAL_SERVER_ERROR)
                self.wfile.write(
                    json.dumps({"error": "internal_executor_error", "message": str(exc)}).encode(
                        "utf-8"
                    )
                )

        def do_GET(self):  # noqa: N802
            if self.path == "/healthz":
                _json_headers(self, HTTPStatus.OK)
                self.wfile.write(json.dumps({"status": "ok"}).encode("utf-8"))
                return
            _json_headers(self, HTTPStatus.NOT_FOUND)
            self.wfile.write(json.dumps({"error": "not_found"}).encode("utf-8"))

        def log_message(self, format: str, *args):
            return

    return ExecutorHandler


def build_context() -> AppContext:
    schemas = load_schemas()
    return AppContext(
        request_validator=Draft202012Validator(schemas.request_schema),
        response_schema=schemas.response_schema,
    )


def run() -> None:
    setup_logging()
    try:
        context = build_context()
    except SchemaLoadError as exc:
        raise SystemExit(str(exc)) from exc

    host = os.getenv("EXECUTOR_HOST", "0.0.0.0")
    port = int(os.getenv("EXECUTOR_PORT", "8080"))
    server = ThreadingHTTPServer((host, port), create_handler(context))
    print(f"Executor dry-run server listening on http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run()
