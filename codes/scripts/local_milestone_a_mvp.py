from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import patch

from ui_tars.executor.aillium_core_client import AilliumCoreClient, AilliumCoreConfig
from ui_tars.executor.worker import (
    TASK_TYPE_REMOTE_HANDSHAKE,
    WorkerConfig,
    build_context,
    run_polling_loop,
)

REQUEST_SCHEMA_FIXTURE = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["tenantId", "requestId", "deviceId", "meta"],
    "properties": {
        "tenantId": {"type": "string", "minLength": 1},
        "requestId": {"type": "string", "minLength": 1},
        "deviceId": {"type": "string", "minLength": 1},
        "traceId": {"type": "string"},
        "meta": {
            "type": "object",
            "required": ["meshcentral_node_id"],
            "properties": {"meshcentral_node_id": {"type": "string", "minLength": 1}},
            "additionalProperties": True,
        },
    },
    "additionalProperties": True,
}

RESPONSE_SCHEMA_FIXTURE = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": True,
}


class _CoreState:
    def __init__(self):
        self._lock = threading.Lock()
        self.task = {
            "task_id": "task-local-1",
            "task_type": TASK_TYPE_REMOTE_HANDSHAKE,
            "payload": {
                "tenantId": "tenant-local",
                "requestId": "req-local-1",
                "traceId": "trace-local-1",
                "deviceId": "device-local-1",
                "meta": {"meshcentral_node_id": "node-local-1"},
            },
            "status": "PENDING",
        }
        self.task_status_by_id = {"task-local-1": "PENDING"}
        self.results_by_task_id: dict[str, dict] = {}
        self.audit_events: list[dict] = []

    def claim(self):
        with self._lock:
            if self.task is None:
                return None
            task = self.task
            task_id = task["task_id"]
            self.task_status_by_id[task_id] = "IN_PROGRESS"
            self.audit_events.append({"event": "task.claimed", "task_id": task_id})
            self.task = None
            return task

    def submit_result(self, task_id: str, payload: dict):
        with self._lock:
            self.results_by_task_id[task_id] = payload
            self.task_status_by_id[task_id] = "COMPLETED"
            self.audit_events.append(
                {
                    "event": "task.completed",
                    "task_id": task_id,
                    "status": payload.get("status"),
                    "artifact_count": len(payload.get("result", {}).get("artifacts", [])),
                }
            )
            return {"ok": True, "accepted": True}


def _start_core_server(state: _CoreState):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path.startswith("/api/v1/workers/tasks/poll"):
                if state.task is None:
                    self.send_response(204)
                    self.end_headers()
                    return

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(state.claim()).encode("utf-8"))
                return

            self.send_response(404)
            self.end_headers()

        def do_POST(self):  # noqa: N802
            if not self.path.startswith("/api/v1/workers/tasks/") or not self.path.endswith("/result"):
                self.send_response(404)
                self.end_headers()
                return

            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            body = json.loads(raw.decode("utf-8"))
            task_id = self.path.split("/")[-2]

            if not body.get("worker_id") and not body.get("workerId"):
                self.send_response(400)
                self.end_headers()
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(state.submit_result(task_id, body)).encode("utf-8"))

        def log_message(self, format: str, *args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return server, port


def main() -> int:
    state = _CoreState()
    server, port = _start_core_server(state)
    try:
        fake_schemas = SimpleNamespace(
            request_schema=REQUEST_SCHEMA_FIXTURE,
            response_schema=RESPONSE_SCHEMA_FIXTURE,
        )

        with patch("ui_tars.executor.worker.load_schemas", return_value=fake_schemas):
            context = build_context()

        client = AilliumCoreClient(
            AilliumCoreConfig(
                base_url=f"http://127.0.0.1:{port}",
                token="dev-token",
                timeout_seconds=2,
            )
        )

        with patch.dict(os.environ, {"MESHCENTRAL_MOCK": "1"}, clear=False):
            run_polling_loop(
                core_client=client,
                context=context,
                config=WorkerConfig(
                    worker_id="worker-local",
                    poll_interval_seconds=0.01,
                    idle_backoff_seconds=0.01,
                    task_type=TASK_TYPE_REMOTE_HANDSHAKE,
                    executor_type="ui-tars",
                ),
                stop_after_iterations=2,
            )

        result = state.results_by_task_id.get("task-local-1")
        if not result:
            raise SystemExit("No result posted back to Core")

        print("=== Milestone A local MVP flow OK ===")
        print(json.dumps(
            {
                "task_status": state.task_status_by_id,
                "audit_events": state.audit_events,
                "result_excerpt": {
                    "worker_id": result.get("worker_id") or result.get("workerId"),
                    "status": result.get("status"),
                    "artifact_uris": [a.get("uri") for a in result.get("result", {}).get("artifacts", [])],
                },
            },
            indent=2,
        ))
        return 0
    finally:
        server.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
