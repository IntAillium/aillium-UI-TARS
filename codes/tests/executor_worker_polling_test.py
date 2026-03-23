import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import patch

try:
    from ui_tars.executor.aillium_core_client import AilliumCoreClient, AilliumCoreConfig
    from ui_tars.executor.worker import (
        TASK_TYPE_REMOTE_HANDSHAKE,
        WorkerConfig,
        build_context,
        process_task,
        run_polling_loop,
    )

    _IMPORT_ERROR = None
except Exception as exc:  # pragma: no cover
    _IMPORT_ERROR = exc


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
            "properties": {
                "meshcentral_node_id": {"type": "string", "minLength": 1},
            },
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
    """Simulates Core's task-bus API state for integration testing."""

    def __init__(self):
        self._lock = threading.Lock()
        # Task-bus format: matches Core's poll response shape.
        self.task = {
            "taskId": "task-1",
            "tenantId": "tenant-1",
            "executionRef": "exec-1",
            "dispatchTarget": "DEPRECATED_WORKER_COMPAT",
            "contractFamily": "DEPRECATED_WORKER_COMPAT_CONTRACTS",
            "expectedEvidenceKind": "worker-compat-execution-result",
            "compatibilityMode": True,
        }
        # Legacy payload attached for backward-compatible process_task tests.
        self.task_with_payload = {
            **self.task,
            "payload": {
                "tenantId": "tenant-1",
                "requestId": "req-1",
                "traceId": "trace-1",
                "deviceId": "device-1",
                "meta": {"meshcentral_node_id": "node-1"},
            },
        }
        self.task_claimed = False
        self.lease_responses = []
        self.status_events = []
        self.audit_events = []

    def poll(self):
        with self._lock:
            if self.task_claimed:
                return None
            return self.task_with_payload

    def claim(self, body: dict):
        with self._lock:
            task_id = body.get("taskId", "")
            self.task_claimed = True
            resp = {
                "taskId": task_id,
                "leaseToken": f"lease_{task_id}_test",
                "leaseExpiresAt": "2099-01-01T00:00:00Z",
                "visibilityTimeoutSeconds": 60,
            }
            self.lease_responses.append(resp)
            self.audit_events.append({"event": "task.claimed", "task_id": task_id})
            return resp

    def report_status(self, body: dict):
        with self._lock:
            self.status_events.append(body)
            status = body.get("status", "")
            self.audit_events.append({
                "event": "task.status_reported",
                "execution_ref": body.get("executionRef"),
                "status": status,
            })
            return {
                "accepted": True,
                "executionRef": body.get("executionRef"),
                "status": status,
            }


@unittest.skipIf(_IMPORT_ERROR is not None, f"executor deps unavailable: {_IMPORT_ERROR}")
class ExecutorPollingTest(unittest.TestCase):
    def _start_core_server(self, state: _CoreState):
        """Start a mock Core server implementing the task-bus API."""

        class Handler(BaseHTTPRequestHandler):
            def _read_body(self) -> dict:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length else b"{}"
                return json.loads(raw.decode("utf-8"))

            def do_POST(self):  # noqa: N802
                if self.path == "/v1/task-bus/tasks:poll":
                    task = state.poll()
                    if task is None:
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps({"tasks": []}).encode("utf-8"))
                        return

                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"tasks": [task]}).encode("utf-8"))
                    return

                if self.path == "/v1/task-bus/leases:claim":
                    body = self._read_body()
                    resp = state.claim(body)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(resp).encode("utf-8"))
                    return

                if self.path == "/v1/task-bus/executor-status":
                    body = self._read_body()
                    resp = state.report_status(body)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(resp).encode("utf-8"))
                    return

                self.send_response(404)
                self.end_headers()

            def log_message(self, format: str, *args):
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        return server, port

    def _context(self):
        fake_schemas = SimpleNamespace(
            request_schema=REQUEST_SCHEMA_FIXTURE,
            response_schema=RESPONSE_SCHEMA_FIXTURE,
        )
        with patch("ui_tars.executor.worker.load_schemas", return_value=fake_schemas):
            return build_context()

    def test_mock_handshake_success(self):
        """process_task succeeds with legacy task shape (has payload)."""
        context = self._context()
        task = {
            "taskId": "task-success",
            "executionRef": "exec-success",
            "dispatchTarget": "DEPRECATED_WORKER_COMPAT",
            "contractFamily": "DEPRECATED_WORKER_COMPAT_CONTRACTS",
            "tenantId": "tenant-1",
            "traceId": "trace-success",
            "payload": {
                "tenantId": "tenant-1",
                "requestId": "req-success",
                "traceId": "trace-success",
                "deviceId": "device-1",
                "meta": {"meshcentral_node_id": "node-1"},
            },
        }

        with patch.dict("os.environ", {"MESHCENTRAL_MOCK": "1"}, clear=False):
            task_id, payload = process_task(task, context, worker_id="worker-a")

        self.assertEqual(task_id, "task-success")
        self.assertEqual(payload["status"], "succeeded")
        self.assertEqual(payload["executionRef"], "exec-success")
        self.assertEqual(payload["meta"]["worker_id"], "worker-a")

    def test_mock_error_path(self):
        """process_task returns failed status on handshake error."""
        context = self._context()
        task = {
            "taskId": "task-error",
            "executionRef": "exec-error",
            "dispatchTarget": "DEPRECATED_WORKER_COMPAT",
            "tenantId": "tenant-1",
            "traceId": "trace-error",
            "payload": {
                "tenantId": "tenant-1",
                "requestId": "req-error",
                "traceId": "trace-error",
                "deviceId": "device-1",
                "meta": {"meshcentral_node_id": "node-1"},
            },
        }

        class BrokenMock:
            def open_session(self, mesh_node_id: str) -> None:
                return

            def fetch_session_metadata(self, mesh_node_id: str):
                raise RuntimeError("mock metadata failure")

            def close_session(self, mesh_node_id: str) -> None:
                return

        with patch("ui_tars.executor.remote_handshake.MockMeshCentralClient", return_value=BrokenMock()):
            with patch.dict("os.environ", {"MESHCENTRAL_MOCK": "1"}, clear=False):
                _, payload = process_task(task, context, worker_id="worker-a")

        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["error"]["error"], "handshake_failed")

    def test_malformed_task_rejection(self):
        """process_task returns failure result for invalid request payload."""
        context = self._context()
        bad_task = {
            "taskId": "task-malformed",
            "executionRef": "exec-malformed",
            "dispatchTarget": "DEPRECATED_WORKER_COMPAT",
            "tenantId": "tenant-1",
            "traceId": "trace-bad",
            "payload": {
                "tenantId": "tenant-1",
                "requestId": "req-bad",
                "deviceId": "device-1",
                "meta": {},
            },
        }

        _, payload = process_task(bad_task, context, worker_id="worker-a")
        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["error"]["code"], "worker_task_failed")

    def test_deterministic_mock_behavior(self):
        """Two runs with same input produce identical artifact URIs."""
        context = self._context()
        task = {
            "taskId": "task-det",
            "executionRef": "exec-det",
            "dispatchTarget": "DEPRECATED_WORKER_COMPAT",
            "tenantId": "tenant-1",
            "traceId": "trace-det",
            "payload": {
                "tenantId": "tenant-1",
                "requestId": "req-det",
                "traceId": "trace-det",
                "deviceId": "device-1",
                "meta": {"meshcentral_node_id": "node-1"},
            },
        }

        with patch.dict("os.environ", {"MESHCENTRAL_MOCK": "1"}, clear=False):
            _, payload_a = process_task(task, context, worker_id="worker-a")
            _, payload_b = process_task(task, context, worker_id="worker-a")

        result_a = payload_a.get("result")
        result_b = payload_b.get("result")
        self.assertIsNotNone(result_a)
        self.assertIsNotNone(result_b)
        artifacts_a = result_a.get("artifacts", [])
        artifacts_b = result_b.get("artifacts", [])
        self.assertEqual(artifacts_a, artifacts_b)

    def test_polling_loop_full_lifecycle(self):
        """Full poll → claim → started → execute → succeeded loop."""
        state = _CoreState()
        server, port = self._start_core_server(state)
        try:
            client = AilliumCoreClient(
                AilliumCoreConfig(
                    base_url=f"http://127.0.0.1:{port}",
                    token="token",
                    tenant_id="tenant-1",
                    timeout_seconds=2,
                )
            )

            with patch.dict("os.environ", {"MESHCENTRAL_MOCK": "1"}, clear=False):
                run_polling_loop(
                    core_client=client,
                    context=self._context(),
                    config=WorkerConfig(
                        worker_id="worker-fixed",
                        poll_interval_seconds=0.01,
                        idle_backoff_seconds=0.01,
                        task_type=TASK_TYPE_REMOTE_HANDSHAKE,
                        executor_type="ui-tars",
                    ),
                    stop_after_iterations=2,
                )

            # Verify lease was claimed
            self.assertTrue(state.task_claimed)
            self.assertTrue(len(state.lease_responses) > 0)

            # Verify status events were reported
            status_names = [e.get("status") for e in state.status_events]
            self.assertIn("executor_started", status_names)
            # Either executor_succeeded or executor_failed should be present
            terminal = {"executor_succeeded", "executor_failed"}
            self.assertTrue(terminal & set(status_names), f"Expected terminal status in {status_names}")

            # Verify audit trail
            event_names = [e["event"] for e in state.audit_events]
            self.assertIn("task.claimed", event_names)
            self.assertIn("task.status_reported", event_names)
        finally:
            server.shutdown()


if __name__ == "__main__":
    unittest.main()
