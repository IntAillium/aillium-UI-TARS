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
    def __init__(self):
        self._lock = threading.Lock()
        self.task = {
            "task_id": "task-1",
            "task_type": TASK_TYPE_REMOTE_HANDSHAKE,
            "payload": {
                "tenantId": "tenant-1",
                "requestId": "req-1",
                "traceId": "trace-1",
                "deviceId": "device-1",
                "meta": {"meshcentral_node_id": "node-1"},
            },
            "status": "PENDING",
        }
        self.task_status_by_id = {"task-1": "PENDING"}
        self.results_by_task_id = {}
        self.audit_events = []

    def claim(self):
        with self._lock:
            if self.task is None:
                return None
            task = self.task
            task_id = task["task_id"]
            self.task_status_by_id[task_id] = "IN_PROGRESS"
            self.audit_events.append(
                {
                    "event": "task.claimed",
                    "task_id": task_id,
                    "tenant_id": task["payload"].get("tenantId"),
                    "trace_id": task["payload"].get("traceId"),
                }
            )
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
                    "tenant_id": payload.get("tenantId"),
                    "trace_id": payload.get("traceId"),
                    "status": payload.get("status"),
                }
            )
            return {"ok": True, "accepted": True}


def _result_shape_is_compatible(payload: dict) -> bool:
    if not isinstance(payload.get("status"), str):
        return False
    if not isinstance(payload.get("result"), dict):
        return False
    artifacts = payload["result"].get("artifacts")
    if not isinstance(artifacts, list):
        return False
    if not payload.get("worker_id") and not payload.get("workerId"):
        return False
    return True


@unittest.skipIf(_IMPORT_ERROR is not None, f"executor deps unavailable: {_IMPORT_ERROR}")
class ExecutorPollingTest(unittest.TestCase):
    def _start_core_server(self, state: _CoreState):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                if self.path.startswith("/api/v1/workers/tasks/poll"):
                    if state.task is None:
                        self.send_response(204)
                        self.end_headers()
                        return

                    task = state.claim()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(task).encode("utf-8"))
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

                if not _result_shape_is_compatible(body):
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "invalid_result_shape"}).encode("utf-8"))
                    return

                response = state.submit_result(task_id, body)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(response).encode("utf-8"))

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
        context = self._context()
        task = {
            "task_id": "task-success",
            "task_type": TASK_TYPE_REMOTE_HANDSHAKE,
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
        self.assertEqual(payload["worker_id"], "worker-a")

    def test_mock_error_path(self):
        context = self._context()
        task = {
            "task_id": "task-error",
            "task_type": TASK_TYPE_REMOTE_HANDSHAKE,
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
        context = self._context()
        bad_task = {
            "task_id": "task-malformed",
            "task_type": TASK_TYPE_REMOTE_HANDSHAKE,
            "payload": {
                "tenantId": "tenant-1",
                "requestId": "req-bad",
                "deviceId": "device-1",
                "meta": {},
            },
        }

        _, payload = process_task(bad_task, context, worker_id="worker-a")
        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["error"]["error"], "worker_task_failed")

    def test_deterministic_mock_behavior(self):
        context = self._context()
        task = {
            "task_id": "task-det",
            "task_type": TASK_TYPE_REMOTE_HANDSHAKE,
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

        artifacts_a = payload_a["result"]["artifacts"]
        artifacts_b = payload_b["result"]["artifacts"]
        self.assertEqual(artifacts_a, artifacts_b)

    def test_polling_posts_result_with_evidence_and_worker_id(self):
        state = _CoreState()
        server, port = self._start_core_server(state)
        try:
            client = AilliumCoreClient(
                AilliumCoreConfig(
                    base_url=f"http://127.0.0.1:{port}",
                    token="token",
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
                    ),
                    stop_after_iterations=2,
                )

            result_payload = state.results_by_task_id.get("task-1")
            self.assertIsNotNone(result_payload)
            self.assertEqual(state.task_status_by_id["task-1"], "COMPLETED")
            self.assertEqual(result_payload["worker_id"], "worker-fixed")
            self.assertEqual(result_payload["meta"]["worker_id"], "worker-fixed")
            artifacts = result_payload["result"].get("artifacts", [])
            self.assertTrue(artifacts)
            for artifact in artifacts:
                self.assertTrue(artifact["uri"].startswith("s3://aillium-evidence/"))

            event_names = [event["event"] for event in state.audit_events]
            self.assertIn("task.claimed", event_names)
            self.assertIn("task.completed", event_names)
        finally:
            server.shutdown()


if __name__ == "__main__":
    unittest.main()
