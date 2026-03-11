import json
import os
import tempfile
import threading
import time
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

try:
    from ui_tars.executor.server import build_context, create_handler

    _IMPORT_ERROR = None
except Exception as exc:  # pragma: no cover
    build_context = None
    create_handler = None
    _IMPORT_ERROR = exc


REQUEST_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "executor.request",
    "type": "object",
    "additionalProperties": True,
    "required": ["tenantId", "requestId", "meta"],
    "properties": {
        "tenantId": {"type": "string"},
        "requestId": {"type": "string"},
        "traceId": {"type": "string"},
        "meta": {
            "type": "object",
            "additionalProperties": True,
            "required": ["deviceId"],
            "properties": {
                "meshcentral_node_id": {"type": "string"},
                "deviceId": {"type": "string"},
            },
        },
    },
}

RESPONSE_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "executor.response",
    "type": "object",
    "additionalProperties": True,
    "required": [
        "tenantId",
        "requestId",
        "traceId",
        "status",
        "startedAt",
        "finishedAt",
        "durationMs",
        "artifacts",
        "logs",
    ],
    "properties": {
        "tenantId": {"type": "string"},
        "requestId": {"type": "string"},
        "traceId": {"type": ["string", "null"]},
        "status": {"type": "string", "enum": ["succeeded", "failed"]},
        "startedAt": {"type": "string", "format": "date-time"},
        "finishedAt": {"type": "string", "format": "date-time"},
        "durationMs": {"type": "integer", "minimum": 0},
        "artifacts": {"type": "array", "minItems": 1},
        "logs": {"type": "array", "minItems": 1},
    },
}


@unittest.skipIf(_IMPORT_ERROR is not None, f"executor deps unavailable: {_IMPORT_ERROR}")
class ExecutorRemoteHandshakeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.TemporaryDirectory()
        root = Path(cls.tmpdir.name)
        (root / "executor.request.json").write_text(json.dumps(REQUEST_SCHEMA), encoding="utf-8")
        (root / "executor.response.json").write_text(json.dumps(RESPONSE_SCHEMA), encoding="utf-8")
        os.environ["AILLIUM_SCHEMAS_OVERRIDE_DIR"] = cls.tmpdir.name

        context = build_context()
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), create_handler(context))
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        time.sleep(0.05)

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.thread.join(timeout=2)
        cls.tmpdir.cleanup()
        os.environ.pop("AILLIUM_SCHEMAS_OVERRIDE_DIR", None)

    def setUp(self):
        os.environ.pop("AILLIUM_CORE_BASE_URL", None)
        os.environ.pop("AILLIUM_CORE_TOKEN", None)
        os.environ.pop("AILLIUM_CORE_TIMEOUT_SECONDS", None)

    def _post(self, payload, headers=None):
        conn = HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request(
            "POST",
            "/executor/remote-handshake",
            body=json.dumps(payload),
            headers={"Content-Type": "application/json", **(headers or {})},
        )
        resp = conn.getresponse()
        body = resp.read().decode("utf-8")
        conn.close()
        return resp.status, json.loads(body)

    def test_success_calls_close_session(self):
        with patch("ui_tars.executor.remote_handshake.MeshCentralClient") as client_cls:
            client = client_cls.return_value
            client.open_session.return_value = {"ok": True}
            client.fetch_session_metadata.return_value = {"hostname": "dev1"}
            client.capture_screenshot.return_value = {"artifact": "fake"}
            client.close_session.return_value = {"closed": True}

            status, body = self._post(
                {
                    "tenantId": "t-1",
                    "requestId": "r-1",
                    "meta": {"meshcentral_node_id": "node-1", "deviceId": "device-1"},
                },
                headers={"x-trace-id": "trace-1"},
            )

            self.assertEqual(status, 200)
            self.assertEqual(body["status"], "succeeded")
            client.close_session.assert_called_once_with("node-1")

    def test_success_uses_core_resolved_node_id(self):
        os.environ["AILLIUM_CORE_BASE_URL"] = "http://aillium-core.local"
        os.environ["AILLIUM_CORE_TOKEN"] = "token"

        with (
            patch("ui_tars.executor.remote_handshake.MeshCentralClient") as mesh_client_cls,
            patch("ui_tars.executor.remote_handshake.AilliumCoreClient") as core_client_cls,
        ):
            mesh_client = mesh_client_cls.return_value
            mesh_client.open_session.return_value = {"ok": True}
            mesh_client.fetch_session_metadata.return_value = {"hostname": "dev1"}
            mesh_client.capture_screenshot.return_value = {"artifact": "fake"}
            mesh_client.close_session.return_value = {"closed": True}
            core_client_cls.return_value.resolve_meshcentral_node_id.return_value = "resolved-node-9"

            status, body = self._post(
                {
                    "tenantId": "t-1",
                    "requestId": "r-core",
                    "meta": {"meshcentral_node_id": "ignored-node", "deviceId": "device-1"},
                }
            )

            self.assertEqual(status, 200)
            self.assertEqual(body["status"], "succeeded")
            core_client_cls.return_value.resolve_meshcentral_node_id.assert_called_once_with(
                tenant_id="t-1",
                device_id="device-1",
            )
            mesh_client.open_session.assert_called_once_with("resolved-node-9")
            mesh_client.close_session.assert_called_once_with("resolved-node-9")

    def test_core_404_yields_400_response(self):
        os.environ["AILLIUM_CORE_BASE_URL"] = "http://aillium-core.local"
        os.environ["AILLIUM_CORE_TOKEN"] = "token"

        with patch("ui_tars.executor.remote_handshake.AilliumCoreClient") as core_client_cls:
            from ui_tars.executor.aillium_core_client import AilliumCoreDeviceNotFoundError

            core_client_cls.return_value.resolve_meshcentral_node_id.side_effect = AilliumCoreDeviceNotFoundError("missing")

            status, body = self._post(
                {
                    "tenantId": "t-1",
                    "requestId": "r-404",
                    "meta": {"deviceId": "device-missing"},
                }
            )

            self.assertEqual(status, 400)
            self.assertEqual(body["reasonCode"], "DEVICE_NOT_FOUND")

    def test_failure_still_calls_close_session(self):
        with patch("ui_tars.executor.remote_handshake.MeshCentralClient") as client_cls:
            client = client_cls.return_value
            client.open_session.return_value = {"ok": True}
            client.fetch_session_metadata.side_effect = RuntimeError("metadata unavailable")
            client.close_session.return_value = {"closed": True}

            status, body = self._post(
                {
                    "tenantId": "t-1",
                    "requestId": "r-2",
                    "meta": {"meshcentral_node_id": "node-2", "deviceId": "device-2"},
                }
            )

            self.assertEqual(status, 200)
            self.assertEqual(body["status"], "failed")
            client.close_session.assert_called_once_with("node-2")


if __name__ == "__main__":
    unittest.main()
