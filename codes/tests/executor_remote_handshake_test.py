import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from http.server import ThreadingHTTPServer
import threading
import urllib.request
import urllib.error

from ui_tars.executor.server import build_context, create_handler


# Minimal local schema fixtures so tests don't depend on aillium-schemas packaging.
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

# Keep response schema permissive for tests; server validates response against this.
RESPONSE_SCHEMA_FIXTURE = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": True,
}


class ExecutorRemoteHandshakeTest(unittest.TestCase):
    def _start_server(self):
        # Patch schema loading so build_context() doesn't require aillium-schemas resources.
        fake_schemas = SimpleNamespace(
            request_schema=REQUEST_SCHEMA_FIXTURE,
            response_schema=RESPONSE_SCHEMA_FIXTURE,
        )

        with patch("ui_tars.executor.server.load_schemas", return_value=fake_schemas):
            context = build_context()

        handler_cls = create_handler(context)
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
        port = server.server_address[1]
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        return server, port

    def _post_json(self, port: int, path: str, payload: dict):
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url=f"http://127.0.0.1:{port}{path}",
            method="POST",
            headers={"Content-Type": "application/json"},
            data=data,
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body)

    def test_success_calls_close_session(self):
        server, port = self._start_server()
        try:
            mc = MagicMock()
            mc.fetch_session_metadata.return_value = {"ok": True}
            mc.capture_screenshot.return_value = {"image": "fake"}

            with patch("ui_tars.executor.remote_handshake.MeshCentralClient", return_value=mc):
                payload = {
                    "tenantId": "t1",
                    "requestId": "r1",
                    "deviceId": "d1",
                    "meta": {"meshcentral_node_id": "node-1"},
                }
                status, resp = self._post_json(port, "/executor/remote-handshake", payload)
                self.assertEqual(status, 200)
                self.assertEqual(resp["status"], "succeeded")
                mc.open_session.assert_called_once_with("node-1")
                mc.fetch_session_metadata.assert_called_once_with("node-1")
                mc.close_session.assert_called_once_with("node-1")
        finally:
            server.shutdown()

    def test_failure_still_calls_close_session(self):
        server, port = self._start_server()
        try:
            mc = MagicMock()
            mc.fetch_session_metadata.side_effect = RuntimeError("metadata boom")

            with patch("ui_tars.executor.remote_handshake.MeshCentralClient", return_value=mc):
                payload = {
                    "tenantId": "t1",
                    "requestId": "r1",
                    "deviceId": "d1",
                    "meta": {"meshcentral_node_id": "node-1"},
                }
                status, resp = self._post_json(port, "/executor/remote-handshake", payload)

                # Current server behavior: remote-handshake returns 200 with status=failed on execution failure
                self.assertEqual(status, 200)
                self.assertEqual(resp["status"], "failed")
                mc.open_session.assert_called_once_with("node-1")
                mc.close_session.assert_called_once_with("node-1")
        finally:
            server.shutdown()

    def test_missing_mesh_node_id_returns_400(self):
        server, port = self._start_server()
        try:
            payload = {
                "tenantId": "t1",
                "requestId": "r1",
                "deviceId": "d1",
                "meta": {},
            }
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                url=f"http://127.0.0.1:{port}/executor/remote-handshake",
                method="POST",
                headers={"Content-Type": "application/json"},
                data=data,
            )
            try:
                urllib.request.urlopen(req, timeout=3)
                self.fail("Expected HTTPError")
            except urllib.error.HTTPError as e:
                self.assertEqual(e.code, 400)
                body = json.loads(e.read().decode("utf-8"))
                self.assertEqual(body["error"], "request_schema_validation_failed")
        finally:
            server.shutdown()


if __name__ == "__main__":
    unittest.main()