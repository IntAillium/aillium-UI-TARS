import unittest
from unittest.mock import MagicMock

from ui_tars.executor.remote_handshake import handle_remote_handshake, HandshakeValidationError


class ExecutorRemoteHandshakeTest(unittest.TestCase):
    def _make_request(self, mesh_node_id="node_abc"):
        req = {
            "contractType": "executor.request",
            "schemaVersion": "1.0.0",
            "requestId": "req_123",
            "createdAt": "2026-03-10T00:00:00Z",
            "tenantId": "tenant_1",
            "traceId": "trace_1",
            "deviceId": "device_1",
            "input": {"taskType": "remote.handshake", "payload": {}, "context": {}},
            "meta": {"meshcentral_node_id": mesh_node_id} if mesh_node_id else {},
        }
        return req

    def test_missing_mesh_node_id_rejected(self):
        client = MagicMock()
        req = self._make_request(mesh_node_id=None)
        with self.assertRaises(HandshakeValidationError):
            handle_remote_handshake(req, client=client)

    def test_close_called_on_success(self):
        client = MagicMock()
        client.open_session.return_value = {"ok": True}
        client.fetch_session_metadata.return_value = {"meta": "ok"}
        client.capture_screenshot.return_value = {"ok": True}
        client.close_session.return_value = {"ok": True}

        req = self._make_request(mesh_node_id="node_ok")
        resp = handle_remote_handshake(req, client=client)

        client.open_session.assert_called_once_with("node_ok")
        client.fetch_session_metadata.assert_called_once_with("node_ok")
        client.close_session.assert_called_once_with("node_ok")
        self.assertIn(resp.get("status"), ["succeeded", "failed"])

    def test_close_called_on_failure(self):
        client = MagicMock()
        client.open_session.return_value = {"ok": True}
        client.fetch_session_metadata.side_effect = RuntimeError("boom")
        client.close_session.return_value = {"ok": True}

        req = self._make_request(mesh_node_id="node_fail")
        resp = handle_remote_handshake(req, client=client)

        client.close_session.assert_called_once_with("node_fail")
        self.assertEqual(resp.get("status"), "failed")


if __name__ == "__main__":
    unittest.main()
