import json
import os
import tempfile
import threading
import time
import unittest
from http.client import HTTPConnection
from pathlib import Path

from http.server import ThreadingHTTPServer

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
    "additionalProperties": False,
    "required": ["tenantId", "requestId", "meta"],
    "properties": {
        "tenantId": {"type": "string"},
        "requestId": {"type": "string"},
        "traceId": {"type": "string"},
        "meta": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "simulate_failure": {"type": "boolean"}
            }
        }
    }
}

RESPONSE_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "executor.response",
    "type": "object",
    "additionalProperties": False,
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
        "traceId": {"type": "string"},
        "status": {"type": "string", "enum": ["succeeded", "failed"]},
        "startedAt": {"type": "string", "format": "date-time"},
        "finishedAt": {"type": "string", "format": "date-time"},
        "durationMs": {"type": "integer", "minimum": 0},
        "artifacts": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["uri", "key"],
                "properties": {
                    "uri": {"type": "string"},
                    "key": {"type": "string"}
                }
            }
        },
        "logs": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["step", "message", "timestamp"],
                "properties": {
                    "step": {"type": "string"},
                    "message": {"type": "string"},
                    "timestamp": {"type": "string", "format": "date-time"}
                }
            }
        }
    }
}


@unittest.skipIf(_IMPORT_ERROR is not None, f"executor deps unavailable: {_IMPORT_ERROR}")
class ExecutorDryRunTest(unittest.TestCase):
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

    def _post(self, payload, headers=None):
        conn = HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request(
            "POST",
            "/executor/dry-run",
            body=json.dumps(payload),
            headers={"Content-Type": "application/json", **(headers or {})},
        )
        resp = conn.getresponse()
        body = resp.read().decode("utf-8")
        conn.close()
        return resp.status, json.loads(body)

    def test_valid_request_succeeds(self):
        status, body = self._post(
            {"tenantId": "t-1", "requestId": "r-1", "meta": {}},
            headers={"x-trace-id": "trace-1"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "succeeded")
        self.assertEqual(body["tenantId"], "t-1")
        self.assertEqual(body["requestId"], "r-1")
        self.assertEqual(body["traceId"], "trace-1")
        self.assertGreaterEqual(body["durationMs"], 0)
        self.assertTrue(body["artifacts"][0]["uri"].startswith("s3://aillium-dry-run/"))

    def test_simulate_failure(self):
        status, body = self._post(
            {
                "tenantId": "t-1",
                "requestId": "r-2",
                "meta": {"simulate_failure": True},
            },
            headers={"x-trace-id": "trace-2"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "failed")

    def test_invalid_request_returns_400(self):
        status, body = self._post({"requestId": "r-3", "meta": {}})
        self.assertEqual(status, 400)
        self.assertEqual(body["error"], "request_schema_validation_failed")


if __name__ == "__main__":
    unittest.main()
