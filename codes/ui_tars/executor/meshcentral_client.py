import os
import json
import urllib.request
import urllib.error
from dataclasses import dataclass


@dataclass
class MeshCentralConfig:
    url: str
    token: str
    timeout_seconds: int = 10


class MeshCentralError(RuntimeError):
    pass


class MeshCentralClient:
    """
    Minimal HTTP client wrapper for MeshCentral.

    - Data Plane connectivity only
    - No autonomy, no policy enforcement
    - Used by executor to open/close sessions + fetch metadata/artifacts
    """

    def __init__(self, config: MeshCentralConfig):
        self.config = config

    @classmethod
    def from_env(cls) -> "MeshCentralClient":
        url = os.getenv("MESHCENTRAL_URL", "").strip()
        token = (os.getenv("MESHCENTRAL_API_KEY") or os.getenv("MESHCENTRAL_TOKEN") or "").strip()
        timeout = int(os.getenv("MESHCENTRAL_TIMEOUT_SECONDS", "10"))

        if not url:
            raise MeshCentralError("MESHCENTRAL_URL is required")
        if not token:
            raise MeshCentralError("MESHCENTRAL_API_KEY or MESHCENTRAL_TOKEN is required")

        return cls(MeshCentralConfig(url=url.rstrip("/"), token=token, timeout_seconds=timeout))

    def _post_json(self, path: str, payload: dict) -> dict:
        url = f"{self.config.url}{path}"
        headers = {
            "Content-Type": "application/json",
            # Token header naming can be adjusted to match your MeshCentral deployment
            "Authorization": f"Bearer {self.config.token}",
        }
        data = json.dumps(payload).encode("utf-8")

        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.config.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8") if resp.readable() else ""
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            raise MeshCentralError(f"MeshCentral HTTPError {e.code}: {e.reason}") from e
        except urllib.error.URLError as e:
            raise MeshCentralError(f"MeshCentral URLError: {e.reason}") from e

    def auth_check(self) -> dict:
        return self._post_json("/api/auth/check", {})

    def open_session(self, mesh_node_id: str) -> dict:
        return self._post_json("/api/session/open", {"node_id": mesh_node_id})

    def fetch_session_metadata(self, mesh_node_id: str) -> dict:
        return self._post_json("/api/session/metadata", {"node_id": mesh_node_id})

    def capture_screenshot(self, mesh_node_id: str) -> dict:
        return self._post_json("/api/session/screenshot", {"node_id": mesh_node_id})

    def close_session(self, mesh_node_id: str) -> dict:
        return self._post_json("/api/session/close", {"node_id": mesh_node_id})
