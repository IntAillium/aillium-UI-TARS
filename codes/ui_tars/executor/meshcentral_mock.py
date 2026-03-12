from __future__ import annotations

from typing import Any


class MockMeshCentralClient:
    """
    Local-only mock for executor development when MeshCentral is not deployed yet.

    Enable with:
      export MESHCENTRAL_MOCK=1

    This avoids requiring:
      MESHCENTRAL_URL / MESHCENTRAL_API_KEY / MESHCENTRAL_TOKEN
    """

    def open_session(self, mesh_node_id: str) -> None:
        # no-op
        return

    def fetch_session_metadata(self, mesh_node_id: str) -> dict[str, Any]:
        return {
            "mock": True,
            "mesh_node_id": mesh_node_id,
            "status": "connected",
            "capabilities": {
                "screenshot": True,
                "rdp": False,
                "clipboard": False,
            },
        }

    def capture_screenshot(self, mesh_node_id: str) -> dict[str, Any]:
        return {
            "mock": True,
            "mesh_node_id": mesh_node_id,
            "image": "mock-screenshot",
            "format": "text/plain",
        }

    def close_session(self, mesh_node_id: str) -> None:
        # no-op
        return
