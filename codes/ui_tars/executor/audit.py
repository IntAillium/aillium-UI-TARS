from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


LOGGER = logging.getLogger("ui_tars.executor")


@dataclass(frozen=True)
class Correlation:
    tenant_id: str | None
    request_id: str | None
    trace_id: str | None


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def setup_logging() -> None:
    if LOGGER.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)


def emit(event: str, correlation: Correlation, **fields: Any) -> None:
    payload: dict[str, Any] = {
        "timestamp": now_utc_iso(),
        "event": event,
        "tenantId": correlation.tenant_id,
        "requestId": correlation.request_id,
        "traceId": correlation.trace_id,
    }
    payload.update(fields)
    LOGGER.info(json.dumps(payload, separators=(",", ":"), sort_keys=True))
