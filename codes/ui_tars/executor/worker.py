from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from .aillium_core_client import AilliumCoreClient, AilliumCoreRetryableError
from .audit import Correlation, emit, setup_logging
from .remote_handshake import (
    RemoteHandshakeValidationError,
    execute_remote_handshake,
)
from .schema_loader import SchemaLoadError, load_schemas

TASK_TYPE_REMOTE_HANDSHAKE = "remote-handshake"


@dataclass(frozen=True)
class WorkerContext:
    request_validator: Draft202012Validator
    response_schema: dict[str, Any]


@dataclass(frozen=True)
class WorkerConfig:
    worker_id: str
    poll_interval_seconds: float
    idle_backoff_seconds: float
    task_type: str

    @staticmethod
    def load_from_env() -> "WorkerConfig":
        worker_id = _load_or_create_worker_id()
        poll_interval_seconds = float(os.getenv("AILLIUM_POLL_INTERVAL_SECONDS", "0.2"))
        idle_backoff_seconds = float(os.getenv("AILLIUM_IDLE_BACKOFF_SECONDS", "1.0"))
        task_type = os.getenv("AILLIUM_TASK_TYPE", TASK_TYPE_REMOTE_HANDSHAKE).strip()
        return WorkerConfig(
            worker_id=worker_id,
            poll_interval_seconds=poll_interval_seconds,
            idle_backoff_seconds=idle_backoff_seconds,
            task_type=task_type,
        )


def _load_or_create_worker_id() -> str:
    explicit = os.getenv("AILLIUM_WORKER_ID", "").strip()
    if explicit:
        return explicit

    worker_id_file = Path(
        os.getenv("AILLIUM_WORKER_ID_FILE", str(Path.home() / ".aillium-worker-id"))
    )
    if worker_id_file.exists():
        existing = worker_id_file.read_text(encoding="utf-8").strip()
        if existing:
            return existing

    generated = f"ui-tars-{uuid.uuid4()}"
    worker_id_file.parent.mkdir(parents=True, exist_ok=True)
    worker_id_file.write_text(generated, encoding="utf-8")
    return generated


def build_context() -> WorkerContext:
    schemas = load_schemas()
    return WorkerContext(
        request_validator=Draft202012Validator(schemas.request_schema),
        response_schema=schemas.response_schema,
    )


def _extract_task_id(task: dict[str, Any]) -> str:
    raw = task.get("taskId") or task.get("task_id")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("task_id is required")
    return raw.strip()


def _extract_task_type(task: dict[str, Any]) -> str:
    raw = task.get("type") or task.get("taskType") or task.get("task_type")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("task_type is required")
    return raw.strip()


def _extract_request_payload(task: dict[str, Any]) -> dict[str, Any]:
    payload = task.get("payload") if isinstance(task.get("payload"), dict) else None
    if payload is None and isinstance(task.get("request"), dict):
        payload = task.get("request")
    if not isinstance(payload, dict):
        raise ValueError("task payload must be an object")
    return payload


def _correlation_from_request(request_payload: dict[str, Any]) -> Correlation:
    return Correlation(
        tenant_id=request_payload.get("tenantId"),
        request_id=request_payload.get("requestId"),
        trace_id=request_payload.get("traceId"),
    )


def _build_failure_result(
    *,
    task_id: str,
    worker_id: str,
    request_payload: dict[str, Any],
    message: str,
) -> dict[str, Any]:
    return {
        "worker_id": worker_id,
        "workerId": worker_id,
        "task_id": task_id,
        "taskId": task_id,
        "tenantId": request_payload.get("tenantId"),
        "requestId": request_payload.get("requestId"),
        "traceId": request_payload.get("traceId"),
        "status": "failed",
        "result": {
            "message": "Worker task failed before execution",
            "artifacts": [],
            "warnings": [message],
        },
        "error": {
            "error": "worker_task_failed",
            "message": message,
        },
        "meta": {
            "worker_id": worker_id,
            "workerId": worker_id,
        },
    }


def _emit_task_event(
    event: str,
    correlation: Correlation,
    *,
    task_id: str,
    worker_id: str,
    task_type: str,
    status: str,
    error: str | None = None,
) -> None:
    payload: dict[str, Any] = {
        "trace_id": correlation.trace_id,
        "task_id": task_id,
        "tenant_id": correlation.tenant_id,
        "worker_id": worker_id,
        "task_type": task_type,
        "status": status,
    }
    if error is not None:
        payload["error"] = error
    emit(event, correlation, **payload)


def process_task(task: dict[str, Any], context: WorkerContext, worker_id: str) -> tuple[str, dict[str, Any]]:
    task_id = _extract_task_id(task)
    task_type = _extract_task_type(task)
    request_payload = _extract_request_payload(task)
    correlation = _correlation_from_request(request_payload)

    if task_type != TASK_TYPE_REMOTE_HANDSHAKE:
        raise ValueError(f"unsupported task type: {task_type}")

    _emit_task_event(
        "worker.task.claimed",
        correlation,
        task_id=task_id,
        worker_id=worker_id,
        task_type=task_type,
        status="claimed",
    )

    try:
        result = execute_remote_handshake(
            request_payload=request_payload,
            request_validator=context.request_validator,
            response_schema=context.response_schema,
            headers={},
        )
    except (ValidationError, RemoteHandshakeValidationError) as exc:
        _emit_task_event(
            "worker.task.rejected",
            correlation,
            task_id=task_id,
            worker_id=worker_id,
            task_type=task_type,
            status="failed",
            error=str(exc),
        )
        return task_id, _build_failure_result(
            task_id=task_id,
            worker_id=worker_id,
            request_payload=request_payload,
            message=str(exc),
        )

    callback_payload = {
        "worker_id": worker_id,
        "workerId": worker_id,
        "task_id": task_id,
        "taskId": task_id,
        "tenantId": result.get("tenantId"),
        "requestId": result.get("requestId"),
        "traceId": result.get("traceId"),
        "status": result.get("status"),
        "result": result.get("result"),
        "error": result.get("error"),
        "meta": {
            **(result.get("meta") if isinstance(result.get("meta"), dict) else {}),
            "worker_id": worker_id,
            "workerId": worker_id,
        },
    }

    _emit_task_event(
        "worker.task.completed",
        correlation,
        task_id=task_id,
        worker_id=worker_id,
        task_type=task_type,
        status=str(callback_payload.get("status")),
    )
    return task_id, callback_payload


def run_polling_loop(
    *,
    core_client: AilliumCoreClient | None = None,
    context: WorkerContext | None = None,
    config: WorkerConfig | None = None,
    stop_after_iterations: int | None = None,
) -> None:
    setup_logging()

    client = core_client or AilliumCoreClient()
    worker_context = context or build_context()
    worker_config = config or WorkerConfig.load_from_env()

    iterations = 0

    while True:
        if stop_after_iterations is not None and iterations >= stop_after_iterations:
            return
        iterations += 1

        try:
            task = client.poll_executor_task(worker_config.task_type)
            if task is None:
                time.sleep(worker_config.idle_backoff_seconds)
                continue

            task_id, result_payload = process_task(task, worker_context, worker_config.worker_id)
            client.submit_executor_result(task_id, result_payload)
        except AilliumCoreRetryableError:
            time.sleep(worker_config.poll_interval_seconds)
        except Exception as exc:
            emit(
                "worker.loop.error",
                Correlation(None, None, None),
                worker_id=worker_config.worker_id,
                task_type=worker_config.task_type,
                status="failed",
                errorType=type(exc).__name__,
                error=str(exc),
            )
            time.sleep(worker_config.poll_interval_seconds)


def run() -> None:
    try:
        run_polling_loop()
    except SchemaLoadError as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    run()
