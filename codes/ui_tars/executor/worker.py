from __future__ import annotations

import os
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

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

# Default executor type used when polling the task-bus.
DEFAULT_EXECUTOR_TYPE = "ui-tars"


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
    executor_type: str
    visibility_timeout_seconds: int = 60
    lease_renew_interval_seconds: float = 20.0

    @staticmethod
    def load_from_env() -> "WorkerConfig":
        worker_id = _load_or_create_worker_id()
        poll_interval_seconds = float(os.getenv("AILLIUM_POLL_INTERVAL_SECONDS", "0.2"))
        idle_backoff_seconds = float(os.getenv("AILLIUM_IDLE_BACKOFF_SECONDS", "1.0"))
        task_type = os.getenv("AILLIUM_TASK_TYPE", TASK_TYPE_REMOTE_HANDSHAKE).strip()
        executor_type = os.getenv("AILLIUM_EXECUTOR_TYPE", DEFAULT_EXECUTOR_TYPE).strip()
        visibility_timeout_seconds = int(
            os.getenv("AILLIUM_VISIBILITY_TIMEOUT_SECONDS", "60")
        )
        lease_renew_interval_seconds = float(
            os.getenv(
                "AILLIUM_LEASE_RENEW_INTERVAL_SECONDS",
                str(max(1.0, visibility_timeout_seconds / 3)),
            )
        )
        return WorkerConfig(
            worker_id=worker_id,
            poll_interval_seconds=poll_interval_seconds,
            idle_backoff_seconds=idle_backoff_seconds,
            task_type=task_type,
            executor_type=executor_type,
            visibility_timeout_seconds=visibility_timeout_seconds,
            lease_renew_interval_seconds=lease_renew_interval_seconds,
        )


class _LeaseRenewer:
    """Renew a lease in the background for the full execution window."""

    def __init__(
        self,
        *,
        client: AilliumCoreClient,
        task_id: str,
        executor_type: str,
        lease_token: str,
        visibility_timeout_seconds: int,
        interval_seconds: float,
    ):
        self._client = client
        self._task_id = task_id
        self._executor_type = executor_type
        self._lease_token = lease_token
        self._visibility_timeout_seconds = visibility_timeout_seconds
        self._interval_seconds = max(0.01, interval_seconds)
        self._stop = threading.Event()
        self._error: Exception | None = None
        self._thread = threading.Thread(
            target=self._run,
            name=f"lease-renewer-{task_id}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(1.0, self._interval_seconds + 0.5))

    def assert_active(self) -> None:
        if self._error is not None:
            raise AilliumCoreRetryableError(
                f"task lease renewal failed: {self._error}"
            ) from self._error

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                self._client.renew_task_lease(
                    task_id=self._task_id,
                    executor_type=self._executor_type,
                    lease_token=self._lease_token,
                    visibility_timeout_seconds=self._visibility_timeout_seconds,
                )
            except Exception as exc:  # surfaced synchronously by assert_active
                self._error = exc
                self._stop.set()
                return


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
    execution_ref: str = "",
    executor_type: str = "ui-tars",
    trace_id: str = "",
) -> dict[str, Any]:
    return {
        "executionRef": execution_ref or f"exec-{task_id}",
        "executorType": executor_type,
        "traceId": trace_id or request_payload.get("traceId", ""),
        "tenantId": request_payload.get("tenantId", ""),
        "status": "failed",
        "result": {
            "message": "Worker task failed before execution",
            "artifacts": [],
            "warnings": [message],
        },
        "error": {
            "code": "worker_task_failed",
            "message": message,
        },
        "artifacts": [],
        "meta": {
            "worker_id": worker_id,
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


def process_task(
    task: dict[str, Any],
    context: WorkerContext,
    worker_id: str,
    lease_checkpoint: Callable[[], None] | None = None,
) -> tuple[str, dict[str, Any]]:
    """Process a task from the task-bus.

    The task dict comes from the task-bus poll response and contains:
    taskId, executionRef, dispatchTarget, contractFamily,
    expectedEvidenceKind, compatibilityMode, traceId, tenantId.

    For backward compatibility, also supports the legacy shape with
    type/payload fields.
    """
    task_id = _extract_task_id(task)
    execution_ref = task.get("executionRef", f"exec-{task_id}")
    executor_type = task.get("executorType", DEFAULT_EXECUTOR_TYPE)
    trace_id = task.get("traceId", "")
    tenant_id = task.get("tenantId", "")

    # Extract task_type: from dispatchTarget hint or legacy field
    dispatch_target = task.get("dispatchTarget", "")
    if dispatch_target == "DEPRECATED_WORKER_COMPAT":
        task_type = TASK_TYPE_REMOTE_HANDSHAKE
    else:
        task_type = _extract_task_type(task) if task.get("type") or task.get("taskType") or task.get("task_type") else TASK_TYPE_REMOTE_HANDSHAKE

    # Build correlation from task-bus fields
    correlation = Correlation(
        tenant_id=tenant_id or None,
        request_id=execution_ref,
        trace_id=trace_id or None,
    )

    # Try to extract a request payload; for task-bus tasks this may not exist
    # in which case we build a minimal one from the task-bus fields.
    try:
        request_payload = _extract_request_payload(task)
    except ValueError:
        request_payload = {
            "tenantId": tenant_id,
            "requestId": execution_ref,
            "traceId": trace_id,
        }

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
            lease_checkpoint=lease_checkpoint,
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
            execution_ref=execution_ref,
            executor_type=executor_type,
            trace_id=trace_id,
        )

    # Build callback payload aligned with Core's executor-status API
    result_status = result.get("status", "failed")
    artifacts = []
    result_data = result.get("result")
    if isinstance(result_data, dict):
        for a in result_data.get("artifacts", []):
            if isinstance(a, dict) and "uri" in a:
                artifacts.append({"uri": a["uri"]})

    callback_payload = {
        "executionRef": execution_ref,
        "executorType": executor_type,
        "traceId": trace_id,
        "tenantId": tenant_id,
        "status": result_status,
        "result": result_data,
        "error": result.get("error"),
        "artifacts": artifacts,
        "meta": {
            **(result.get("meta") if isinstance(result.get("meta"), dict) else {}),
            "worker_id": worker_id,
        },
    }

    _emit_task_event(
        "worker.task.completed",
        correlation,
        task_id=task_id,
        worker_id=worker_id,
        task_type=task_type,
        status=str(result_status),
    )
    return task_id, callback_payload


def run_polling_loop(
    *,
    core_client: AilliumCoreClient | None = None,
    context: WorkerContext | None = None,
    config: WorkerConfig | None = None,
    stop_after_iterations: int | None = None,
) -> None:
    """Run the worker polling loop.

    Loop sequence per iteration:
    1. Poll task-bus for eligible tasks (POST /v1/task-bus/tasks:poll)
    2. Claim lease on the task (POST /v1/task-bus/leases:claim)
    3. Report executor_started status
    4. Execute the task deterministically
    5. Report executor_succeeded or executor_failed status with artifacts
    """
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
            # Step 1: Poll for tasks using executor_type
            task = client.poll_executor_task(worker_config.executor_type)
            if task is None:
                time.sleep(worker_config.idle_backoff_seconds)
                continue

            task_id = _extract_task_id(task)
            execution_ref = task.get("executionRef", f"exec-{task_id}")
            trace_id = task.get("traceId", str(uuid.uuid4()))

            # Step 2: Claim the lease
            try:
                lease = client.claim_task_lease(
                    task_id=task_id,
                    executor_type=worker_config.executor_type,
                    visibility_timeout_seconds=worker_config.visibility_timeout_seconds,
                    execution_ref=execution_ref,
                    attempt_id=trace_id,
                )
            except AilliumCoreRetryableError:
                time.sleep(worker_config.poll_interval_seconds)
                continue

            lease_token = lease.get("leaseToken")
            if not isinstance(lease_token, str) or not lease_token.strip():
                raise ValueError("claim response missing leaseToken")

            # Poll responses intentionally contain dispatch metadata only. Task
            # content is released after claim through the lease-bound endpoint.
            task_payload = client.fetch_task_payload(
                task_id=task_id,
                lease_token=lease_token,
            )
            task = {
                **task,
                "payload": task_payload.get("payload"),
                "conversationKey": task_payload.get("conversationKey"),
            }

            renewer = _LeaseRenewer(
                client=client,
                task_id=task_id,
                executor_type=worker_config.executor_type,
                lease_token=lease_token,
                visibility_timeout_seconds=worker_config.visibility_timeout_seconds,
                interval_seconds=worker_config.lease_renew_interval_seconds,
            )
            renewer.start()
            try:
                # Step 3: Report executor_started
                client.report_executor_status(
                    execution_ref=execution_ref,
                    executor_type=worker_config.executor_type,
                    status="executor_started",
                    trace_id=trace_id,
                    lease_token=lease_token,
                    task_id=task_id,
                )

                # Step 4: Execute the task
                _task_id, result_payload = process_task(
                    task,
                    worker_context,
                    worker_config.worker_id,
                    lease_checkpoint=renewer.assert_active,
                )
                renewer.assert_active()

                # Step 5: Report final status
                raw_status = result_payload.get("status", "failed")
                status_map = {
                    "succeeded": "executor_succeeded",
                    "failed": "executor_failed",
                    "cancelled": "executor_cancelled",
                    "timed_out": "executor_timed_out",
                }
                final_status = status_map.get(raw_status, "executor_failed")

                artifacts = result_payload.get("artifacts")

                client.report_executor_status(
                    execution_ref=execution_ref,
                    executor_type=worker_config.executor_type,
                    status=final_status,
                    trace_id=trace_id,
                    lease_token=lease_token,
                    task_id=task_id,
                    artifacts=artifacts if artifacts else None,
                )
            finally:
                renewer.stop()
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
