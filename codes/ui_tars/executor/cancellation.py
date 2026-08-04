from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Callable


@dataclass(frozen=True)
class CancellationCommand:
    task_id: str
    fence_token: int
    cancellation_generation: int
    requested_at: datetime
    reason: str


class TaskCancellationRequested(RuntimeError):
    """Raised when Core has fenced this execution attempt."""

    def __init__(self, command: CancellationCommand):
        super().__init__(
            f"task cancellation requested generation={command.cancellation_generation} "
            f"fence={command.fence_token}"
        )
        self.command = command


class CancellationScope:
    """Thread-safe cancellation signal with teardown acknowledgement.

    Teardown callbacks are registered by side-effecting transports. Cancellation
    is considered acknowledged only after every registered callback returns.
    A callback registered after cancellation runs immediately, closing the race
    between command polling and resource acquisition.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cancelled = threading.Event()
        self._teardown_complete = threading.Event()
        self._callbacks: dict[int, Callable[[], None]] = {}
        self._next_callback_id = 1
        self._command: CancellationCommand | None = None
        self._teardown_error: Exception | None = None
        self._late_teardowns = 0
        self._cancel_callbacks_done = False

    @property
    def command(self) -> CancellationCommand | None:
        with self._lock:
            return self._command

    @property
    def teardown_error(self) -> Exception | None:
        with self._lock:
            return self._teardown_error

    def register_teardown(self, callback: Callable[[], None]) -> Callable[[], None]:
        with self._lock:
            callback_id = self._next_callback_id
            self._next_callback_id += 1
            if not self._cancelled.is_set():
                self._callbacks[callback_id] = callback
                return lambda: self._unregister(callback_id)
            self._late_teardowns += 1

        # Cancellation won the registration race. Teardown this resource before
        # returning control to the caller that was about to use it.
        try:
            callback()
        except Exception as exc:
            with self._lock:
                self._teardown_error = exc
            raise
        finally:
            with self._lock:
                self._late_teardowns -= 1
                if (
                    self._cancel_callbacks_done
                    and self._late_teardowns == 0
                    and self._teardown_error is None
                ):
                    self._teardown_complete.set()
        return lambda: None

    def _unregister(self, callback_id: int) -> None:
        with self._lock:
            self._callbacks.pop(callback_id, None)

    def cancel(self, command: CancellationCommand) -> bool:
        with self._lock:
            if self._command is not None:
                if self._command != command:
                    raise ValueError("conflicting cancellation command")
                return self._teardown_complete.is_set()
            self._command = command
            self._cancelled.set()
            callbacks = list(self._callbacks.values())

        try:
            for callback in callbacks:
                callback()
        except Exception as exc:
            with self._lock:
                self._teardown_error = exc
                self._cancel_callbacks_done = True
            return False

        with self._lock:
            self._cancel_callbacks_done = True
            if self._late_teardowns == 0 and self._teardown_error is None:
                self._teardown_complete.set()
        return self._teardown_complete.is_set()

    def checkpoint(self) -> None:
        command = self.command
        if command is not None:
            raise TaskCancellationRequested(command)

    def wait_for_teardown(self, timeout_seconds: float) -> bool:
        return self._teardown_complete.wait(timeout_seconds)
