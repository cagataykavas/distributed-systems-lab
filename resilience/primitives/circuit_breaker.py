from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeVar

T = TypeVar("T")


class CircuitState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("circuit open")


@dataclass(frozen=True, slots=True)
class CircuitSnapshot:
    state: CircuitState
    failures: int
    opened_at: float | None


class CircuitBreaker:
    """Small synchronous circuit-breaker state machine with injectable time."""

    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_timeout: float = 5.0,
        *,
        retry_on: tuple[type[Exception], ...] = (Exception,),
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least one")
        if recovery_timeout < 0:
            raise ValueError("recovery_timeout must be non-negative")
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.retry_on = retry_on
        self._clock = clock
        self.failures = 0
        self.opened_at: float | None = None
        self.state = CircuitState.CLOSED

    def snapshot(self) -> CircuitSnapshot:
        return CircuitSnapshot(
            state=self.state,
            failures=self.failures,
            opened_at=self.opened_at,
        )

    def _before_call(self) -> None:
        if self.state is not CircuitState.OPEN:
            return
        assert self.opened_at is not None
        if self._clock() - self.opened_at < self.recovery_timeout:
            raise CircuitOpenError()
        self.state = CircuitState.HALF_OPEN

    def _record_success(self) -> None:
        self.failures = 0
        self.opened_at = None
        self.state = CircuitState.CLOSED

    def _record_failure(self) -> None:
        if self.state is CircuitState.HALF_OPEN:
            self.failures = self.failure_threshold
        else:
            self.failures += 1
        if self.failures >= self.failure_threshold:
            self.state = CircuitState.OPEN
            self.opened_at = self._clock()

    def call(self, operation: Callable[[], T]) -> T:
        self._before_call()
        try:
            result = operation()
        except self.retry_on:
            self._record_failure()
            raise
        self._record_success()
        return result
