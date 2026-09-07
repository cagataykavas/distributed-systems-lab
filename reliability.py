from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


def retry(
    operation: Callable[[], T],
    attempts: int = 5,
    base_delay: float = 0.05,
    retry_on: tuple[type[Exception], ...] = (Exception,),
) -> T:
    if attempts < 1:
        raise ValueError("attempts must be at least one")
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            return operation()
        except retry_on as exc:
            last_error = exc
            if attempt == attempts - 1:
                break
            delay = base_delay * (2**attempt)
            delay *= random.uniform(0.5, 1.5)
            time.sleep(delay)
    if last_error is None:
        raise RuntimeError("retry loop exited without an operation result")
    raise last_error


@dataclass
class CircuitBreaker:
    failure_threshold: int = 3
    recovery_timeout: float = 5.0
    failures: int = 0
    opened_at: float | None = None
    retry_on: tuple[type[Exception], ...] = (Exception,)

    def call(self, operation: Callable[[], T]) -> T:
        now = time.monotonic()
        if self.opened_at is not None:
            if now - self.opened_at < self.recovery_timeout:
                raise RuntimeError("circuit open")
            self.failures = 0
            self.opened_at = None

        try:
            result = operation()
            self.failures = 0
            return result
        except self.retry_on:
            self.failures += 1
            if self.failures >= self.failure_threshold:
                self.opened_at = time.monotonic()
            raise
