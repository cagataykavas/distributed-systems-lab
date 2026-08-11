from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Callable, TypeVar

T = TypeVar("T")


def retry(operation: Callable[[], T], attempts: int = 5, base_delay: float = 0.05) -> T:
    last_error = None
    for attempt in range(attempts):
        try:
            return operation()
        except Exception as exc:
            last_error = exc
            if attempt == attempts - 1:
                break
            delay = base_delay * (2 ** attempt)
            delay *= random.uniform(0.5, 1.5)
            time.sleep(delay)
    raise last_error


@dataclass
class CircuitBreaker:
    failure_threshold: int = 3
    recovery_timeout: float = 5.0
    failures: int = 0
    opened_at: float | None = None

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
        except Exception:
            self.failures += 1
            if self.failures >= self.failure_threshold:
                self.opened_at = time.monotonic()
            raise
