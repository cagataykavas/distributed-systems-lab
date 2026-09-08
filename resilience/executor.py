from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

from resilience.primitives.circuit_breaker import CircuitBreaker
from resilience.primitives.idempotency import IdempotencyStore, fingerprint_json
from resilience.primitives.retry import RetryPolicy

T = TypeVar("T")
R = TypeVar("R")


@dataclass(frozen=True, slots=True)
class ExecutionResult(Generic[R]):
    value: R
    executed: bool
    attempts: int


class ResilientExecutor(Generic[R]):
    """Compose idempotency, retries and a circuit breaker around a downstream call.

    Ordering matters: idempotency is checked before any downstream attempt so a
    completed request can be replayed without consuming breaker capacity or retry
    budget. A new request is then executed through the breaker, with retries bounded
    by a deterministic policy.
    """

    def __init__(
        self,
        *,
        breaker: CircuitBreaker,
        retry_policy: RetryPolicy,
        store: IdempotencyStore[R] | None = None,
        retry_on: tuple[type[Exception], ...] = (Exception,),
        sleeper: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self.breaker = breaker
        self.retry_policy = retry_policy
        self.store = store or IdempotencyStore()
        self.retry_on = retry_on
        self._sleeper = sleeper
        self._rng = rng or random.Random()

    def _execute_with_resilience(self, operation: Callable[[], R]) -> tuple[R, int]:
        last_error: Exception | None = None
        attempts = 0

        for attempt_index in range(self.retry_policy.attempts):
            attempts += 1
            try:
                return self.breaker.call(operation), attempts
            except self.retry_on as exc:
                last_error = exc
                if attempt_index == self.retry_policy.attempts - 1:
                    break
                delay = self.retry_policy.delay_for(attempt_index + 1, self._rng)
                self._sleeper(delay)

        if last_error is None:
            raise RuntimeError("executor exited without a result or captured failure")
        raise last_error

    def execute(
        self,
        *,
        key: str,
        payload: T,
        operation: Callable[[], R],
    ) -> ExecutionResult[R]:
        fingerprint = fingerprint_json(payload)
        attempts = 0

        def guarded() -> R:
            nonlocal attempts
            value, attempts = self._execute_with_resilience(operation)
            return value

        value, executed = self.store.execute_once(key, fingerprint, guarded)
        return ExecutionResult(
            value=value,
            executed=executed,
            attempts=attempts if executed else 0,
        )
