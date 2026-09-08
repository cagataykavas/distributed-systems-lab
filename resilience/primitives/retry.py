from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    attempts: int = 5
    base_delay: float = 0.05
    multiplier: float = 2.0
    max_delay: float | None = None
    jitter_ratio: float = 0.5

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("attempts must be at least one")
        if self.base_delay < 0:
            raise ValueError("base_delay must be non-negative")
        if self.multiplier < 1:
            raise ValueError("multiplier must be at least one")
        if self.max_delay is not None and self.max_delay < 0:
            raise ValueError("max_delay must be non-negative")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be between zero and one")

    def delay_for(self, retry_number: int, rng: random.Random) -> float:
        """Return the delay before retry number 1, 2, ... ."""
        if retry_number < 1:
            raise ValueError("retry_number must be at least one")
        delay = self.base_delay * (self.multiplier ** (retry_number - 1))
        if self.max_delay is not None:
            delay = min(delay, self.max_delay)
        if delay == 0 or self.jitter_ratio == 0:
            return delay
        lower = 1.0 - self.jitter_ratio
        upper = 1.0 + self.jitter_ratio
        return delay * rng.uniform(lower, upper)


def retry(
    operation: Callable[[], T],
    attempts: int = 5,
    base_delay: float = 0.05,
    retry_on: tuple[type[Exception], ...] = (Exception,),
    *,
    sleeper: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
) -> T:
    """Execute an operation with bounded exponential backoff and jitter.

    The compatibility arguments mirror the original lab function. ``sleeper`` and
    ``rng`` are injectable so tests and simulations do not need real wall-clock
    sleeps or global random state.
    """
    policy = RetryPolicy(attempts=attempts, base_delay=base_delay)
    local_rng = rng or random.Random()
    last_error: Exception | None = None

    for attempt_index in range(policy.attempts):
        try:
            return operation()
        except retry_on as exc:
            last_error = exc
            if attempt_index == policy.attempts - 1:
                break
            sleeper(policy.delay_for(attempt_index + 1, local_rng))

    if last_error is None:
        raise RuntimeError("retry loop exited without an operation result")
    raise last_error
