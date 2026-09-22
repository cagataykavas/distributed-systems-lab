from __future__ import annotations

import math
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any


def _finite_positive(name: str, value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0:
        raise ValueError(f"{name} must be finite and positive")
    return normalized


def _finite_non_negative(name: str, value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{name} must be finite and non-negative")
    return normalized


@dataclass(frozen=True, slots=True)
class DeadlineSnapshot:
    started_at: float
    expires_at: float
    observed_at: float
    elapsed_seconds: float
    remaining_seconds: float

    def as_dict(self) -> dict[str, float]:
        return asdict(self)


class DeadlineExceeded(TimeoutError):
    """A new downstream attempt cannot safely fit within the request budget."""

    def __init__(
        self,
        *,
        phase: str,
        attempts: int,
        snapshot: DeadlineSnapshot,
        required_delay_seconds: float = 0.0,
    ) -> None:
        self.phase = phase
        self.attempts = attempts
        self.snapshot = snapshot
        self.required_delay_seconds = required_delay_seconds
        super().__init__(f"request deadline exhausted during {phase} after {attempts} attempt(s)")

    def as_dict(self) -> dict[str, Any]:
        return {
            "reason": "request_deadline_exceeded",
            "phase": self.phase,
            "attempts": self.attempts,
            "required_delay_seconds": self.required_delay_seconds,
            "deadline": self.snapshot.as_dict(),
        }


class RequestDeadline:
    """Monotonic request budget shared across retry boundaries.

    The deadline prevents new attempts and backoff sleeps after the budget is no
    longer useful. It cannot preempt a synchronous operation that is already in
    flight; callers must propagate the remaining budget into network-client
    timeouts for that stronger guarantee.
    """

    def __init__(
        self,
        *,
        started_at: float,
        expires_at: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.started_at = _finite_non_negative("started_at", started_at)
        self.expires_at = _finite_positive("expires_at", expires_at)
        if self.expires_at <= self.started_at:
            raise ValueError("expires_at must be greater than started_at")
        self._clock = clock

    @classmethod
    def after(
        cls,
        timeout_seconds: float,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> RequestDeadline:
        timeout = _finite_positive("timeout_seconds", timeout_seconds)
        started_at = _finite_non_negative("clock value", clock())
        return cls(
            started_at=started_at,
            expires_at=started_at + timeout,
            clock=clock,
        )

    def snapshot(self) -> DeadlineSnapshot:
        observed_at = _finite_non_negative("clock value", self._clock())
        if observed_at < self.started_at:
            raise RuntimeError("monotonic clock moved before the deadline start")
        return DeadlineSnapshot(
            started_at=self.started_at,
            expires_at=self.expires_at,
            observed_at=observed_at,
            elapsed_seconds=observed_at - self.started_at,
            remaining_seconds=max(self.expires_at - observed_at, 0.0),
        )

    def require_attempt(self, *, attempts: int) -> DeadlineSnapshot:
        snapshot = self.snapshot()
        if snapshot.remaining_seconds <= 0:
            raise DeadlineExceeded(
                phase="before_attempt",
                attempts=attempts,
                snapshot=snapshot,
            )
        return snapshot

    def require_retry_delay(
        self,
        delay_seconds: float,
        *,
        attempts: int,
    ) -> DeadlineSnapshot:
        delay = _finite_non_negative("delay_seconds", delay_seconds)
        snapshot = self.snapshot()
        if snapshot.remaining_seconds <= 0 or delay >= snapshot.remaining_seconds:
            raise DeadlineExceeded(
                phase="before_retry_sleep",
                attempts=attempts,
                snapshot=snapshot,
                required_delay_seconds=delay,
            )
        return snapshot
