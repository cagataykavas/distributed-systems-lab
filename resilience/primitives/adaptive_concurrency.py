"""Cancellation-safe adaptive concurrency admission for network dependencies."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import TypeVar

T = TypeVar("T")


class AdmissionRejected(RuntimeError):
    """The dependency has no concurrency capacity for this request."""


class LeaseError(ValueError):
    """A lease is foreign, already completed, or has invalid clock evidence."""


class Outcome(StrEnum):
    SUCCESS = "success"
    FAILURE = "failure"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class ConcurrencyPolicy:
    initial_limit: int = 4
    minimum_limit: int = 1
    maximum_limit: int = 64
    sample_window: int = 20
    latency_budget_seconds: float = 0.2
    maximum_failure_rate: float = 0.05
    decrease_ratio: float = 0.5
    additive_step: int = 1
    minimum_utilization: float = 0.8

    def __post_init__(self) -> None:
        integer_fields = {
            "initial_limit": self.initial_limit,
            "minimum_limit": self.minimum_limit,
            "maximum_limit": self.maximum_limit,
            "sample_window": self.sample_window,
            "additive_step": self.additive_step,
        }
        if any(
            isinstance(value, bool) or not isinstance(value, int)
            for value in integer_fields.values()
        ):
            raise ValueError("integer policy fields must be integers")
        if not 1 <= self.minimum_limit <= self.initial_limit <= self.maximum_limit <= 100_000:
            raise ValueError("concurrency limits must satisfy 1 <= minimum <= initial <= maximum")
        if not 1 <= self.sample_window <= 100_000:
            raise ValueError("sample_window must be in [1, 100000]")
        if not 1 <= self.additive_step <= self.maximum_limit:
            raise ValueError("additive_step must be positive and bounded")
        floats = (
            self.latency_budget_seconds,
            self.maximum_failure_rate,
            self.decrease_ratio,
            self.minimum_utilization,
        )
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            for value in floats
        ):
            raise ValueError("floating policy fields must be finite numbers")
        if not 0 < self.latency_budget_seconds <= 3600:
            raise ValueError("latency_budget_seconds must be in (0, 3600]")
        if not 0 <= self.maximum_failure_rate <= 1:
            raise ValueError("maximum_failure_rate must be in [0, 1]")
        if not 0 < self.decrease_ratio < 1:
            raise ValueError("decrease_ratio must be in (0, 1)")
        if not 0 < self.minimum_utilization <= 1:
            raise ValueError("minimum_utilization must be in (0, 1]")


@dataclass(frozen=True)
class Lease:
    controller_id: str
    sequence: int
    started_at: float


@dataclass(frozen=True)
class Admission:
    admitted: bool
    lease: Lease | None
    limit: int
    in_flight: int
    reason: str


@dataclass(frozen=True)
class _Sample:
    outcome: Outcome
    latency_seconds: float


Clock = Callable[[], float]


class AdaptiveConcurrencyLimiter:
    """Non-blocking AIMD admission with bounded completion-window evidence.

    Requests above the current limit are rejected immediately; the limiter never
    creates a hidden waiter queue. Limit changes happen only after a complete sample
    window, keeping one policy generation stable within each decision window.
    """

    def __init__(self, policy: ConcurrencyPolicy, *, clock: Clock = time.monotonic) -> None:
        self._policy = policy
        self._clock = clock
        self._controller_id = hashlib.sha256(f"{id(self)}:{clock()}".encode()).hexdigest()[:16]
        self._lock = asyncio.Lock()
        self._limit = policy.initial_limit
        self._next_sequence = 0
        self._live: dict[int, float] = {}
        self._samples: list[_Sample] = []
        self._window_peak = 0
        self._generation = 0
        self._last_adjustment = "initial"
        self._last_p95_seconds: float | None = None
        self._last_failure_rate: float | None = None
        self._admitted_total = 0
        self._rejected_total = 0
        self._success_total = 0
        self._failure_total = 0
        self._cancelled_total = 0

    @property
    def policy_digest(self) -> str:
        payload = json.dumps(asdict(self._policy), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()

    async def try_acquire(self) -> Admission:
        async with self._lock:
            if len(self._live) >= self._limit:
                self._rejected_total += 1
                return Admission(False, None, self._limit, len(self._live), "limit_reached")
            started_at = self._read_clock()
            self._next_sequence += 1
            lease = Lease(self._controller_id, self._next_sequence, started_at)
            self._live[lease.sequence] = started_at
            self._admitted_total += 1
            self._window_peak = max(self._window_peak, len(self._live))
            return Admission(True, lease, self._limit, len(self._live), "admitted")

    async def complete(self, lease: Lease, outcome: Outcome) -> None:
        if not isinstance(outcome, Outcome):
            raise LeaseError("outcome must be an Outcome")
        async with self._lock:
            if lease.controller_id != self._controller_id:
                raise LeaseError("lease belongs to another controller")
            started_at = self._live.pop(lease.sequence, None)
            if started_at is None or started_at != lease.started_at:
                raise LeaseError("lease is unknown or already completed")
            completed_at = self._read_clock()
            latency = completed_at - started_at
            if latency < 0:
                raise LeaseError("monotonic clock moved backwards")
            if outcome is Outcome.CANCELLED:
                self._cancelled_total += 1
                return
            self._samples.append(_Sample(outcome, latency))
            if outcome is Outcome.SUCCESS:
                self._success_total += 1
            else:
                self._failure_total += 1
            if len(self._samples) == self._policy.sample_window:
                self._adjust_limit()

    async def run(self, operation: Callable[[], Awaitable[T]]) -> T:
        admission = await self.try_acquire()
        if not admission.admitted or admission.lease is None:
            raise AdmissionRejected("dependency concurrency limit reached")
        lease = admission.lease
        try:
            pending = operation()
            if not inspect.isawaitable(pending):
                raise TypeError("operation must return an awaitable")
            result = await pending
        except asyncio.CancelledError:
            await asyncio.shield(self.complete(lease, Outcome.CANCELLED))
            raise
        except Exception:
            await self.complete(lease, Outcome.FAILURE)
            raise
        else:
            await self.complete(lease, Outcome.SUCCESS)
            return result

    async def snapshot(self) -> dict[str, object]:
        async with self._lock:
            return {
                "limit": self._limit,
                "in_flight": len(self._live),
                "pending_samples": len(self._samples),
                "generation": self._generation,
                "last_adjustment": self._last_adjustment,
                "last_p95_seconds": self._last_p95_seconds,
                "last_failure_rate": self._last_failure_rate,
                "admitted_total": self._admitted_total,
                "rejected_total": self._rejected_total,
                "success_total": self._success_total,
                "failure_total": self._failure_total,
                "cancelled_total": self._cancelled_total,
                "policy_digest": self.policy_digest,
            }

    def _read_clock(self) -> float:
        value = self._clock()
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            raise LeaseError("clock must return a finite number")
        return float(value)

    def _adjust_limit(self) -> None:
        latencies = sorted(sample.latency_seconds for sample in self._samples)
        rank = max(0, math.ceil(0.95 * len(latencies)) - 1)
        p95 = latencies[rank]
        failures = sum(sample.outcome is Outcome.FAILURE for sample in self._samples)
        failure_rate = failures / len(self._samples)
        utilization = self._window_peak / self._limit
        if failure_rate > self._policy.maximum_failure_rate:
            updated = max(
                self._policy.minimum_limit,
                math.floor(self._limit * self._policy.decrease_ratio),
            )
            reason = "failure_rate"
        elif p95 > self._policy.latency_budget_seconds:
            updated = max(
                self._policy.minimum_limit,
                math.floor(self._limit * self._policy.decrease_ratio),
            )
            reason = "latency_budget"
        elif utilization >= self._policy.minimum_utilization:
            updated = min(self._policy.maximum_limit, self._limit + self._policy.additive_step)
            reason = "healthy_saturation" if updated != self._limit else "maximum_limit"
        else:
            updated = self._limit
            reason = "underutilized"
        self._limit = updated
        self._generation += 1
        self._last_adjustment = reason
        self._last_p95_seconds = round(p95, 9)
        self._last_failure_rate = round(failure_rate, 9)
        self._samples.clear()
        self._window_peak = len(self._live)
