from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeVar

T = TypeVar("T")
MAX_FAULT_WINDOWS = 128
MAX_SCHEDULE_MS = 86_400_000.0
MAX_INJECTED_LATENCY_MS = 60_000.0


class NetworkFaultKind(StrEnum):
    TIMEOUT = "timeout"
    RESET = "reset"
    LATENCY = "latency"


@dataclass(frozen=True, slots=True)
class FaultWindow:
    start_ms: float
    end_ms: float
    kind: NetworkFaultKind
    latency_ms: float = 0.0


@dataclass(frozen=True, slots=True)
class FaultSnapshot:
    calls: int
    passed: int
    delayed: int
    timed_out: int
    reset: int


class NetworkFaultInjector:
    """Inject deterministic network-like faults from a monotonic schedule."""

    def __init__(
        self,
        windows: tuple[FaultWindow, ...],
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.windows = _validate_windows(windows)
        self._clock = clock
        self._sleeper = sleeper
        self._started_at = self._clock()
        if not _is_finite_number(self._started_at) or self._started_at < 0:
            raise ValueError("monotonic clock returned an invalid initial value")
        self._lock = threading.Lock()
        self._calls = 0
        self._passed = 0
        self._delayed = 0
        self._timed_out = 0
        self._reset = 0

    def call(self, operation: Callable[[], T]) -> T:
        now = self._clock()
        if not _is_finite_number(now):
            raise RuntimeError("monotonic clock returned an invalid value")
        elapsed_ms = (now - self._started_at) * 1000
        if not math.isfinite(elapsed_ms) or elapsed_ms < 0:
            raise RuntimeError("monotonic clock returned an invalid value")
        active = next(
            (window for window in self.windows if window.start_ms <= elapsed_ms < window.end_ms),
            None,
        )

        with self._lock:
            self._calls += 1
            if active is None:
                self._passed += 1
            elif active.kind is NetworkFaultKind.LATENCY:
                self._delayed += 1
            elif active.kind is NetworkFaultKind.TIMEOUT:
                self._timed_out += 1
            else:
                self._reset += 1

        if active is None:
            return operation()
        if active.kind is NetworkFaultKind.LATENCY:
            self._sleeper(active.latency_ms / 1000)
            return operation()
        if active.kind is NetworkFaultKind.TIMEOUT:
            raise TimeoutError("injected network timeout")
        raise ConnectionResetError("injected connection reset")

    def snapshot(self) -> FaultSnapshot:
        with self._lock:
            return FaultSnapshot(
                calls=self._calls,
                passed=self._passed,
                delayed=self._delayed,
                timed_out=self._timed_out,
                reset=self._reset,
            )


def _validate_windows(windows: tuple[FaultWindow, ...]) -> tuple[FaultWindow, ...]:
    if len(windows) > MAX_FAULT_WINDOWS:
        raise ValueError("fault window budget exceeded")
    previous_end = 0.0
    for index, window in enumerate(windows):
        if not isinstance(window, FaultWindow):
            raise TypeError("windows must contain FaultWindow values")
        if not isinstance(window.kind, NetworkFaultKind):
            raise ValueError("fault kind must be a NetworkFaultKind")
        values = (window.start_ms, window.end_ms, window.latency_ms)
        if any(not _is_finite_number(value) for value in values):
            raise ValueError("fault window values must be finite numbers")
        if not 0 <= window.start_ms < window.end_ms <= MAX_SCHEDULE_MS:
            raise ValueError("fault window bounds are invalid")
        if index and window.start_ms < previous_end:
            raise ValueError("fault windows must be ordered and non-overlapping")
        if window.kind is NetworkFaultKind.LATENCY:
            if not 0 < window.latency_ms <= MAX_INJECTED_LATENCY_MS:
                raise ValueError("latency fault delay is invalid")
        elif window.latency_ms != 0:
            raise ValueError("non-latency faults cannot define latency_ms")
        previous_end = window.end_ms
    return windows


def _is_finite_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and math.isfinite(value)
