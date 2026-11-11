from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal, TypeVar

T = TypeVar("T")


@dataclass(slots=True)
class BulkheadStats:
    active: int = 0
    waiting: int = 0
    peak_active: int = 0
    peak_waiting: int = 0
    completed: int = 0
    failed: int = 0
    rejected: int = 0
    timed_out: int = 0


class BulkheadRejected(RuntimeError):
    """Raised before an operation starts when dependency admission is exhausted."""

    def __init__(self, reason: Literal["queue_full", "wait_timeout"]) -> None:
        self.reason = reason
        super().__init__(f"bulkhead rejected: {reason}")


class Bulkhead:
    """Bound concurrent access to a fragile async downstream dependency."""

    def __init__(self, max_concurrency: int, *, max_waiters: int | None = None) -> None:
        if (
            isinstance(max_concurrency, bool)
            or not isinstance(max_concurrency, int)
            or max_concurrency <= 0
        ):
            raise ValueError("max_concurrency must be a positive integer")
        if max_waiters is not None and (
            isinstance(max_waiters, bool) or not isinstance(max_waiters, int) or max_waiters < 0
        ):
            raise ValueError("max_waiters must be a non-negative integer or None")
        self.max_concurrency = max_concurrency
        self.max_waiters = max_waiters
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self.stats = BulkheadStats()

    def _reject(self, reason: Literal["queue_full", "wait_timeout"]) -> None:
        self.stats.rejected += 1
        if reason == "wait_timeout":
            self.stats.timed_out += 1
        raise BulkheadRejected(reason)

    async def run(
        self,
        operation: Callable[[], Awaitable[T]],
        *,
        wait_seconds: float | None = None,
    ) -> T:
        """Run after bounded admission without starting rejected operations.

        ``max_waiters=None`` and ``wait_seconds=None`` preserve the original
        unbounded-wait behavior. Set either limit at network dependency boundaries
        so saturation becomes explicit instead of accumulating hidden tail latency.
        """

        if wait_seconds is not None and (
            isinstance(wait_seconds, bool)
            or not isinstance(wait_seconds, (int, float))
            or not math.isfinite(wait_seconds)
            or wait_seconds < 0
        ):
            raise ValueError("wait_seconds must be a finite non-negative number or None")

        queued = False
        if self._semaphore.locked():
            if wait_seconds == 0:
                self._reject("wait_timeout")
            if self.max_waiters is not None and self.stats.waiting >= self.max_waiters:
                self._reject("queue_full")
            self.stats.waiting += 1
            self.stats.peak_waiting = max(self.stats.peak_waiting, self.stats.waiting)
            queued = True

        try:
            try:
                if wait_seconds is None:
                    await self._semaphore.acquire()
                else:
                    async with asyncio.timeout(wait_seconds):
                        await self._semaphore.acquire()
            except TimeoutError:
                self._reject("wait_timeout")
        finally:
            if queued:
                self.stats.waiting -= 1

        self.stats.active += 1
        self.stats.peak_active = max(self.stats.peak_active, self.stats.active)
        try:
            result = await operation()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.stats.failed += 1
            raise
        else:
            self.stats.completed += 1
            return result
        finally:
            self.stats.active -= 1
            self._semaphore.release()
