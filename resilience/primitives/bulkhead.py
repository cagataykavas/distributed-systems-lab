from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


@dataclass(slots=True)
class BulkheadStats:
    active: int = 0
    peak_active: int = 0
    completed: int = 0
    failed: int = 0


class Bulkhead:
    """Bound concurrent access to a fragile async downstream dependency."""

    def __init__(self, max_concurrency: int) -> None:
        if max_concurrency <= 0:
            raise ValueError("max_concurrency must be positive")
        self.max_concurrency = max_concurrency
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self.stats = BulkheadStats()

    async def run(self, operation: Callable[[], Awaitable[T]]) -> T:
        async with self._semaphore:
            self.stats.active += 1
            self.stats.peak_active = max(
                self.stats.peak_active,
                self.stats.active,
            )
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
