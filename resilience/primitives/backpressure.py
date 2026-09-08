from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(slots=True)
class QueueStats:
    accepted: int = 0
    rejected: int = 0
    processed: int = 0
    failed: int = 0


class BackpressureQueue(Generic[T]):
    """Bound producer pressure with a finite queue instead of unbounded growth."""

    def __init__(self, maxsize: int = 100) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        self.queue: asyncio.Queue[T] = asyncio.Queue(maxsize=maxsize)
        self.stats = QueueStats()

    @property
    def depth(self) -> int:
        return self.queue.qsize()

    async def submit(self, item: T, timeout: float = 0.05) -> bool:
        if timeout < 0:
            raise ValueError("timeout must be non-negative")
        try:
            if timeout == 0:
                self.queue.put_nowait(item)
            else:
                await asyncio.wait_for(self.queue.put(item), timeout=timeout)
        except (asyncio.QueueFull, TimeoutError):
            self.stats.rejected += 1
            return False
        self.stats.accepted += 1
        return True

    async def worker(self, handler: Callable[[T], Awaitable[None]]) -> None:
        while True:
            item = await self.queue.get()
            try:
                await handler(item)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.stats.failed += 1
            else:
                self.stats.processed += 1
            finally:
                self.queue.task_done()
