from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass
class QueueStats:
    accepted: int = 0
    rejected: int = 0
    processed: int = 0


class BackpressureQueue(Generic[T]):
    """Bound producer pressure with a finite queue instead of unbounded memory growth."""

    def __init__(self, maxsize: int = 100) -> None:
        self.queue: asyncio.Queue[T] = asyncio.Queue(maxsize=maxsize)
        self.stats = QueueStats()

    async def submit(self, item: T, timeout: float = 0.05) -> bool:
        try:
            await asyncio.wait_for(self.queue.put(item), timeout=timeout)
            self.stats.accepted += 1
            return True
        except TimeoutError:
            self.stats.rejected += 1
            return False

    async def worker(self, handler: Callable[[T], Awaitable[None]]) -> None:
        while True:
            item = await self.queue.get()
            try:
                await handler(item)
                self.stats.processed += 1
            finally:
                self.queue.task_done()


async def demo() -> None:
    pressure = BackpressureQueue[int](maxsize=3)

    async def slow_handler(value: int) -> None:
        await asyncio.sleep(0.05)
        print("processed", value)

    worker = asyncio.create_task(pressure.worker(slow_handler))
    results = await asyncio.gather(
        *(pressure.submit(index, timeout=0.01) for index in range(20))
    )
    await pressure.queue.join()
    worker.cancel()
    print({"accepted": sum(results), "stats": pressure.stats})


if __name__ == "__main__":
    asyncio.run(demo())
