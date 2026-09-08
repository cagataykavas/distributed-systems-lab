from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

from resilience.primitives.backpressure import BackpressureQueue
from resilience.primitives.bulkhead import Bulkhead

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class WorkerPoolSnapshot:
    accepted: int
    rejected: int
    processed: int
    failed: int
    peak_concurrency: int
    queue_depth: int


class AsyncWorkerPool(Generic[T]):
    """Bound ingress pressure and downstream concurrency with explicit lifecycle."""

    def __init__(
        self,
        *,
        handler: Callable[[T], Awaitable[None]],
        queue_size: int = 100,
        max_concurrency: int = 8,
        workers: int | None = None,
    ) -> None:
        if workers is not None and workers <= 0:
            raise ValueError("workers must be positive")
        self.queue = BackpressureQueue[T](maxsize=queue_size)
        self.bulkhead = Bulkhead(max_concurrency=max_concurrency)
        self._handler = handler
        self._worker_count = workers or max_concurrency
        self._tasks: list[asyncio.Task[None]] = []
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._tasks = [
            asyncio.create_task(
                self.queue.worker(self._handle),
                name=f"resilience-worker-{index}",
            )
            for index in range(self._worker_count)
        ]

    async def _handle(self, item: T) -> None:
        await self.bulkhead.run(lambda: self._handler(item))

    async def submit(self, item: T, *, wait_seconds: float = 0.05) -> bool:
        if not self._started:
            raise RuntimeError("worker pool must be started before submit")
        return await self.queue.submit(item, wait_seconds=wait_seconds)

    async def drain(self) -> None:
        if not self._started:
            return
        await self.queue.queue.join()

    async def close(self) -> None:
        if not self._started:
            return
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._started = False

    async def __aenter__(self) -> AsyncWorkerPool[T]:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    def snapshot(self) -> WorkerPoolSnapshot:
        return WorkerPoolSnapshot(
            accepted=self.queue.stats.accepted,
            rejected=self.queue.stats.rejected,
            processed=self.queue.stats.processed,
            failed=self.queue.stats.failed,
            peak_concurrency=self.bulkhead.stats.peak_active,
            queue_depth=self.queue.depth,
        )
