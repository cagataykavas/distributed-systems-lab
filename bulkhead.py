from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


class Bulkhead:
    """Bound concurrent access to a fragile downstream dependency."""

    def __init__(self, max_concurrency: int) -> None:
        if max_concurrency <= 0:
            raise ValueError("max_concurrency must be positive")
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def run(self, operation: Callable[[], Awaitable[T]]) -> T:
        async with self._semaphore:
            return await operation()


async def demo() -> None:
    bulkhead = Bulkhead(max_concurrency=3)

    async def call(i: int) -> int:
        async def operation() -> int:
            await asyncio.sleep(0.05)
            return i * 2

        return await bulkhead.run(operation)

    print(await asyncio.gather(*(call(i) for i in range(10))))


if __name__ == "__main__":
    asyncio.run(demo())
