from __future__ import annotations

import asyncio

import pytest

from resilience.primitives.bulkhead import Bulkhead, BulkheadRejected


async def wait_for_waiters(bulkhead: Bulkhead, expected: int) -> None:
    for _ in range(100):
        if bulkhead.stats.waiting == expected:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"expected {expected} waiters, saw {bulkhead.stats.waiting}")


def test_waiter_capacity_rejects_without_starting_operation() -> None:
    async def scenario() -> None:
        bulkhead = Bulkhead(max_concurrency=1, max_waiters=1)
        release = asyncio.Event()
        started: list[str] = []

        async def hold(name: str) -> str:
            started.append(name)
            await release.wait()
            return name

        active = asyncio.create_task(bulkhead.run(lambda: hold("active")))
        await asyncio.sleep(0)
        waiter = asyncio.create_task(bulkhead.run(lambda: hold("waiter")))
        await wait_for_waiters(bulkhead, 1)

        with pytest.raises(BulkheadRejected) as error:
            await bulkhead.run(lambda: hold("rejected"))

        assert error.value.reason == "queue_full"
        assert started == ["active"]
        assert bulkhead.stats.active == 1
        assert bulkhead.stats.waiting == 1
        assert bulkhead.stats.rejected == 1
        assert bulkhead.stats.peak_waiting == 1

        release.set()
        assert await active == "active"
        assert await waiter == "waiter"
        assert started == ["active", "waiter"]
        assert bulkhead.stats.completed == 2
        assert bulkhead.stats.active == 0
        assert bulkhead.stats.waiting == 0

    asyncio.run(scenario())


def test_zero_wait_budget_fails_fast_with_distinct_evidence() -> None:
    async def scenario() -> None:
        bulkhead = Bulkhead(max_concurrency=1, max_waiters=5)
        release = asyncio.Event()
        operation_started = False

        async def hold() -> None:
            await release.wait()

        async def must_not_start() -> None:
            nonlocal operation_started
            operation_started = True

        active = asyncio.create_task(bulkhead.run(hold))
        await asyncio.sleep(0)
        with pytest.raises(BulkheadRejected) as error:
            await bulkhead.run(must_not_start, wait_seconds=0)

        assert error.value.reason == "wait_timeout"
        assert operation_started is False
        assert bulkhead.stats.rejected == 1
        assert bulkhead.stats.timed_out == 1
        assert bulkhead.stats.waiting == 0
        release.set()
        await active

    asyncio.run(scenario())


def test_zero_wait_budget_still_admits_an_available_slot() -> None:
    async def scenario() -> None:
        bulkhead = Bulkhead(max_concurrency=1, max_waiters=0)

        result = await bulkhead.run(
            lambda: asyncio.sleep(0, result="ok"),
            wait_seconds=0,
        )

        assert result == "ok"
        assert bulkhead.stats.completed == 1
        assert bulkhead.stats.rejected == 0

    asyncio.run(scenario())


def test_cancelled_waiter_releases_waiter_capacity() -> None:
    async def scenario() -> None:
        bulkhead = Bulkhead(max_concurrency=1, max_waiters=1)
        release = asyncio.Event()

        async def hold() -> None:
            await release.wait()

        active = asyncio.create_task(bulkhead.run(hold))
        await asyncio.sleep(0)
        waiter = asyncio.create_task(bulkhead.run(hold))
        await wait_for_waiters(bulkhead, 1)

        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert bulkhead.stats.waiting == 0

        replacement = asyncio.create_task(bulkhead.run(hold))
        await wait_for_waiters(bulkhead, 1)
        release.set()
        await active
        await replacement

        assert bulkhead.stats.completed == 2
        assert bulkhead.stats.rejected == 0
        assert bulkhead.stats.active == 0
        assert bulkhead.stats.waiting == 0

    asyncio.run(scenario())


def test_positive_wait_timeout_releases_capacity_and_permit() -> None:
    async def scenario() -> None:
        bulkhead = Bulkhead(max_concurrency=1, max_waiters=1)
        release = asyncio.Event()

        async def hold() -> None:
            await release.wait()

        active = asyncio.create_task(bulkhead.run(hold))
        await asyncio.sleep(0)
        with pytest.raises(BulkheadRejected) as error:
            await bulkhead.run(hold, wait_seconds=0.001)
        assert error.value.reason == "wait_timeout"
        assert bulkhead.stats.waiting == 0
        assert bulkhead.stats.timed_out == 1

        release.set()
        await active
        assert await bulkhead.run(lambda: asyncio.sleep(0, result="ok")) == "ok"
        assert bulkhead.stats.completed == 2

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_concurrency": 0},
        {"max_concurrency": True},
        {"max_concurrency": 1.5},
        {"max_concurrency": "1"},
        {"max_concurrency": 1, "max_waiters": -1},
        {"max_concurrency": 1, "max_waiters": True},
        {"max_concurrency": 1, "max_waiters": 1.5},
    ],
)
def test_invalid_bulkhead_limits_are_rejected(kwargs) -> None:
    with pytest.raises(ValueError):
        Bulkhead(**kwargs)


@pytest.mark.parametrize("wait_seconds", [-1, float("nan"), float("inf"), True, "1"])
def test_invalid_wait_budgets_are_rejected(wait_seconds) -> None:
    async def scenario() -> None:
        bulkhead = Bulkhead(max_concurrency=1)
        with pytest.raises(ValueError):
            await bulkhead.run(lambda: asyncio.sleep(0), wait_seconds=wait_seconds)

    asyncio.run(scenario())
