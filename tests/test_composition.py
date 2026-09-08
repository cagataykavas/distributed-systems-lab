from __future__ import annotations

import asyncio

import pytest

from resilience.executor import ResilientExecutor
from resilience.primitives.circuit_breaker import CircuitBreaker, CircuitOpenError
from resilience.primitives.idempotency import IdempotencyConflict, IdempotencyStore
from resilience.primitives.retry import RetryPolicy
from resilience.worker_pool import AsyncWorkerPool


def test_resilient_executor_retries_then_replays_without_downstream_call() -> None:
    breaker = CircuitBreaker(
        failure_threshold=4,
        recovery_timeout=30,
        retry_on=(TimeoutError,),
    )
    delays: list[float] = []
    store: IdempotencyStore[int] = IdempotencyStore()
    executor: ResilientExecutor[int] = ResilientExecutor(
        breaker=breaker,
        retry_policy=RetryPolicy(
            attempts=3,
            base_delay=0.01,
            jitter_ratio=0,
        ),
        store=store,
        retry_on=(TimeoutError,),
        sleeper=delays.append,
    )
    calls = 0

    def flaky() -> int:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise TimeoutError("temporary")
        return 42

    first = executor.execute(
        key="request-1",
        payload={"value": 21},
        operation=flaky,
    )
    assert first.value == 42
    assert first.executed is True
    assert first.attempts == 3
    assert delays == [0.01, 0.02]
    assert calls == 3
    assert executor.store is store

    replay = executor.execute(
        key="request-1",
        payload={"value": 21},
        operation=lambda: (_ for _ in ()).throw(AssertionError("must not execute")),
    )
    assert replay.value == 42
    assert replay.executed is False
    assert replay.attempts == 0
    assert calls == 3


def test_resilient_executor_rejects_changed_payload_for_same_key() -> None:
    executor: ResilientExecutor[int] = ResilientExecutor(
        breaker=CircuitBreaker(failure_threshold=3),
        retry_policy=RetryPolicy(attempts=1),
        sleeper=lambda _: None,
    )
    executor.execute(key="key-1", payload={"x": 1}, operation=lambda: 10)

    with pytest.raises(IdempotencyConflict):
        executor.execute(key="key-1", payload={"x": 2}, operation=lambda: 20)


def test_resilient_executor_does_not_retry_open_circuit() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=60,
        retry_on=(TimeoutError,),
    )
    with pytest.raises(TimeoutError):
        breaker.call(lambda: (_ for _ in ()).throw(TimeoutError("down")))

    delays: list[float] = []
    executor: ResilientExecutor[int] = ResilientExecutor(
        breaker=breaker,
        retry_policy=RetryPolicy(attempts=5, base_delay=1, jitter_ratio=0),
        retry_on=(Exception,),
        sleeper=delays.append,
    )

    with pytest.raises(CircuitOpenError):
        executor.execute(key="open", payload={"x": 1}, operation=lambda: 1)
    assert delays == []


def test_worker_pool_requires_explicit_start() -> None:
    async def scenario() -> None:
        async def handler(_: int) -> None:
            return None

        pool = AsyncWorkerPool[int](handler=handler, queue_size=2, max_concurrency=1)
        with pytest.raises(RuntimeError, match="started"):
            await pool.submit(1)

    asyncio.run(scenario())


def test_worker_pool_drains_and_bounds_concurrency() -> None:
    async def scenario() -> None:
        active = 0
        peak = 0
        processed: list[int] = []
        lock = asyncio.Lock()

        async def handler(value: int) -> None:
            nonlocal active, peak
            async with lock:
                active += 1
                peak = max(peak, active)
            await asyncio.sleep(0.002)
            processed.append(value)
            async with lock:
                active -= 1

        async with AsyncWorkerPool[int](
            handler=handler,
            queue_size=20,
            max_concurrency=2,
            workers=6,
        ) as pool:
            accepted = await asyncio.gather(
                *(pool.submit(value, wait_seconds=0.05) for value in range(12))
            )
            assert all(accepted)
            await pool.drain()
            snapshot = pool.snapshot()

        assert sorted(processed) == list(range(12))
        assert snapshot.processed == 12
        assert snapshot.failed == 0
        assert snapshot.rejected == 0
        assert peak <= 2
        assert snapshot.peak_concurrency <= 2
        assert snapshot.queue_depth == 0

    asyncio.run(scenario())
