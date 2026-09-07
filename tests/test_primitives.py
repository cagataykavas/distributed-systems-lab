from __future__ import annotations

import asyncio

import pytest

from backpressure import BackpressureQueue
from bulkhead import Bulkhead
from dead_letter_queue import Message, RetryQueue
from idempotency import DedupStore
from reliability import CircuitBreaker, retry


def test_retry_retries_only_configured_failures() -> None:
    calls = 0

    def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise TimeoutError("temporary")
        return "ok"

    assert retry(flaky, attempts=3, base_delay=0, retry_on=(TimeoutError,)) == "ok"
    assert calls == 3

    with pytest.raises(ValueError, match="permanent"):
        retry(
            lambda: (_ for _ in ()).throw(ValueError("permanent")),
            attempts=5,
            base_delay=0,
            retry_on=(TimeoutError,),
        )


def test_circuit_breaker_opens_and_rejects() -> None:
    breaker = CircuitBreaker(
        failure_threshold=2,
        recovery_timeout=3600,
        retry_on=(RuntimeError,),
    )

    def fail() -> None:
        raise RuntimeError("downstream")

    with pytest.raises(RuntimeError, match="downstream"):
        breaker.call(fail)
    with pytest.raises(RuntimeError, match="downstream"):
        breaker.call(fail)
    with pytest.raises(RuntimeError, match="circuit open"):
        breaker.call(lambda: None)


def test_dedup_store_executes_side_effect_once() -> None:
    store = DedupStore[int]()
    executions = 0

    def operation() -> int:
        nonlocal executions
        executions += 1
        return executions

    assert store.execute_once("payment-1", operation) == (1, True)
    assert store.execute_once("payment-1", operation) == (1, False)
    assert executions == 1


def test_retry_queue_moves_poison_message_to_dlq() -> None:
    queue: RetryQueue[str] = RetryQueue(
        max_attempts=2,
        retry_on=(RuntimeError,),
    )
    queue.publish(Message("evt-1", "poison"))

    def fail(_: str) -> None:
        raise RuntimeError("poison")

    assert queue.process_one(fail) == "retry_scheduled"
    assert queue.process_one(fail) == "dead_lettered"
    assert queue.ready == []
    assert len(queue.dead_letter) == 1
    assert queue.dead_letter[0].attempts == 2
    assert queue.dead_letter[0].errors == ["poison", "poison"]


def test_backpressure_rejects_when_queue_stays_full() -> None:
    async def scenario() -> None:
        queue = BackpressureQueue[int](maxsize=1)
        assert await queue.submit(1, timeout=0.01)
        assert not await queue.submit(2, timeout=0.001)
        assert queue.stats.accepted == 1
        assert queue.stats.rejected == 1

    asyncio.run(scenario())


def test_bulkhead_bounds_concurrency() -> None:
    async def scenario() -> None:
        bulkhead = Bulkhead(max_concurrency=2)
        active = 0
        peak = 0
        lock = asyncio.Lock()

        async def operation(value: int) -> int:
            nonlocal active, peak
            async with lock:
                active += 1
                peak = max(peak, active)
            await asyncio.sleep(0.005)
            async with lock:
                active -= 1
            return value * 2

        results = await asyncio.gather(
            *(bulkhead.run(lambda value=value: operation(value)) for value in range(8))
        )
        assert results == [value * 2 for value in range(8)]
        assert peak == 2

    asyncio.run(scenario())
