from __future__ import annotations

import asyncio
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from backpressure import BackpressureQueue
from bulkhead import Bulkhead
from dead_letter_queue import Message, RetryQueue
from idempotency import IdempotencyConflict, IdempotencyStore, fingerprint_json
from reliability import CircuitBreaker, CircuitOpenError, CircuitState, RetryPolicy, retry


class ManualClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def test_retry_retries_only_configured_failures() -> None:
    calls = 0
    delays: list[float] = []

    def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise TimeoutError("temporary")
        return "ok"

    result = retry(
        flaky,
        attempts=3,
        base_delay=0.01,
        retry_on=(TimeoutError,),
        sleeper=delays.append,
        rng=random.Random(7),
    )

    assert result == "ok"
    assert calls == 3
    assert len(delays) == 2
    assert 0.005 <= delays[0] <= 0.015
    assert 0.01 <= delays[1] <= 0.03

    with pytest.raises(ValueError, match="permanent"):
        retry(
            lambda: (_ for _ in ()).throw(ValueError("permanent")),
            attempts=5,
            base_delay=0,
            retry_on=(TimeoutError,),
        )


def test_retry_policy_caps_backoff_without_jitter() -> None:
    policy = RetryPolicy(
        attempts=5,
        base_delay=0.5,
        multiplier=2,
        max_delay=1.0,
        jitter_ratio=0,
    )
    rng = random.Random(1)
    assert [policy.delay_for(index, rng) for index in (1, 2, 3, 4)] == [
        0.5,
        1.0,
        1.0,
        1.0,
    ]


def test_circuit_breaker_opens_half_opens_and_recovers() -> None:
    clock = ManualClock()
    breaker = CircuitBreaker(
        failure_threshold=2,
        recovery_timeout=5,
        retry_on=(RuntimeError,),
        clock=clock,
    )

    def fail() -> None:
        raise RuntimeError("downstream")

    with pytest.raises(RuntimeError, match="downstream"):
        breaker.call(fail)
    with pytest.raises(RuntimeError, match="downstream"):
        breaker.call(fail)

    assert breaker.state is CircuitState.OPEN
    with pytest.raises(CircuitOpenError):
        breaker.call(lambda: None)

    clock.advance(5)
    assert breaker.call(lambda: "ok") == "ok"
    assert breaker.state is CircuitState.CLOSED
    assert breaker.failures == 0


def test_half_open_failure_reopens_circuit() -> None:
    clock = ManualClock()
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=1,
        retry_on=(RuntimeError,),
        clock=clock,
    )

    with pytest.raises(RuntimeError):
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("first")))
    assert breaker.state is CircuitState.OPEN

    clock.advance(1)
    with pytest.raises(RuntimeError, match="probe"):
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("probe")))
    assert breaker.state is CircuitState.OPEN


def test_idempotency_store_replays_and_rejects_payload_mutation() -> None:
    store: IdempotencyStore[int] = IdempotencyStore()
    calls = 0
    fingerprint = fingerprint_json({"amount": 42})

    def operation() -> int:
        nonlocal calls
        calls += 1
        return 100

    assert store.execute_once("payment-1", fingerprint, operation) == (100, True)
    assert store.execute_once("payment-1", fingerprint, operation) == (100, False)
    assert calls == 1

    with pytest.raises(IdempotencyConflict, match="different request fingerprint"):
        store.execute_once(
            "payment-1",
            fingerprint_json({"amount": 43}),
            operation,
        )


def test_concurrent_duplicates_execute_side_effect_once() -> None:
    store: IdempotencyStore[int] = IdempotencyStore()
    fingerprint = fingerprint_json({"order": 7})
    calls = 0
    calls_lock = threading.Lock()

    def operation() -> int:
        nonlocal calls
        with calls_lock:
            calls += 1
            value = calls
        time.sleep(0.01)
        return value

    def invoke() -> tuple[int, bool]:
        return store.execute_once("order-7", fingerprint, operation)

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: invoke(), range(8)))

    assert calls == 1
    assert {value for value, _ in results} == {1}
    assert sum(executed for _, executed in results) == 1


def test_retry_queue_moves_poison_message_to_dlq_with_evidence() -> None:
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
    assert queue.dead_letter[0].errors == [
        "RuntimeError: poison",
        "RuntimeError: poison",
    ]


def test_backpressure_rejects_when_queue_stays_full() -> None:
    async def scenario() -> None:
        queue = BackpressureQueue[int](maxsize=1)
        assert await queue.submit(1, timeout=0)
        assert not await queue.submit(2, timeout=0)
        assert queue.stats.accepted == 1
        assert queue.stats.rejected == 1
        assert queue.depth == 1

    asyncio.run(scenario())


def test_bulkhead_bounds_concurrency_and_tracks_peak() -> None:
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
        assert bulkhead.stats.peak_active == 2
        assert bulkhead.stats.completed == 8
        assert bulkhead.stats.active == 0

    asyncio.run(scenario())
