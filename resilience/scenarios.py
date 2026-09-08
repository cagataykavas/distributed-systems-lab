from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Callable

from resilience.executor import ResilientExecutor
from resilience.primitives.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
)
from resilience.primitives.dead_letter import Message, RetryQueue
from resilience.primitives.idempotency import IdempotencyConflict
from resilience.primitives.retry import RetryPolicy
from resilience.reporting import ScenarioEvent, ScenarioReport, compare_reports
from resilience.worker_pool import AsyncWorkerPool


class ManualClock:
    def __init__(self, value: float = 0.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("seconds must be non-negative")
        self.value += seconds


class FaultyDependency:
    """Seeded downstream dependency with configurable failure probability."""

    def __init__(
        self,
        *,
        failure_probability: float,
        seed: int = 42,
        min_latency_ms: float = 0.0,
        max_latency_ms: float = 0.0,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if not 0 <= failure_probability <= 1:
            raise ValueError("failure_probability must be between 0 and 1")
        if min_latency_ms < 0 or max_latency_ms < min_latency_ms:
            raise ValueError("latency bounds are invalid")
        self.failure_probability = failure_probability
        self.rng = random.Random(seed)
        self.min_latency_ms = min_latency_ms
        self.max_latency_ms = max_latency_ms
        self._sleeper = sleeper
        self.calls = 0

    def call(self, payload: int) -> int:
        self.calls += 1
        latency_ms = self.rng.uniform(self.min_latency_ms, self.max_latency_ms)
        if latency_ms:
            self._sleeper(latency_ms / 1000)
        if self.rng.random() < self.failure_probability:
            raise RuntimeError("synthetic downstream failure")
        return payload * 2


def circuit_breaker_scenario(
    *,
    requests: int = 20,
    failure_probability: float = 0.65,
    failure_threshold: int = 3,
    seed: int = 42,
) -> ScenarioReport:
    if requests < 1:
        raise ValueError("requests must be positive")

    clock = ManualClock()
    dependency = FaultyDependency(
        failure_probability=failure_probability,
        seed=seed,
        sleeper=lambda _: None,
    )
    breaker = CircuitBreaker(
        failure_threshold=failure_threshold,
        recovery_timeout=1.0,
        retry_on=(RuntimeError,),
        clock=clock,
    )
    report = ScenarioReport("circuit_breaker", requests=requests)
    open_rejection_seen = False

    for step in range(1, requests + 1):
        started = time.perf_counter()
        state_before = breaker.state
        try:
            breaker.call(lambda step=step: dependency.call(step))
        except CircuitOpenError as exc:
            report.rejected += 1
            outcome = "rejected"
            detail = f"{state_before.value}: {exc}"
            if not open_rejection_seen:
                open_rejection_seen = True
                clock.advance(1.0)
        except RuntimeError as exc:
            report.failures += 1
            outcome = "failure"
            detail = f"{state_before.value}: {exc}"
        else:
            report.successes += 1
            outcome = "success"
            detail = f"{state_before.value} -> {breaker.state.value}"

        report.events.append(
            ScenarioEvent(
                step=step,
                operation="dependency_call",
                outcome=outcome,
                detail=detail,
                latency_ms=(time.perf_counter() - started) * 1000,
            )
        )

    return report


def retry_dlq_scenario(
    *,
    messages: int = 12,
    max_attempts: int = 3,
    poison_every: int = 4,
) -> ScenarioReport:
    if messages < 1:
        raise ValueError("messages must be positive")
    if poison_every < 1:
        raise ValueError("poison_every must be positive")

    queue: RetryQueue[dict[str, object]] = RetryQueue(
        max_attempts=max_attempts,
        retry_on=(RuntimeError,),
    )
    report = ScenarioReport("retry_dlq", requests=messages)
    poison_ids = {
        f"evt-{index}"
        for index in range(1, messages + 1)
        if index % poison_every == 0
    }

    for index in range(1, messages + 1):
        message_id = f"evt-{index}"
        queue.publish(
            Message(
                message_id,
                {"message_id": message_id, "value": index},
            )
        )

    def handler(payload: dict[str, object]) -> None:
        if payload["message_id"] in poison_ids:
            raise RuntimeError("synthetic poison message")

    step = 0
    while queue.ready:
        step += 1
        before_dlq = len(queue.dead_letter)
        status = queue.process_one(handler)
        if status == "processed":
            report.successes += 1
        elif status == "retry_scheduled":
            report.retries += 1
        elif status == "dead_lettered":
            report.dead_lettered += 1
            report.failures += 1

        dead_lettered_now = len(queue.dead_letter) > before_dlq
        detail = (
            queue.dead_letter[-1].errors[-1]
            if dead_lettered_now
            else "consumer attempt completed"
        )
        report.events.append(
            ScenarioEvent(
                step=step,
                operation="consume_message",
                outcome=status,
                detail=detail,
                latency_ms=0.0,
            )
        )

    return report


def resilient_executor_scenario() -> ScenarioReport:
    """Exercise retry success, replay and conflicting idempotency payloads."""
    breaker = CircuitBreaker(
        failure_threshold=4,
        recovery_timeout=1.0,
        retry_on=(RuntimeError,),
    )
    executor: ResilientExecutor[int] = ResilientExecutor(
        breaker=breaker,
        retry_policy=RetryPolicy(
            attempts=3,
            base_delay=0.01,
            jitter_ratio=0,
        ),
        retry_on=(RuntimeError,),
        sleeper=lambda _: None,
    )
    report = ScenarioReport("resilient_executor", requests=3)
    downstream_calls = 0

    def flaky_operation() -> int:
        nonlocal downstream_calls
        downstream_calls += 1
        if downstream_calls < 3:
            raise RuntimeError("temporary timeout")
        return 84

    started = time.perf_counter()
    first = executor.execute(
        key="payment-42",
        payload={"amount": 42},
        operation=flaky_operation,
    )
    report.successes += 1
    report.retries += max(first.attempts - 1, 0)
    report.events.append(
        ScenarioEvent(
            step=1,
            operation="execute_payment",
            outcome="success",
            detail=f"completed after {first.attempts} attempts",
            latency_ms=(time.perf_counter() - started) * 1000,
        )
    )

    replay = executor.execute(
        key="payment-42",
        payload={"amount": 42},
        operation=lambda: 999,
    )
    if replay.executed:
        raise AssertionError("idempotent replay unexpectedly executed downstream")
    report.successes += 1
    report.duplicates += 1
    report.events.append(
        ScenarioEvent(
            step=2,
            operation="execute_payment",
            outcome="duplicate",
            detail="replayed cached result without downstream execution",
            latency_ms=0.0,
        )
    )

    try:
        executor.execute(
            key="payment-42",
            payload={"amount": 43},
            operation=lambda: 86,
        )
    except IdempotencyConflict as exc:
        report.failures += 1
        report.events.append(
            ScenarioEvent(
                step=3,
                operation="execute_payment",
                outcome="conflict",
                detail=str(exc),
                latency_ms=0.0,
            )
        )
    else:
        raise AssertionError("conflicting idempotency payload was accepted")

    return report


async def async_worker_pool_scenario(
    *,
    items: int = 20,
    queue_size: int = 3,
    max_concurrency: int = 2,
) -> ScenarioReport:
    if items < 1:
        raise ValueError("items must be positive")

    processed: list[int] = []

    async def handler(value: int) -> None:
        await asyncio.sleep(0.002)
        processed.append(value)

    report = ScenarioReport("async_worker_pool", requests=items)
    async with AsyncWorkerPool[int](
        handler=handler,
        queue_size=queue_size,
        max_concurrency=max_concurrency,
        workers=max_concurrency,
    ) as pool:
        accepted = await asyncio.gather(
            *(pool.submit(index, wait_seconds=0) for index in range(items))
        )
        await pool.drain()
        snapshot = pool.snapshot()

    report.successes = snapshot.processed
    report.rejected = snapshot.rejected
    for step, was_accepted in enumerate(accepted, start=1):
        report.events.append(
            ScenarioEvent(
                step=step,
                operation="submit_work",
                outcome="accepted" if was_accepted else "rejected",
                detail=(
                    f"peak_concurrency={snapshot.peak_concurrency}"
                    if was_accepted
                    else "bounded queue applied backpressure"
                ),
                latency_ms=0.0,
            )
        )

    if len(processed) != snapshot.processed:
        raise AssertionError("worker accounting mismatch")
    return report


def worker_pool_scenario(**kwargs: int) -> ScenarioReport:
    return asyncio.run(async_worker_pool_scenario(**kwargs))


def full_suite() -> dict[str, object]:
    reports = [
        circuit_breaker_scenario(),
        retry_dlq_scenario(),
        resilient_executor_scenario(),
        worker_pool_scenario(),
    ]
    return compare_reports(reports)


__all__ = [
    "CircuitState",
    "FaultyDependency",
    "ManualClock",
    "async_worker_pool_scenario",
    "circuit_breaker_scenario",
    "compare_reports",
    "full_suite",
    "resilient_executor_scenario",
    "retry_dlq_scenario",
    "worker_pool_scenario",
]
