from __future__ import annotations

import asyncio
import math

import pytest

from resilience.primitives.adaptive_concurrency import (
    AdaptiveConcurrencyLimiter,
    AdmissionRejected,
    ConcurrencyPolicy,
    Lease,
    LeaseError,
    Outcome,
)


class Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def policy(**overrides: object) -> ConcurrencyPolicy:
    values: dict[str, object] = {
        "initial_limit": 2,
        "minimum_limit": 1,
        "maximum_limit": 8,
        "sample_window": 2,
        "latency_budget_seconds": 0.1,
        "maximum_failure_rate": 0.25,
        "decrease_ratio": 0.5,
        "additive_step": 1,
        "minimum_utilization": 1.0,
    }
    values.update(overrides)
    return ConcurrencyPolicy(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("initial_limit", 0),
        ("initial_limit", True),
        ("maximum_limit", 100_001),
        ("sample_window", 0),
        ("latency_budget_seconds", math.nan),
        ("maximum_failure_rate", 1.1),
        ("decrease_ratio", 1.0),
        ("minimum_utilization", 0.0),
    ],
)
def test_policy_rejects_invalid_values(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        policy(**{field: value})


@pytest.mark.asyncio
async def test_rejects_immediately_at_limit_without_waiter_queue() -> None:
    limiter = AdaptiveConcurrencyLimiter(policy())
    one = await limiter.try_acquire()
    two = await limiter.try_acquire()
    rejected = await limiter.try_acquire()
    assert one.admitted and two.admitted
    assert not rejected.admitted
    assert rejected.reason == "limit_reached"
    assert rejected.in_flight == 2
    snapshot = await limiter.snapshot()
    assert snapshot["in_flight"] == 2
    assert snapshot["rejected_total"] == 1


@pytest.mark.asyncio
async def test_healthy_saturation_additively_increases_limit() -> None:
    clock = Clock()
    limiter = AdaptiveConcurrencyLimiter(policy(), clock=clock)
    admissions = [await limiter.try_acquire() for _ in range(2)]
    for admission in admissions:
        clock.advance(0.01)
        await limiter.complete(admission.lease, Outcome.SUCCESS)  # type: ignore[arg-type]
    snapshot = await limiter.snapshot()
    assert snapshot["limit"] == 3
    assert snapshot["last_adjustment"] == "healthy_saturation"
    assert snapshot["generation"] == 1


@pytest.mark.asyncio
async def test_underutilized_window_does_not_increase_limit() -> None:
    clock = Clock()
    limiter = AdaptiveConcurrencyLimiter(policy(initial_limit=4), clock=clock)
    for _ in range(2):
        admission = await limiter.try_acquire()
        clock.advance(0.01)
        await limiter.complete(admission.lease, Outcome.SUCCESS)  # type: ignore[arg-type]
    snapshot = await limiter.snapshot()
    assert snapshot["limit"] == 4
    assert snapshot["last_adjustment"] == "underutilized"


@pytest.mark.asyncio
async def test_latency_breach_multiplicatively_decreases_limit() -> None:
    clock = Clock()
    limiter = AdaptiveConcurrencyLimiter(policy(initial_limit=4), clock=clock)
    admissions = [await limiter.try_acquire() for _ in range(2)]
    for admission in admissions:
        clock.advance(0.11)
        await limiter.complete(admission.lease, Outcome.SUCCESS)  # type: ignore[arg-type]
    snapshot = await limiter.snapshot()
    assert snapshot["limit"] == 2
    assert snapshot["last_adjustment"] == "latency_budget"


@pytest.mark.asyncio
async def test_failure_rate_takes_precedence_over_latency() -> None:
    clock = Clock()
    limiter = AdaptiveConcurrencyLimiter(policy(initial_limit=4), clock=clock)
    one = await limiter.try_acquire()
    two = await limiter.try_acquire()
    clock.advance(0.2)
    await limiter.complete(one.lease, Outcome.FAILURE)  # type: ignore[arg-type]
    await limiter.complete(two.lease, Outcome.SUCCESS)  # type: ignore[arg-type]
    snapshot = await limiter.snapshot()
    assert snapshot["limit"] == 2
    assert snapshot["last_adjustment"] == "failure_rate"
    assert snapshot["last_failure_rate"] == 0.5


@pytest.mark.asyncio
async def test_decrease_never_crosses_minimum() -> None:
    clock = Clock()
    limiter = AdaptiveConcurrencyLimiter(
        policy(initial_limit=2, minimum_limit=2, sample_window=1), clock=clock
    )
    admission = await limiter.try_acquire()
    await limiter.complete(admission.lease, Outcome.FAILURE)  # type: ignore[arg-type]
    assert (await limiter.snapshot())["limit"] == 2


@pytest.mark.asyncio
async def test_increase_never_crosses_maximum() -> None:
    clock = Clock()
    limiter = AdaptiveConcurrencyLimiter(
        policy(initial_limit=2, maximum_limit=2, sample_window=1), clock=clock
    )
    admission = await limiter.try_acquire()
    second = await limiter.try_acquire()
    await limiter.complete(admission.lease, Outcome.SUCCESS)  # type: ignore[arg-type]
    snapshot = await limiter.snapshot()
    assert snapshot["limit"] == 2
    assert snapshot["last_adjustment"] == "maximum_limit"
    await limiter.complete(second.lease, Outcome.SUCCESS)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_cancelled_operation_releases_without_penalizing_dependency() -> None:
    limiter = AdaptiveConcurrencyLimiter(policy())
    started = asyncio.Event()

    async def operation() -> None:
        started.set()
        await asyncio.Future()

    task = asyncio.create_task(limiter.run(operation))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    snapshot = await limiter.snapshot()
    assert snapshot["in_flight"] == 0
    assert snapshot["cancelled_total"] == 1
    assert snapshot["pending_samples"] == 0


@pytest.mark.asyncio
async def test_operation_failure_releases_and_is_sampled() -> None:
    limiter = AdaptiveConcurrencyLimiter(policy(sample_window=1))

    async def operation() -> None:
        raise ConnectionError("sensitive upstream detail")

    with pytest.raises(ConnectionError):
        await limiter.run(operation)
    snapshot = await limiter.snapshot()
    assert snapshot["in_flight"] == 0
    assert snapshot["failure_total"] == 1
    assert "sensitive" not in str(snapshot)


@pytest.mark.asyncio
async def test_non_awaitable_operation_is_failure_and_releases() -> None:
    limiter = AdaptiveConcurrencyLimiter(policy(sample_window=1))
    with pytest.raises(TypeError):
        await limiter.run(lambda: 1)  # type: ignore[arg-type,return-value]
    assert (await limiter.snapshot())["in_flight"] == 0


@pytest.mark.asyncio
async def test_duplicate_and_foreign_leases_fail_closed() -> None:
    clock = Clock()
    limiter = AdaptiveConcurrencyLimiter(policy(), clock=clock)
    admission = await limiter.try_acquire()
    assert admission.lease is not None
    foreign = Lease("foreign", admission.lease.sequence, admission.lease.started_at)
    with pytest.raises(LeaseError):
        await limiter.complete(foreign, Outcome.SUCCESS)
    await limiter.complete(admission.lease, Outcome.SUCCESS)
    with pytest.raises(LeaseError):
        await limiter.complete(admission.lease, Outcome.SUCCESS)


@pytest.mark.asyncio
async def test_invalid_outcome_does_not_consume_lease() -> None:
    limiter = AdaptiveConcurrencyLimiter(policy())
    admission = await limiter.try_acquire()
    with pytest.raises(LeaseError):
        await limiter.complete(admission.lease, "success")  # type: ignore[arg-type]
    await limiter.complete(admission.lease, Outcome.SUCCESS)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_bad_clock_fails_closed_and_does_not_leak_slot() -> None:
    values = iter([0.0, 0.0, -1.0])
    limiter = AdaptiveConcurrencyLimiter(policy(), clock=lambda: next(values))
    admission = await limiter.try_acquire()
    with pytest.raises(LeaseError):
        await limiter.complete(admission.lease, Outcome.SUCCESS)  # type: ignore[arg-type]
    assert (await limiter.snapshot())["in_flight"] == 0


@pytest.mark.asyncio
async def test_policy_digest_is_stable_and_snapshot_is_bounded() -> None:
    one = AdaptiveConcurrencyLimiter(policy())
    two = AdaptiveConcurrencyLimiter(policy())
    assert one.policy_digest == two.policy_digest
    assert len(one.policy_digest) == 64
    snapshot = await one.snapshot()
    assert set(snapshot) == {
        "limit",
        "in_flight",
        "pending_samples",
        "generation",
        "last_adjustment",
        "last_p95_seconds",
        "last_failure_rate",
        "admitted_total",
        "rejected_total",
        "success_total",
        "failure_total",
        "cancelled_total",
        "policy_digest",
    }


@pytest.mark.asyncio
async def test_real_loopback_tcp_admission_never_exceeds_limit() -> None:
    active = 0
    peak = 0
    release = asyncio.Event()

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await release.wait()
        writer.write(await reader.read(4))
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        active -= 1

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    limiter = AdaptiveConcurrencyLimiter(policy(initial_limit=2, maximum_limit=2, sample_window=8))

    async def round_trip() -> bytes:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"ping")
        await writer.drain()
        response = await reader.read(4)
        writer.close()
        await writer.wait_closed()
        return response

    tasks = [asyncio.create_task(limiter.run(round_trip)) for _ in range(8)]
    await asyncio.sleep(0.05)
    release.set()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    server.close()
    await server.wait_closed()
    assert sum(item == b"ping" for item in results) == 2
    assert sum(isinstance(item, AdmissionRejected) for item in results) == 6
    assert peak == 2
    snapshot = await limiter.snapshot()
    assert snapshot["in_flight"] == 0
    assert snapshot["rejected_total"] == 6
