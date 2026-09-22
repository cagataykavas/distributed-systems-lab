from __future__ import annotations

import json

import pytest

from resilience.deadline import DeadlineExceeded, RequestDeadline
from resilience.executor import ResilientExecutor
from resilience.primitives.circuit_breaker import CircuitBreaker
from resilience.primitives.retry import RetryPolicy


class ManualClock:
    def __init__(self, value: float = 0.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def make_executor(
    *,
    sleeper,
    base_delay: float = 0.25,
    attempts: int = 3,
) -> ResilientExecutor[int]:
    return ResilientExecutor(
        breaker=CircuitBreaker(
            failure_threshold=10,
            retry_on=(TimeoutError,),
        ),
        retry_policy=RetryPolicy(
            attempts=attempts,
            base_delay=base_delay,
            jitter_ratio=0,
        ),
        retry_on=(TimeoutError,),
        sleeper=sleeper,
    )


def test_deadline_stops_retry_when_backoff_cannot_fit() -> None:
    clock = ManualClock()
    delays: list[float] = []
    executor = make_executor(sleeper=delays.append, base_delay=0.3)
    deadline = RequestDeadline.after(1.0, clock=clock)
    calls = 0

    def fail_slowly() -> int:
        nonlocal calls
        calls += 1
        clock.advance(0.8)
        raise TimeoutError("downstream timeout")

    with pytest.raises(DeadlineExceeded) as captured:
        executor.execute(
            key="budgeted-request",
            payload={"value": 1},
            operation=fail_slowly,
            deadline=deadline,
        )

    evidence = captured.value.as_dict()
    assert calls == 1
    assert delays == []
    assert evidence["reason"] == "request_deadline_exceeded"
    assert evidence["phase"] == "before_retry_sleep"
    assert evidence["attempts"] == 1
    assert evidence["required_delay_seconds"] == pytest.approx(0.3)
    assert evidence["deadline"]["remaining_seconds"] == pytest.approx(0.2)
    json.dumps(evidence)


def test_executor_retries_when_budget_remains() -> None:
    clock = ManualClock()
    delays: list[float] = []

    def sleep(seconds: float) -> None:
        delays.append(seconds)
        clock.advance(seconds)

    executor = make_executor(sleeper=sleep)
    deadline = RequestDeadline.after(2.0, clock=clock)
    calls = 0

    def flaky() -> int:
        nonlocal calls
        calls += 1
        clock.advance(0.1)
        if calls == 1:
            raise TimeoutError("temporary")
        return 42

    result = executor.execute(
        key="enough-budget",
        payload={"value": 21},
        operation=flaky,
        deadline=deadline,
    )

    assert result.value == 42
    assert result.attempts == 2
    assert delays == [0.25]
    assert deadline.snapshot().remaining_seconds == pytest.approx(1.55)


def test_sleep_overshoot_prevents_the_next_attempt() -> None:
    clock = ManualClock()
    delays: list[float] = []

    def oversleep(seconds: float) -> None:
        delays.append(seconds)
        clock.advance(0.5)

    executor = make_executor(sleeper=oversleep, base_delay=0.2)
    deadline = RequestDeadline.after(0.4, clock=clock)
    calls = 0

    def fail() -> int:
        nonlocal calls
        calls += 1
        raise TimeoutError("temporary")

    with pytest.raises(DeadlineExceeded) as captured:
        executor.execute(
            key="overslept",
            payload={"value": 1},
            operation=fail,
            deadline=deadline,
        )

    assert calls == 1
    assert delays == [0.2]
    assert captured.value.phase == "before_attempt"
    assert captured.value.attempts == 1


def test_expired_deadline_blocks_new_work() -> None:
    clock = ManualClock()
    deadline = RequestDeadline.after(0.5, clock=clock)
    clock.advance(0.5)
    executor = make_executor(sleeper=lambda _: None)
    calls = 0

    def operation() -> int:
        nonlocal calls
        calls += 1
        return 1

    with pytest.raises(DeadlineExceeded) as captured:
        executor.execute(
            key="expired",
            payload={"value": 1},
            operation=operation,
            deadline=deadline,
        )

    assert calls == 0
    assert captured.value.attempts == 0


def test_completed_idempotent_replay_does_not_consume_deadline() -> None:
    clock = ManualClock()
    executor = make_executor(sleeper=lambda _: None, attempts=1)
    first = executor.execute(
        key="completed",
        payload={"value": 2},
        operation=lambda: 4,
    )
    deadline = RequestDeadline.after(0.1, clock=clock)
    clock.advance(0.1)

    replay = executor.execute(
        key="completed",
        payload={"value": 2},
        operation=lambda: (_ for _ in ()).throw(AssertionError("must not execute")),
        deadline=deadline,
    )

    assert first.executed is True
    assert replay.value == 4
    assert replay.executed is False
    assert replay.attempts == 0


@pytest.mark.parametrize(
    "timeout",
    [0, -1, float("nan"), float("inf"), True, "1"],
)
def test_invalid_deadline_timeout_fails_closed(timeout) -> None:
    with pytest.raises((TypeError, ValueError)):
        RequestDeadline.after(timeout)


def test_non_finite_or_regressing_clock_fails_closed() -> None:
    with pytest.raises(ValueError, match="clock value"):
        RequestDeadline.after(1, clock=lambda: float("nan"))

    clock = ManualClock(10)
    deadline = RequestDeadline.after(1, clock=clock)
    clock.value = 9
    with pytest.raises(RuntimeError, match="moved before"):
        deadline.snapshot()
