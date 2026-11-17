from __future__ import annotations

import pytest

from resilience.network_faults import (
    FaultWindow,
    NetworkFaultInjector,
    NetworkFaultKind,
)


class Clock:
    def __init__(self, milliseconds: float = 0) -> None:
        self.milliseconds = milliseconds

    def __call__(self) -> float:
        return self.milliseconds / 1000


def test_fault_window_uses_half_open_boundaries() -> None:
    clock = Clock()
    injector = NetworkFaultInjector(
        (FaultWindow(100, 200, NetworkFaultKind.TIMEOUT),),
        clock=clock,
    )

    clock.milliseconds = 100
    with pytest.raises(TimeoutError):
        injector.call(lambda: "unreachable")

    clock.milliseconds = 200
    assert injector.call(lambda: "recovered") == "recovered"
    assert injector.snapshot().timed_out == 1
    assert injector.snapshot().passed == 1


def test_latency_injection_uses_supplied_sleeper() -> None:
    sleeps: list[float] = []
    injector = NetworkFaultInjector(
        (FaultWindow(0, 50, NetworkFaultKind.LATENCY, latency_ms=25),),
        clock=Clock(),
        sleeper=sleeps.append,
    )

    assert injector.call(lambda: 42) == 42
    assert sleeps == [0.025]
    assert injector.snapshot().delayed == 1


def test_reset_and_timeout_have_distinct_evidence() -> None:
    clock = Clock()
    injector = NetworkFaultInjector(
        (
            FaultWindow(0, 20, NetworkFaultKind.RESET),
            FaultWindow(20, 40, NetworkFaultKind.TIMEOUT),
        ),
        clock=clock,
    )

    clock.milliseconds = 10
    with pytest.raises(ConnectionResetError):
        injector.call(lambda: None)
    clock.milliseconds = 20
    with pytest.raises(TimeoutError):
        injector.call(lambda: None)

    snapshot = injector.snapshot()
    assert snapshot.calls == 2
    assert snapshot.reset == 1
    assert snapshot.timed_out == 1


@pytest.mark.parametrize(
    "windows",
    [
        (
            FaultWindow(10, 20, NetworkFaultKind.TIMEOUT),
            FaultWindow(19, 30, NetworkFaultKind.RESET),
        ),
        (FaultWindow(20, 30, NetworkFaultKind.TIMEOUT), FaultWindow(0, 10, NetworkFaultKind.RESET)),
        (FaultWindow(0, 10, NetworkFaultKind.TIMEOUT, latency_ms=1),),
        (FaultWindow(0, 10, NetworkFaultKind.LATENCY, latency_ms=0),),
    ],
)
def test_ambiguous_or_invalid_fault_schedules_are_rejected(
    windows: tuple[FaultWindow, ...],
) -> None:
    with pytest.raises(ValueError):
        NetworkFaultInjector(windows)


def test_invalid_clock_fails_before_operation() -> None:
    called = False

    def operation() -> None:
        nonlocal called
        called = True

    with pytest.raises(ValueError, match="clock"):
        NetworkFaultInjector((), clock=lambda: float("nan"))
    assert not called
