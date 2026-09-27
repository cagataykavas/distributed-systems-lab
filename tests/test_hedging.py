from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable

import pytest

from resilience.primitives.hedging import (
    AttemptEvidence,
    AttemptOutcome,
    HedgedCallError,
    HedgePolicy,
    hedged_call,
)


def run(coroutine: Awaitable[object]) -> object:
    return asyncio.run(coroutine)


def operation(
    value: str,
    *,
    delay: float = 0,
    error: Exception | None = None,
    cancelled: list[int] | None = None,
) -> Callable[[int], Awaitable[str]]:
    async def execute(attempt: int) -> str:
        try:
            await asyncio.sleep(delay)
            if error is not None:
                raise error
            return value
        except asyncio.CancelledError:
            if cancelled is not None:
                cancelled.append(attempt)
            raise

    return execute


def policy(**overrides: object) -> HedgePolicy:
    values: dict[str, object] = {
        "hedge_delay_seconds": 0.01,
        "timeout_seconds": 0.25,
        "max_attempts": 2,
        "cancellation_grace_seconds": 0.05,
    }
    values.update(overrides)
    return HedgePolicy(**values)  # type: ignore[arg-type]


def test_primary_success_avoids_unnecessary_hedge() -> None:
    launched: list[int] = []

    async def primary(attempt: int) -> str:
        launched.append(attempt)
        return "primary"

    async def backup(attempt: int) -> str:
        launched.append(attempt)
        return "backup"

    result = run(
        hedged_call(
            [primary, backup],
            request_token="request-1",
            replay_safe=True,
            policy=policy(),
        )
    )

    assert result.value == "primary"
    assert result.evidence.winner_attempt == 0
    assert result.evidence.launched_attempts == 1
    assert launched == [0]


def test_slow_primary_launches_hedge_and_cancels_loser() -> None:
    cancelled: list[int] = []
    result = run(
        hedged_call(
            [
                operation("slow", delay=1, cancelled=cancelled),
                operation("fast"),
            ],
            request_token="request-2",
            replay_safe=True,
            policy=policy(),
        )
    )

    assert result.value == "fast"
    assert result.evidence.winner_attempt == 1
    assert result.evidence.launched_attempts == 2
    assert cancelled == [0]
    assert result.evidence.attempts == (
        AttemptEvidence(0, AttemptOutcome.CANCELLED),
        AttemptEvidence(1, AttemptOutcome.SUCCEEDED),
    )


def test_hedge_uses_first_configured_delay_slot() -> None:
    async def scenario() -> float:
        loop = asyncio.get_running_loop()
        started = loop.time()
        backup_started = 0.0

        async def backup(_: int) -> str:
            nonlocal backup_started
            backup_started = loop.time()
            return "backup"

        await hedged_call(
            [operation("slow", delay=1), backup],
            request_token="hedge-slot",
            replay_safe=True,
            policy=policy(hedge_delay_seconds=0.02),
        )
        return backup_started - started

    delay = run(scenario())
    assert 0.015 <= delay < 0.1


def test_failure_accelerates_next_candidate() -> None:
    async def scenario() -> tuple[float, object]:
        loop = asyncio.get_running_loop()
        started = loop.time()
        result = await hedged_call(
            [operation("unused", error=OSError("private endpoint text")), operation("ok")],
            request_token="request-3",
            replay_safe=True,
            policy=policy(hedge_delay_seconds=0.2),
        )
        return loop.time() - started, result

    elapsed, result = run(scenario())
    assert elapsed < 0.1
    assert result.value == "ok"
    assert [item.outcome for item in result.evidence.attempts] == [
        AttemptOutcome.FAILED,
        AttemptOutcome.SUCCEEDED,
    ]


def test_all_failures_use_stable_content_free_evidence() -> None:
    with pytest.raises(HedgedCallError) as captured:
        run(
            hedged_call(
                [
                    operation("unused", error=ConnectionError("secret-a")),
                    operation("unused", error=RuntimeError("secret-b")),
                ],
                request_token="sensitive-request-token",
                replay_safe=True,
                policy=policy(),
            )
        )

    error = captured.value
    assert error.reason == "all_attempts_failed"
    assert error.evidence.outcome == "all_attempts_failed"
    assert error.evidence.request_digest == hashlib.sha256(b"sensitive-request-token").hexdigest()
    rendered = str(error.evidence.as_dict())
    assert "sensitive-request-token" not in rendered
    assert "secret-a" not in rendered
    assert "secret-b" not in rendered


def test_global_deadline_cancels_every_live_attempt() -> None:
    cancelled: list[int] = []
    with pytest.raises(HedgedCallError) as captured:
        run(
            hedged_call(
                [
                    operation("unused", delay=1, cancelled=cancelled),
                    operation("unused", delay=1, cancelled=cancelled),
                ],
                request_token="deadline",
                replay_safe=True,
                policy=policy(timeout_seconds=0.04),
            )
        )

    assert captured.value.reason == "deadline_exceeded"
    assert captured.value.evidence.launched_attempts == 2
    assert cancelled == [0, 1]
    assert all(
        item.outcome is AttemptOutcome.CANCELLED for item in captured.value.evidence.attempts
    )


def test_simultaneous_success_prefers_lower_attempt_index() -> None:
    ready = asyncio.Event()
    launched = 0

    async def candidate(attempt: int) -> str:
        nonlocal launched
        launched += 1
        if launched == 2:
            ready.set()
        await ready.wait()
        return f"candidate-{attempt}"

    result = run(
        hedged_call(
            [candidate, candidate],
            request_token="tie",
            replay_safe=True,
            policy=policy(),
        )
    )

    assert result.value == "candidate-0"
    assert result.evidence.winner_attempt == 0
    assert result.evidence.attempts == (
        AttemptEvidence(0, AttemptOutcome.SUCCEEDED),
        AttemptEvidence(1, AttemptOutcome.SUCCEEDED),
    )


def test_caller_cancellation_reaches_active_attempts() -> None:
    cancelled: list[int] = []

    async def scenario() -> None:
        task = asyncio.create_task(
            hedged_call(
                [
                    operation("unused", delay=1, cancelled=cancelled),
                    operation("unused", delay=1, cancelled=cancelled),
                ],
                request_token="caller-cancel",
                replay_safe=True,
                policy=policy(),
            )
        )
        await asyncio.sleep(0.03)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    run(scenario())
    assert cancelled == [0, 1]


def test_cancellation_resistance_is_reported_without_blocking_winner() -> None:
    async def cancellation_resistant(_: int) -> str:
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            await asyncio.sleep(0.05)
        return "late"

    result = run(
        hedged_call(
            [cancellation_resistant, operation("fast")],
            request_token="resistant-adapter",
            replay_safe=True,
            policy=policy(cancellation_grace_seconds=0.001),
        )
    )

    assert result.value == "fast"
    assert result.evidence.attempts == (
        AttemptEvidence(0, AttemptOutcome.CANCELLATION_UNCONFIRMED),
        AttemptEvidence(1, AttemptOutcome.SUCCEEDED),
    )


def test_real_loopback_tcp_hedge_uses_responsive_endpoint() -> None:
    async def scenario() -> object:
        async def slow_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                await reader.readline()
                await asyncio.sleep(0.2)
                writer.write(b"slow\n")
                await writer.drain()
            except (ConnectionError, asyncio.CancelledError):
                pass
            finally:
                writer.close()
                await writer.wait_closed()

        async def fast_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                await reader.readline()
                writer.write(b"fast\n")
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        slow_server = await asyncio.start_server(slow_handler, "127.0.0.1", 0)
        fast_server = await asyncio.start_server(fast_handler, "127.0.0.1", 0)
        slow_port = slow_server.sockets[0].getsockname()[1]
        fast_port = fast_server.sockets[0].getsockname()[1]

        def tcp_request(port: int) -> Callable[[int], Awaitable[str]]:
            async def request(_: int) -> str:
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                try:
                    writer.write(b"probe\n")
                    await writer.drain()
                    return (await reader.readline()).decode().strip()
                finally:
                    writer.close()
                    await writer.wait_closed()

            return request

        try:
            return await hedged_call(
                [tcp_request(slow_port), tcp_request(fast_port)],
                request_token="loopback-probe",
                replay_safe=True,
                policy=policy(timeout_seconds=0.5),
            )
        finally:
            slow_server.close()
            fast_server.close()
            await slow_server.wait_closed()
            await fast_server.wait_closed()

    result = run(scenario())
    assert result.value == "fast"
    assert result.evidence.winner_attempt == 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("hedge_delay_seconds", 0),
        ("hedge_delay_seconds", float("nan")),
        ("timeout_seconds", 0),
        ("timeout_seconds", float("inf")),
        ("max_attempts", 0),
        ("max_attempts", 9),
        ("cancellation_grace_seconds", -1),
    ],
)
def test_policy_rejects_unbounded_or_invalid_values(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        policy(**{field: value})


@pytest.mark.parametrize("token", ["", "x" * 257])
def test_request_token_is_bounded(token: str) -> None:
    with pytest.raises(ValueError):
        run(
            hedged_call(
                [operation("ok")],
                request_token=token,
                replay_safe=True,
                policy=policy(),
            )
        )


def test_requires_explicit_replay_safety() -> None:
    with pytest.raises(ValueError, match="replay-safe"):
        run(
            hedged_call(
                [operation("ok")],
                request_token="unsafe-write",
                replay_safe=False,
                policy=policy(),
            )
        )


def test_rejects_attempt_fanout_above_policy() -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        run(
            hedged_call(
                [operation("a"), operation("b"), operation("c")],
                request_token="fanout",
                replay_safe=True,
                policy=policy(max_attempts=2),
            )
        )


def test_rejects_non_awaitable_adapter_result() -> None:
    def invalid_operation(_: int) -> str:
        return "not-awaitable"

    with pytest.raises(TypeError, match="awaitables"):
        run(
            hedged_call(  # type: ignore[list-item]
                [invalid_operation],
                request_token="bad-adapter",
                replay_safe=True,
                policy=policy(),
            )
        )
