from __future__ import annotations

import asyncio
import hashlib
import math
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Generic, TypeVar

T = TypeVar("T")
Operation = Callable[[int], Awaitable[T]]


class AttemptOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    CANCELLATION_UNCONFIRMED = "cancellation_unconfirmed"


@dataclass(frozen=True, slots=True)
class HedgePolicy:
    """Resource and timing limits for one logical hedged request."""

    hedge_delay_seconds: float = 0.05
    timeout_seconds: float = 1.0
    max_attempts: int = 2
    cancellation_grace_seconds: float = 0.05

    def __post_init__(self) -> None:
        finite_values = {
            "hedge_delay_seconds": self.hedge_delay_seconds,
            "timeout_seconds": self.timeout_seconds,
            "cancellation_grace_seconds": self.cancellation_grace_seconds,
        }
        for name, value in finite_values.items():
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
        if self.hedge_delay_seconds <= 0:
            raise ValueError("hedge_delay_seconds must be positive")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.max_attempts < 1 or self.max_attempts > 8:
            raise ValueError("max_attempts must be between one and eight")
        if self.cancellation_grace_seconds < 0:
            raise ValueError("cancellation_grace_seconds must be non-negative")


@dataclass(frozen=True, slots=True)
class AttemptEvidence:
    attempt: int
    outcome: AttemptOutcome


@dataclass(frozen=True, slots=True)
class HedgeEvidence:
    request_digest: str
    outcome: str
    winner_attempt: int | None
    launched_attempts: int
    attempts: tuple[AttemptEvidence, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "request_digest": self.request_digest,
            "outcome": self.outcome,
            "winner_attempt": self.winner_attempt,
            "launched_attempts": self.launched_attempts,
            "attempts": [asdict(attempt) for attempt in self.attempts],
        }


@dataclass(frozen=True, slots=True)
class HedgedResult(Generic[T]):
    value: T
    evidence: HedgeEvidence


class HedgedCallError(RuntimeError):
    def __init__(self, reason: str, evidence: HedgeEvidence) -> None:
        super().__init__(reason)
        self.reason = reason
        self.evidence = evidence


def _request_digest(request_token: str) -> str:
    if not isinstance(request_token, str):
        raise TypeError("request_token must be a string")
    if not request_token or len(request_token.encode("utf-8")) > 256:
        raise ValueError("request_token must contain between 1 and 256 UTF-8 bytes")
    return hashlib.sha256(request_token.encode("utf-8")).hexdigest()


def _consume_background_result(task: asyncio.Task[object]) -> None:
    try:
        task.exception()
    except (asyncio.CancelledError, Exception):
        pass


async def _cancel_losers(
    active: dict[asyncio.Task[T], int],
    *,
    grace_seconds: float,
) -> list[AttemptEvidence]:
    if not active:
        return []
    for task in active:
        task.cancel()

    done, pending = await asyncio.wait(active, timeout=grace_seconds)
    evidence: list[AttemptEvidence] = []
    for task in done:
        try:
            task.result()
        except asyncio.CancelledError:
            outcome = AttemptOutcome.CANCELLED
        except Exception:
            outcome = AttemptOutcome.FAILED
        else:
            outcome = AttemptOutcome.SUCCEEDED
        evidence.append(AttemptEvidence(active[task], outcome))
    for task in pending:
        task.add_done_callback(_consume_background_result)
        evidence.append(AttemptEvidence(active[task], AttemptOutcome.CANCELLATION_UNCONFIRMED))
    return evidence


def _build_evidence(
    *,
    request_digest: str,
    outcome: str,
    winner_attempt: int | None,
    launched_attempts: int,
    attempts: list[AttemptEvidence],
) -> HedgeEvidence:
    return HedgeEvidence(
        request_digest=request_digest,
        outcome=outcome,
        winner_attempt=winner_attempt,
        launched_attempts=launched_attempts,
        attempts=tuple(sorted(attempts, key=lambda item: item.attempt)),
    )


async def hedged_call(
    operations: Sequence[Operation[T]],
    *,
    request_token: str,
    replay_safe: bool,
    policy: HedgePolicy | None = None,
) -> HedgedResult[T]:
    """Return the first successful bounded attempt and cancel slower work.

    ``operations`` are ordered candidates. Attempt zero starts immediately; later
    candidates start at fixed hedge-delay intervals while the request is pending.
    A failed attempt accelerates the next candidate instead of waiting for its slot.
    """

    selected_policy = policy or HedgePolicy()
    digest = _request_digest(request_token)
    if replay_safe is not True:
        raise ValueError("hedging requires an explicitly replay-safe operation")
    if not operations:
        raise ValueError("at least one operation is required")
    if len(operations) > selected_policy.max_attempts:
        raise ValueError("operation count exceeds max_attempts")
    if any(not callable(operation) for operation in operations):
        raise TypeError("every operation must be callable")

    loop = asyncio.get_running_loop()
    started_at = loop.time()
    deadline = started_at + selected_policy.timeout_seconds
    next_slot = started_at + selected_policy.hedge_delay_seconds
    active: dict[asyncio.Task[T], int] = {}
    attempt_evidence: list[AttemptEvidence] = []
    next_attempt = 0

    def launch() -> None:
        nonlocal next_attempt, next_slot
        attempt = next_attempt
        try:
            awaitable = operations[attempt](attempt)
        except Exception as exc:

            async def failed_operation(captured: Exception = exc) -> T:
                raise captured

            awaitable = failed_operation()
        if not isinstance(awaitable, Awaitable):
            raise TypeError("operations must return awaitables")
        active[asyncio.create_task(awaitable)] = attempt
        next_attempt += 1
        next_slot = started_at + next_attempt * selected_policy.hedge_delay_seconds

    launch()
    try:
        while active:
            now = loop.time()
            if now >= deadline:
                cancelled = await _cancel_losers(
                    active, grace_seconds=selected_policy.cancellation_grace_seconds
                )
                attempt_evidence.extend(cancelled)
                evidence = _build_evidence(
                    request_digest=digest,
                    outcome="deadline_exceeded",
                    winner_attempt=None,
                    launched_attempts=next_attempt,
                    attempts=attempt_evidence,
                )
                raise HedgedCallError("deadline_exceeded", evidence)

            wake_at = deadline
            if next_attempt < len(operations):
                wake_at = min(wake_at, next_slot)
            done, _ = await asyncio.wait(
                active,
                timeout=max(0.0, wake_at - now),
                return_when=asyncio.FIRST_COMPLETED,
            )

            if not done:
                if loop.time() >= deadline:
                    continue
                launch()
                continue

            saw_failure = False
            successes: list[tuple[int, T]] = []
            for task in sorted(done, key=active.__getitem__):
                attempt = active.pop(task)
                try:
                    value = task.result()
                except asyncio.CancelledError:
                    attempt_evidence.append(AttemptEvidence(attempt, AttemptOutcome.CANCELLED))
                except Exception:
                    saw_failure = True
                    attempt_evidence.append(AttemptEvidence(attempt, AttemptOutcome.FAILED))
                else:
                    attempt_evidence.append(AttemptEvidence(attempt, AttemptOutcome.SUCCEEDED))
                    successes.append((attempt, value))

            if successes:
                winner_attempt, value = min(successes, key=lambda item: item[0])
                attempt_evidence.extend(
                    await _cancel_losers(
                        active,
                        grace_seconds=selected_policy.cancellation_grace_seconds,
                    )
                )
                evidence = _build_evidence(
                    request_digest=digest,
                    outcome="succeeded",
                    winner_attempt=winner_attempt,
                    launched_attempts=next_attempt,
                    attempts=attempt_evidence,
                )
                return HedgedResult(value=value, evidence=evidence)

            if saw_failure and not active and next_attempt < len(operations):
                launch()

        evidence = _build_evidence(
            request_digest=digest,
            outcome="all_attempts_failed",
            winner_attempt=None,
            launched_attempts=next_attempt,
            attempts=attempt_evidence,
        )
        raise HedgedCallError("all_attempts_failed", evidence)
    except asyncio.CancelledError:
        await _cancel_losers(active, grace_seconds=selected_policy.cancellation_grace_seconds)
        raise
    except Exception:
        await _cancel_losers(active, grace_seconds=selected_policy.cancellation_grace_seconds)
        raise
