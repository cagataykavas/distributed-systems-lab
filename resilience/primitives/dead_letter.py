from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Generic, TypeVar

T = TypeVar("T")


class QueueOutcome(StrEnum):
    IDLE = "idle"
    PROCESSED = "processed"
    RETRY_SCHEDULED = "retry_scheduled"
    DEAD_LETTERED = "dead_lettered"


@dataclass(slots=True)
class Message(Generic[T]):
    message_id: str
    payload: T
    attempts: int = 0
    errors: list[str] = field(default_factory=list)
    error_types: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.message_id:
            raise ValueError("message_id must not be empty")


class RetryQueue(Generic[T]):
    """In-memory queue that makes retry and DLQ transitions explicit."""

    def __init__(
        self,
        max_attempts: int = 3,
        retry_on: tuple[type[Exception], ...] = (Exception,),
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        self.max_attempts = max_attempts
        self.retry_on = retry_on
        self._ready: deque[Message[T]] = deque()
        self.dead_letter: list[Message[T]] = []

    @property
    def ready(self) -> list[Message[T]]:
        """Compatibility/read-only snapshot used by the original lab examples."""
        return list(self._ready)

    def publish(self, message: Message[T]) -> None:
        self._ready.append(message)

    def process_one(self, handler: Callable[[T], None]) -> str:
        if not self._ready:
            return QueueOutcome.IDLE.value

        message = self._ready.popleft()
        try:
            handler(message.payload)
        except self.retry_on as exc:
            message.attempts += 1
            error_type = type(exc).__name__
            message.errors.append(f"{error_type}: {exc}")
            message.error_types.append(error_type)
            if message.attempts >= self.max_attempts:
                self.dead_letter.append(message)
                return QueueOutcome.DEAD_LETTERED.value
            self._ready.append(message)
            return QueueOutcome.RETRY_SCHEDULED.value
        return QueueOutcome.PROCESSED.value
