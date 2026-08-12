from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Generic, TypeVar

T = TypeVar("T")


@dataclass
class Message(Generic[T]):
    message_id: str
    payload: T
    attempts: int = 0
    errors: list[str] = field(default_factory=list)


class RetryQueue(Generic[T]):
    def __init__(self, max_attempts: int = 3):
        self.max_attempts = max_attempts
        self.ready: list[Message[T]] = []
        self.dead_letter: list[Message[T]] = []

    def publish(self, message: Message[T]) -> None:
        self.ready.append(message)

    def process_one(self, handler: Callable[[T], None]) -> str:
        if not self.ready:
            return "idle"
        message = self.ready.pop(0)
        try:
            handler(message.payload)
            return "processed"
        except Exception as exc:
            message.attempts += 1
            message.errors.append(str(exc))
            if message.attempts >= self.max_attempts:
                self.dead_letter.append(message)
                return "dead_lettered"
            self.ready.append(message)
            return "retry_scheduled"


if __name__ == "__main__":
    queue: RetryQueue[dict] = RetryQueue(max_attempts=3)
    queue.publish(Message("evt-1", {"amount": 42.0}))

    def broken_handler(_: dict) -> None:
        raise RuntimeError("downstream timeout")

    while queue.ready:
        print(queue.process_one(broken_handler))
    print("DLQ:", queue.dead_letter)
