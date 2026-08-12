from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Generic, TypeVar

T = TypeVar("T")


@dataclass
class DedupStore(Generic[T]):
    values: dict[str, T]

    def __init__(self) -> None:
        self.values = {}

    def execute_once(self, key: str, operation: Callable[[], T]) -> tuple[T, bool]:
        if key in self.values:
            return self.values[key], False
        result = operation()
        self.values[key] = result
        return result, True


if __name__ == "__main__":
    store = DedupStore[int]()
    counter = {"value": 0}

    def charge() -> int:
        counter["value"] += 1
        return counter["value"]

    print(store.execute_once("payment-42", charge))
    print(store.execute_once("payment-42", charge))
    print("actual executions:", counter["value"])
