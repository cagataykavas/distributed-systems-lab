from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


class IdempotencyConflict(ValueError):
    """The same idempotency key was reused for a different logical request."""


@dataclass(frozen=True, slots=True)
class IdempotencyEntry(Generic[T]):
    fingerprint: str
    value: T


def fingerprint_json(payload: object) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class IdempotencyStore(Generic[T]):
    """Process-local idempotency store with one lock per logical key.

    The per-key lock makes concurrent duplicates serialize without blocking unrelated
    keys. This is intentionally a local teaching primitive; a multi-process service
    would require the same atomicity guarantee from a shared datastore.
    """

    def __init__(self) -> None:
        self._entries: dict[str, IdempotencyEntry[T]] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def _lock_for(self, key: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(key, threading.Lock())

    def get_entry(
        self,
        key: str,
        fingerprint: str,
    ) -> IdempotencyEntry[T] | None:
        entry = self._entries.get(key)
        if entry is not None and entry.fingerprint != fingerprint:
            raise IdempotencyConflict(
                "idempotency key reused with a different request fingerprint"
            )
        return entry

    def execute_once(
        self,
        key: str,
        fingerprint: str,
        operation: Callable[[], T],
    ) -> tuple[T, bool]:
        if not key:
            raise ValueError("idempotency key must not be empty")
        if not fingerprint:
            raise ValueError("fingerprint must not be empty")

        with self._lock_for(key):
            entry = self.get_entry(key, fingerprint)
            if entry is not None:
                return entry.value, False

            value = operation()
            self._entries[key] = IdempotencyEntry(
                fingerprint=fingerprint,
                value=value,
            )
            return value, True

    def __len__(self) -> int:
        return len(self._entries)


class DedupStore(Generic[T]):
    """Compatibility wrapper for the original key-only lab API."""

    def __init__(self) -> None:
        self._store: IdempotencyStore[T] = IdempotencyStore()

    @property
    def values(self) -> dict[str, T]:
        return {
            key: entry.value
            for key, entry in self._store._entries.items()
        }

    def execute_once(
        self,
        key: str,
        operation: Callable[[], T],
    ) -> tuple[T, bool]:
        return self._store.execute_once(key, key, operation)
