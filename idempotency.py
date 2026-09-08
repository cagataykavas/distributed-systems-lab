"""Backward-compatible imports for the original idempotency lab module."""

from resilience.primitives.idempotency import (
    DedupStore,
    IdempotencyConflict,
    IdempotencyEntry,
    IdempotencyStore,
    fingerprint_json,
)

__all__ = [
    "DedupStore",
    "IdempotencyConflict",
    "IdempotencyEntry",
    "IdempotencyStore",
    "fingerprint_json",
]
