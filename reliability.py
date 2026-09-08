"""Backward-compatible imports for the original reliability lab module.

New code should import from ``resilience.primitives``. This module remains so old
examples and interview snippets keep working while the implementation lives in a
real package.
"""

from resilience.primitives.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitSnapshot,
    CircuitState,
)
from resilience.primitives.retry import RetryPolicy, retry

__all__ = [
    "CircuitBreaker",
    "CircuitOpenError",
    "CircuitSnapshot",
    "CircuitState",
    "RetryPolicy",
    "retry",
]
