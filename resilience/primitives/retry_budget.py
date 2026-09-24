from __future__ import annotations

import math
import threading
from dataclasses import asdict, dataclass
from decimal import Decimal
from enum import StrEnum


def _finite_decimal(name: str, value: float) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number")
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return Decimal(str(value))


@dataclass(frozen=True, slots=True)
class RetryBudgetPolicy:
    """Bound aggregate retry amplification for one downstream dependency.

    Every original request adds ``tokens_per_original`` credits, up to
    ``max_tokens``. A retry must atomically consume ``retry_cost`` credits.
    ``initial_tokens`` is a deliberately small reserve for cold starts.
    """

    initial_tokens: float = 1.0
    max_tokens: float = 10.0
    tokens_per_original: float = 0.2
    retry_cost: float = 1.0

    def __post_init__(self) -> None:
        initial = _finite_decimal("initial_tokens", self.initial_tokens)
        maximum = _finite_decimal("max_tokens", self.max_tokens)
        ratio = _finite_decimal("tokens_per_original", self.tokens_per_original)
        cost = _finite_decimal("retry_cost", self.retry_cost)

        if initial < 0:
            raise ValueError("initial_tokens must be non-negative")
        if maximum <= 0:
            raise ValueError("max_tokens must be positive")
        if initial > maximum:
            raise ValueError("initial_tokens cannot exceed max_tokens")
        if not 0 <= ratio <= 1:
            raise ValueError("tokens_per_original must be between zero and one")
        if cost <= 0:
            raise ValueError("retry_cost must be positive")
        if cost > maximum:
            raise ValueError("retry_cost cannot exceed max_tokens")


class RetryBudgetReason(StrEnum):
    ADMITTED = "admitted"
    BUDGET_EXHAUSTED = "budget_exhausted"


@dataclass(frozen=True, slots=True)
class RetryBudgetSnapshot:
    available_tokens: float
    max_tokens: float
    retry_cost: float
    original_requests: int
    admitted_retries: int
    rejected_retries: int

    def as_dict(self) -> dict[str, float | int]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RetryBudgetDecision:
    admitted: bool
    reason: RetryBudgetReason
    sequence: int
    tokens_before: float
    tokens_after: float
    retry_cost: float

    def as_dict(self) -> dict[str, bool | float | int | str]:
        result = asdict(self)
        result["reason"] = self.reason.value
        return result


class RetryBudget:
    """Thread-safe, constant-memory retry admission controller.

    Create one instance per downstream dependency. Call ``record_original``
    exactly once for each top-level request, then require a successful
    ``try_acquire_retry`` decision before every additional network attempt.
    """

    def __init__(self, policy: RetryBudgetPolicy | None = None) -> None:
        self.policy = policy or RetryBudgetPolicy()
        self._maximum = Decimal(str(self.policy.max_tokens))
        self._credit = Decimal(str(self.policy.tokens_per_original))
        self._cost = Decimal(str(self.policy.retry_cost))
        self._tokens = Decimal(str(self.policy.initial_tokens))
        self._original_requests = 0
        self._admitted_retries = 0
        self._rejected_retries = 0
        self._decision_sequence = 0
        self._lock = threading.Lock()

    def record_original(self, count: int = 1) -> RetryBudgetSnapshot:
        """Credit one or more non-retry attempts and return the new snapshot."""
        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError("count must be an integer")
        if count < 1:
            raise ValueError("count must be at least one")

        with self._lock:
            self._original_requests += count
            self._tokens = min(self._maximum, self._tokens + self._credit * count)
            return self._snapshot_unlocked()

    def try_acquire_retry(self) -> RetryBudgetDecision:
        """Atomically consume retry credit, or return fail-closed evidence."""
        with self._lock:
            before = self._tokens
            self._decision_sequence += 1
            if before >= self._cost:
                self._tokens -= self._cost
                self._admitted_retries += 1
                admitted = True
                reason = RetryBudgetReason.ADMITTED
            else:
                self._rejected_retries += 1
                admitted = False
                reason = RetryBudgetReason.BUDGET_EXHAUSTED

            return RetryBudgetDecision(
                admitted=admitted,
                reason=reason,
                sequence=self._decision_sequence,
                tokens_before=float(before),
                tokens_after=float(self._tokens),
                retry_cost=float(self._cost),
            )

    def snapshot(self) -> RetryBudgetSnapshot:
        with self._lock:
            return self._snapshot_unlocked()

    def _snapshot_unlocked(self) -> RetryBudgetSnapshot:
        return RetryBudgetSnapshot(
            available_tokens=float(self._tokens),
            max_tokens=float(self._maximum),
            retry_cost=float(self._cost),
            original_requests=self._original_requests,
            admitted_retries=self._admitted_retries,
            rejected_retries=self._rejected_retries,
        )
