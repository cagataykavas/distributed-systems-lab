from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from resilience.primitives import RetryBudget as PublicRetryBudget
from resilience.primitives.retry_budget import (
    RetryBudget,
    RetryBudgetPolicy,
    RetryBudgetReason,
)


def test_retry_budget_is_available_from_public_primitives_api() -> None:
    assert PublicRetryBudget is RetryBudget


def test_original_traffic_earns_bounded_retry_credit() -> None:
    budget = RetryBudget(
        RetryBudgetPolicy(
            initial_tokens=0,
            max_tokens=3,
            tokens_per_original=0.25,
        )
    )

    snapshot = budget.record_original(20)

    assert snapshot.available_tokens == 3
    assert snapshot.original_requests == 20


def test_retry_is_rejected_until_enough_credit_exists() -> None:
    budget = RetryBudget(
        RetryBudgetPolicy(
            initial_tokens=0,
            max_tokens=2,
            tokens_per_original=0.2,
        )
    )
    budget.record_original(4)

    rejected = budget.try_acquire_retry()
    assert rejected.admitted is False
    assert rejected.reason is RetryBudgetReason.BUDGET_EXHAUSTED
    assert rejected.tokens_before == rejected.tokens_after == 0.8

    budget.record_original()
    admitted = budget.try_acquire_retry()
    assert admitted.admitted is True
    assert admitted.reason is RetryBudgetReason.ADMITTED
    assert admitted.tokens_before == 1
    assert admitted.tokens_after == 0


def test_rejection_does_not_consume_partial_credit() -> None:
    budget = RetryBudget(
        RetryBudgetPolicy(
            initial_tokens=0.5,
            max_tokens=2,
            tokens_per_original=0,
        )
    )

    assert budget.try_acquire_retry().admitted is False
    assert budget.try_acquire_retry().tokens_after == 0.5
    assert budget.snapshot().rejected_retries == 2


def test_custom_retry_cost_is_accounted_exactly() -> None:
    budget = RetryBudget(
        RetryBudgetPolicy(
            initial_tokens=1,
            max_tokens=2,
            tokens_per_original=0.1,
            retry_cost=0.3,
        )
    )

    decisions = [budget.try_acquire_retry() for _ in range(4)]

    assert [decision.admitted for decision in decisions] == [True, True, True, False]
    assert budget.snapshot().available_tokens == 0.1


def test_concurrent_admission_cannot_overspend_budget() -> None:
    budget = RetryBudget(
        RetryBudgetPolicy(
            initial_tokens=5,
            max_tokens=5,
            tokens_per_original=0,
        )
    )

    with ThreadPoolExecutor(max_workers=20) as pool:
        decisions = list(pool.map(lambda _: budget.try_acquire_retry(), range(50)))

    assert sum(decision.admitted for decision in decisions) == 5
    assert sorted(decision.sequence for decision in decisions) == list(range(1, 51))
    assert budget.snapshot().as_dict() == {
        "available_tokens": 0.0,
        "max_tokens": 5.0,
        "retry_cost": 1.0,
        "original_requests": 0,
        "admitted_retries": 5,
        "rejected_retries": 45,
    }


def test_budget_isolated_per_downstream_dependency() -> None:
    policy = RetryBudgetPolicy(
        initial_tokens=1,
        max_tokens=1,
        tokens_per_original=0,
    )
    inventory = RetryBudget(policy)
    payments = RetryBudget(policy)

    assert inventory.try_acquire_retry().admitted is True
    assert inventory.try_acquire_retry().admitted is False
    assert payments.try_acquire_retry().admitted is True


def test_decision_evidence_is_json_serializable_and_content_free() -> None:
    budget = RetryBudget()

    encoded = json.dumps(budget.try_acquire_retry().as_dict(), sort_keys=True)

    assert '"reason": "admitted"' in encoded
    assert "payload" not in encoded
    assert "error" not in encoded


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("initial_tokens", -1, "initial_tokens must be non-negative"),
        ("max_tokens", 0, "max_tokens must be positive"),
        ("tokens_per_original", 1.01, "between zero and one"),
        ("retry_cost", 0, "retry_cost must be positive"),
        ("max_tokens", float("nan"), "max_tokens must be finite"),
        ("retry_cost", float("inf"), "retry_cost must be finite"),
    ],
)
def test_policy_rejects_unsafe_values(field: str, value: float, message: str) -> None:
    values = {
        "initial_tokens": 1,
        "max_tokens": 10,
        "tokens_per_original": 0.2,
        "retry_cost": 1,
    }
    values[field] = value

    with pytest.raises(ValueError, match=message):
        RetryBudgetPolicy(**values)


def test_policy_rejects_impossible_reserve_and_cost() -> None:
    with pytest.raises(ValueError, match="initial_tokens cannot exceed max_tokens"):
        RetryBudgetPolicy(initial_tokens=2, max_tokens=1)
    with pytest.raises(ValueError, match="retry_cost cannot exceed max_tokens"):
        RetryBudgetPolicy(retry_cost=2, max_tokens=1)


@pytest.mark.parametrize("count", [0, -1])
def test_record_original_rejects_non_positive_batches(count: int) -> None:
    with pytest.raises(ValueError, match="count must be at least one"):
        RetryBudget().record_original(count)


@pytest.mark.parametrize("count", [True, 1.5, "2"])
def test_record_original_rejects_ambiguous_batch_types(count: object) -> None:
    with pytest.raises(TypeError, match="count must be an integer"):
        RetryBudget().record_original(count)  # type: ignore[arg-type]
