from __future__ import annotations

import random
import time
from dataclasses import asdict, dataclass, field

from dead_letter_queue import Message, RetryQueue
from reliability import CircuitBreaker


@dataclass(frozen=True)
class ScenarioEvent:
    step: int
    operation: str
    outcome: str
    detail: str
    latency_ms: float


@dataclass
class ScenarioReport:
    name: str
    requests: int
    successes: int = 0
    failures: int = 0
    rejected: int = 0
    retries: int = 0
    dead_lettered: int = 0
    events: list[ScenarioEvent] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        return self.successes / self.requests if self.requests else 0.0

    def as_dict(self) -> dict:
        payload = asdict(self)
        payload["success_rate"] = self.success_rate
        return payload


class FaultyDependency:
    """Seeded downstream dependency with configurable failure probability/latency."""

    def __init__(
        self,
        *,
        failure_probability: float,
        seed: int = 42,
        min_latency_ms: float = 0.0,
        max_latency_ms: float = 0.0,
    ) -> None:
        if not 0 <= failure_probability <= 1:
            raise ValueError("failure_probability must be between 0 and 1")
        self.failure_probability = failure_probability
        self.rng = random.Random(seed)
        self.min_latency_ms = min_latency_ms
        self.max_latency_ms = max_latency_ms

    def call(self, payload: int) -> int:
        latency_ms = self.rng.uniform(self.min_latency_ms, self.max_latency_ms)
        if latency_ms:
            time.sleep(latency_ms / 1000)
        if self.rng.random() < self.failure_probability:
            raise RuntimeError("synthetic downstream failure")
        return payload * 2


def circuit_breaker_scenario(
    *,
    requests: int = 20,
    failure_probability: float = 0.65,
    failure_threshold: int = 3,
    seed: int = 42,
) -> ScenarioReport:
    dependency = FaultyDependency(failure_probability=failure_probability, seed=seed)
    breaker = CircuitBreaker(
        failure_threshold=failure_threshold,
        recovery_timeout=3600,
        retry_on=(RuntimeError,),
    )
    report = ScenarioReport("circuit_breaker", requests=requests)

    for step in range(1, requests + 1):
        started = time.perf_counter()
        try:
            breaker.call(lambda step=step: dependency.call(step))
            outcome, detail = "success", "downstream call completed"
            report.successes += 1
        except RuntimeError as exc:
            if str(exc) == "circuit open":
                outcome, detail = "rejected", "circuit breaker rejected request"
                report.rejected += 1
            else:
                outcome, detail = "failure", str(exc)
                report.failures += 1
        report.events.append(
            ScenarioEvent(
                step=step,
                operation="dependency_call",
                outcome=outcome,
                detail=detail,
                latency_ms=(time.perf_counter() - started) * 1000,
            )
        )
    return report


def retry_dlq_scenario(
    *,
    messages: int = 12,
    max_attempts: int = 3,
    poison_every: int = 4,
) -> ScenarioReport:
    queue: RetryQueue[dict] = RetryQueue(
        max_attempts=max_attempts,
        retry_on=(RuntimeError,),
    )
    report = ScenarioReport("retry_dlq", requests=messages)
    poison_ids = {
        f"evt-{index}"
        for index in range(1, messages + 1)
        if index % poison_every == 0
    }

    for index in range(1, messages + 1):
        queue.publish(
            Message(
                f"evt-{index}",
                {"message_id": f"evt-{index}", "value": index},
            )
        )

    def handler(payload: dict) -> None:
        if payload["message_id"] in poison_ids:
            raise RuntimeError("synthetic poison message")

    step = 0
    while queue.ready:
        step += 1
        before_dlq = len(queue.dead_letter)
        status = queue.process_one(handler)
        if status == "processed":
            report.successes += 1
        elif status == "retry_scheduled":
            report.retries += 1
        elif status == "dead_lettered":
            report.dead_lettered += 1
            report.failures += 1
        report.events.append(
            ScenarioEvent(
                step=step,
                operation="consume_message",
                outcome=status,
                detail=(
                    "message moved to DLQ"
                    if len(queue.dead_letter) > before_dlq
                    else "consumer attempt completed"
                ),
                latency_ms=0.0,
            )
        )
    return report


def compare_reports(reports: list[ScenarioReport]) -> dict:
    return {
        "scenarios": [report.as_dict() for report in reports],
        "summary": {
            "total_requests": sum(report.requests for report in reports),
            "total_successes": sum(report.successes for report in reports),
            "total_failures": sum(report.failures for report in reports),
            "total_rejected": sum(report.rejected for report in reports),
            "total_retries": sum(report.retries for report in reports),
            "total_dead_lettered": sum(report.dead_lettered for report in reports),
        },
    }
