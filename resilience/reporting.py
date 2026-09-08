from __future__ import annotations

from dataclasses import asdict, dataclass, field
from statistics import quantiles


@dataclass(frozen=True, slots=True)
class ScenarioEvent:
    step: int
    operation: str
    outcome: str
    detail: str
    latency_ms: float


@dataclass(slots=True)
class ScenarioReport:
    name: str
    requests: int
    successes: int = 0
    failures: int = 0
    rejected: int = 0
    retries: int = 0
    dead_lettered: int = 0
    duplicates: int = 0
    events: list[ScenarioEvent] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        return self.successes / self.requests if self.requests else 0.0

    @property
    def p95_latency_ms(self) -> float:
        latencies = [event.latency_ms for event in self.events]
        if not latencies:
            return 0.0
        if len(latencies) == 1:
            return latencies[0]
        return quantiles(latencies, n=100, method="inclusive")[94]

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["success_rate"] = self.success_rate
        payload["p95_latency_ms"] = self.p95_latency_ms
        return payload


def compare_reports(reports: list[ScenarioReport]) -> dict[str, object]:
    return {
        "scenarios": [report.as_dict() for report in reports],
        "summary": {
            "total_requests": sum(report.requests for report in reports),
            "total_successes": sum(report.successes for report in reports),
            "total_failures": sum(report.failures for report in reports),
            "total_rejected": sum(report.rejected for report in reports),
            "total_retries": sum(report.retries for report in reports),
            "total_dead_lettered": sum(report.dead_lettered for report in reports),
            "total_duplicates": sum(report.duplicates for report in reports),
        },
    }
