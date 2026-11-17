from __future__ import annotations

import json
from pathlib import Path

from resilience.cli import main
from resilience.scenarios import (
    circuit_breaker_scenario,
    network_partition_scenario,
    resilient_executor_scenario,
    retry_dlq_scenario,
    worker_pool_scenario,
)


def test_network_partition_scenario_recovers_deterministically() -> None:
    first = network_partition_scenario()
    repeated = network_partition_scenario()

    assert first.as_dict() == repeated.as_dict()
    assert first.successes == 8
    assert first.failures == 2
    assert first.rejected == 2
    assert [event.outcome for event in first.events[3:8]] == [
        "timeout",
        "timeout",
        "rejected",
        "rejected",
        "success",
    ]


def test_network_partition_cli_writes_report(tmp_path: Path) -> None:
    output = tmp_path / "partition.json"

    result = main(["network-partition", "--requests", "8", "--output", str(output)])

    assert result == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["name"] == "network_partition"
    assert payload["requests"] == 8
    assert payload["successes"] + payload["failures"] + payload["rejected"] == 8


def test_circuit_breaker_scenario_records_open_rejections() -> None:
    report = circuit_breaker_scenario(
        requests=10,
        failure_probability=1.0,
        failure_threshold=2,
        seed=1,
    )

    assert report.successes == 0
    assert report.failures >= 2
    assert report.rejected >= 1
    assert report.failures + report.rejected == report.requests
    assert any(event.outcome == "rejected" for event in report.events)


def test_poison_messages_reach_dlq_with_retry_counts() -> None:
    report = retry_dlq_scenario(messages=8, max_attempts=3, poison_every=4)

    assert report.successes == 6
    assert report.dead_lettered == 2
    assert report.failures == 2
    assert report.retries == 4
    assert any("RuntimeError: synthetic poison message" in event.detail for event in report.events)


def test_resilient_executor_scenario_exposes_replay_and_conflict() -> None:
    report = resilient_executor_scenario()

    assert report.requests == 3
    assert report.successes == 2
    assert report.failures == 1
    assert report.retries == 2
    assert report.duplicates == 1
    assert [event.outcome for event in report.events] == [
        "success",
        "duplicate",
        "conflict",
    ]


def test_worker_pool_scenario_accounts_for_every_submission() -> None:
    report = worker_pool_scenario(items=12, queue_size=2, max_concurrency=1)

    assert report.successes + report.rejected == report.requests
    assert report.failures == 0
    assert len(report.events) == report.requests


def test_cli_suite_writes_structured_report(tmp_path: Path) -> None:
    output = tmp_path / "report.json"
    result = main(
        [
            "suite",
            "--requests",
            "5",
            "--messages",
            "4",
            "--items",
            "6",
            "--output",
            str(output),
        ]
    )

    assert result == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["summary"]["total_requests"] == 5 + 12 + 4 + 3 + 6
    assert {scenario["name"] for scenario in payload["scenarios"]} == {
        "circuit_breaker",
        "network_partition",
        "retry_dlq",
        "resilient_executor",
        "async_worker_pool",
    }
