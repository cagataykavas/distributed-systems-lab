from resilience.cli import main
from resilience.scenarios import circuit_breaker_scenario, retry_dlq_scenario


def test_circuit_breaker_eventually_rejects_requests() -> None:
    report = circuit_breaker_scenario(
        requests=10,
        failure_probability=1.0,
        failure_threshold=2,
        seed=1,
    )
    assert report.failures == 2
    assert report.rejected == 8
    assert report.successes == 0


def test_poison_messages_reach_dlq() -> None:
    report = retry_dlq_scenario(messages=8, max_attempts=3, poison_every=4)
    assert report.successes == 6
    assert report.dead_lettered == 2
    assert report.failures == 2
    assert report.retries == 4


def test_cli_suite_runs() -> None:
    assert main(["suite", "--requests", "5", "--messages", "4"]) == 0
